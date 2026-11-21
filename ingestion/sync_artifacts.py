"""Copy incremental runs collected on GitHub Actions into the MinIO raw zone, unchanged.

The scheduled workflow runs the collector on a GitHub runner (which cannot reach our MinIO) and
keeps ``raw/cfpb/api/...`` as a workflow artifact. This command brings those runs home:

* downloads recent artifacts (needs ``GITHUB_TOKEN``) **or** reads an already-downloaded folder
  (``--from-dir``, no token needed);
* for every run: validates ``run_metadata.json`` against the contract, verifies the size and
  SHA-256 of every page against it, and skips runs already in MinIO;
* uploads the pages first and ``run_metadata.json`` last (completion marker), keeping the
  original ``run_id`` and the original bytes;
* advances ``state/high_water_mark.json`` only for ``success`` runs, never backward.

Run:  ``python -m ingestion.sync_artifacts --help``
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from common.config import ConfigError, load_settings
from ingestion.contracts import ContractError, validate_run_metadata
from ingestion.download import sha256_file
from ingestion.incremental import HighWaterMark, HighWaterMarkStore, IncrementalError
from ingestion.layout import RUN_METADATA_NAME, Layout, parse_run_prefix, utc_now
from ingestion.net import HttpClient, HttpFailure, RetryPolicy
from ingestion.storage import RawStore, StorageError, make_store

LOG = logging.getLogger("ingestion.sync")

DEFAULT_REPO = "usj-cfpb-platform/cfpb-complaints-platform"
DEFAULT_ARTIFACT_PREFIX = "api-run-"
GITHUB_API = "https://api.github.com"


class SyncError(RuntimeError):
    """A collected run could not be verified or stored; it was NOT marked complete."""


@dataclass
class SyncReport:
    uploaded: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    rejected: list[tuple[str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.rejected


# --------------------------------------------------------------------------------------
# One collected run (a folder that mirrors the MinIO key layout)
# --------------------------------------------------------------------------------------
def find_metadata_files(root: Path) -> list[Path]:
    return sorted(root.rglob(RUN_METADATA_NAME))


def _verify_pages(root: Path, metadata: dict[str, Any]) -> None:
    for item in metadata["raw_objects"]:
        path = root / item["path"]
        if not path.is_file():
            raise SyncError(f"page missing from artifact: {item['path']}")
        if path.stat().st_size != item["bytes"]:
            raise SyncError(f"size mismatch for {item['path']}")
        if sha256_file(path) != item["sha256"]:
            raise SyncError(f"SHA-256 mismatch for {item['path']} (corrupted or altered)")


def _ensure_object(store: RawStore, key: str, path: Path) -> bool:
    """Upload ``path`` unless an identical-size object is already there. Never overwrites."""
    existing = store.stat(key)
    if existing is not None:
        if existing.size != path.stat().st_size:
            raise SyncError(f"{key} already exists in the raw zone with different content")
        return False
    digest = sha256_file(path)
    store.put_file(key, path, content_type="application/json", metadata={"sha256": digest})
    return True


def sync_run(
    root: Path,
    metadata_file: Path,
    store: RawStore,
    layout: Layout,
    hwm_store: HighWaterMarkStore | None,
    *,
    dry_run: bool = False,
) -> str:
    """Sync one run. Returns ``"uploaded"`` or ``"skipped"``; raises ``SyncError`` if rejected."""
    try:
        metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
        validate_run_metadata(metadata)
    except (ValueError, ContractError) as exc:
        raise SyncError(f"invalid run_metadata.json: {exc}") from exc

    relative = metadata_file.relative_to(root).as_posix()
    parsed = parse_run_prefix(relative)
    run_id = metadata["run_id"]
    if not relative.startswith(layout.api_prefix + "/") or not parsed or parsed[1] != run_id:
        raise SyncError(f"artifact path {relative} does not match run {run_id} / the raw layout")

    if store.exists(relative):
        return "skipped"
    _verify_pages(root, metadata)
    if dry_run:
        return "uploaded"

    for item in metadata["raw_objects"]:
        _ensure_object(store, item["path"], root / item["path"])
    # Completion marker last, byte-for-byte as collected.
    store.put_file(
        relative,
        metadata_file,
        content_type="application/json",
        metadata={"sha256": sha256_file(metadata_file)},
    )

    if metadata["outcome"] == "success" and hwm_store is not None:
        nxt = metadata["high_water_mark"]["next"]
        if nxt:
            hwm_store.advance(HighWaterMark.from_dict(nxt), run_id, utc_now())
    return "uploaded"


def sync_directory(
    root: Path,
    store: RawStore,
    layout: Layout,
    hwm_store: HighWaterMarkStore | None,
    *,
    dry_run: bool = False,
) -> SyncReport:
    report = SyncReport()
    # Oldest first, so the high-water mark moves forward in order.
    files = sorted(find_metadata_files(root), key=lambda p: p.parent.name)
    for metadata_file in files:
        run_label = metadata_file.parent.name
        try:
            status = sync_run(root, metadata_file, store, layout, hwm_store, dry_run=dry_run)
        except (SyncError, StorageError, IncrementalError) as exc:
            LOG.error("Rejected %s: %s", run_label, exc)
            report.rejected.append((run_label, str(exc)))
            continue
        (report.uploaded if status == "uploaded" else report.skipped).append(run_label)
        LOG.info("%s: %s", run_label, status)
    return report


# --------------------------------------------------------------------------------------
# GitHub Actions artifacts
# --------------------------------------------------------------------------------------
class GitHubArtifacts:
    """Minimal REST client for workflow artifacts (list + download)."""

    def __init__(
        self, repo: str, token: str, client: HttpClient | None = None, api_base: str = GITHUB_API
    ) -> None:
        self.repo = repo
        self.api_base = api_base.rstrip("/")
        self.client = client or HttpClient(RetryPolicy(timeout_seconds=60, max_retries=4))
        self.headers = {
            "Authorization": f"Bearer {token}",  # never logged
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def list_recent(
        self, name_prefix: str, since: datetime, max_pages: int = 10
    ) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            response = self.client.request(
                "GET",
                f"{self.api_base}/repos/{self.repo}/actions/artifacts",
                params={"per_page": 100, "page": page},
                headers=self.headers,
            )
            artifacts = response.json().get("artifacts", [])
            response.close()
            if not artifacts:
                break
            for art in artifacts:
                created = datetime.fromisoformat(str(art["created_at"]).replace("Z", "+00:00"))
                if (
                    not art.get("expired")
                    and str(art["name"]).startswith(name_prefix)
                    and created >= since
                ):
                    found.append(art)
        return found

    def download(self, artifact: dict[str, Any], destination: Path) -> None:
        response = self.client.request(
            "GET", artifact["archive_download_url"], headers=self.headers, stream=True
        )
        try:
            with destination.open("wb") as out:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    out.write(chunk)
        finally:
            response.close()


def safe_extract(zip_path: Path, destination: Path) -> None:
    """Extract an artifact, refusing paths that would escape ``destination`` (zip-slip)."""
    base = destination.resolve()
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            target = (base / member.filename).resolve()
            if base != target and base not in target.parents:
                raise SyncError(f"unsafe path in artifact: {member.filename!r}")
        archive.extractall(base)


# --------------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.sync_artifacts",
        description="Upload incremental runs collected on GitHub Actions into the MinIO raw zone.",
    )
    parser.add_argument("--from-dir", help="folder with already-downloaded artifact content")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", DEFAULT_REPO))
    parser.add_argument("--name-prefix", default=DEFAULT_ARTIFACT_PREFIX)
    parser.add_argument("--since-days", type=int, default=14, help="only artifacts this recent")
    parser.add_argument("--sink", choices=["minio", "local"], default="minio")
    parser.add_argument("--out", help="folder when --sink local")
    parser.add_argument("--config", help="settings YAML (default: config/settings.yaml)")
    parser.add_argument("--dry-run", action="store_true", help="verify only; upload nothing")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    try:
        settings = load_settings(args.config) if args.config else load_settings()
        layout = Layout.from_settings(settings)
        store = make_store(args.sink, args.out)
        hwm_store = HighWaterMarkStore(store, layout.state_key)
        report = SyncReport()

        if args.from_dir:
            report = sync_directory(
                Path(args.from_dir), store, layout, hwm_store, dry_run=args.dry_run
            )
        else:
            token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
            if not token:
                raise ConfigError(
                    "Set GITHUB_TOKEN (read access to Actions artifacts) or use --from-dir"
                )
            github = GitHubArtifacts(args.repo, token)
            since = datetime.now(UTC) - timedelta(days=args.since_days)
            artifacts = github.list_recent(args.name_prefix, since)
            LOG.info("%d artifact(s) to inspect", len(artifacts))
            for artifact in sorted(artifacts, key=lambda a: a["created_at"]):
                with tempfile.TemporaryDirectory(prefix="cfpb-sync-") as tmp:
                    zip_path = Path(tmp) / "artifact.zip"
                    github.download(artifact, zip_path)
                    extracted = Path(tmp) / "content"
                    safe_extract(zip_path, extracted)
                    part = sync_directory(extracted, store, layout, hwm_store, dry_run=args.dry_run)
                report.uploaded += part.uploaded
                report.skipped += part.skipped
                report.rejected += part.rejected
    except (ConfigError, StorageError) as exc:
        print(f"CONFIGURATION/STORAGE ERROR: {exc}", file=sys.stderr)
        return 2
    except (HttpFailure, SyncError, zipfile.BadZipFile) as exc:
        print(f"SYNC FAILED: {exc}", file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "uploaded": report.uploaded,
                "skipped_already_synced": report.skipped,
                "rejected": report.rejected,
                "dry_run": args.dry_run,
            },
            indent=2,
        )
    )
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
