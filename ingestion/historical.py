"""Historical loader: CFPB full complaint CSV (ZIP) -> MinIO raw zone, with a manifest.

Run:  ``python -m ingestion.historical --help``

Pipeline (issue "Historical loader", ``docs/data_contracts.md`` sections 1-3):

 1. HEAD the source (size, ETag, Last-Modified). If the newest *completed* load already came from
    the same source version, stop: no repeated download or upload (``--force`` overrides).
 2. Stream the ZIP to local disk, resuming after interruptions (``ingestion.download``).
 3. Verify the archive: a valid ZIP with exactly the expected CSV member.
 4. Stream-extract the CSV (the ZIP CRC is verified while extracting), hashing as it goes.
 5. Read the CSV header and count data rows by streaming (no pandas, constant memory).
 6. Build the manifest and validate it against ``config/schemas/manifest.schema.json``
    **before** uploading anything.
 7. Upload ZIP, then CSV, then ``manifest.json`` (completion marker, always last).

Nothing in this module cleans or transforms data: that is Spark's job (Turn 3).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import shutil
import struct
import sys
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.config import REPO_ROOT, ConfigError, load_settings
from ingestion.contracts import ContractError, validate_manifest
from ingestion.discovery import RawRun, latest_complete_full_load
from ingestion.download import (
    DEFAULT_CHUNK_BYTES,
    DownloadError,
    SourceInfo,
    download_resumable,
    head_source,
)
from ingestion.layout import SOURCE_FULL, Layout, ingest_date_of, iso_utc, new_run_id, utc_now
from ingestion.net import HttpClient, HttpFailure, RetryPolicy
from ingestion.storage import RawStore, StorageError, make_store
from ingestion.version import code_version

LOG = logging.getLogger("ingestion.historical")

CSV_FIELD_LIMIT = 10_000_000
DISK_SAFETY_FACTOR = 1.05
DEFAULT_TMP_DIR = REPO_ROOT / "data" / "tmp"


class HistoricalError(RuntimeError):
    """The historical load failed; nothing was marked complete."""


@dataclass(frozen=True)
class HistoricalSettings:
    url: str
    timeout_seconds: float = 60.0
    max_retries: int = 5
    backoff_base_seconds: float = 2.0
    chunk_bytes: int = DEFAULT_CHUNK_BYTES
    tmp_dir: Path = DEFAULT_TMP_DIR
    expected_member: str = "complaints.csv"
    required_columns: tuple[str, ...] = ("Complaint ID", "Date received")

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> HistoricalSettings:
        source = settings.get("source") or {}
        section = settings.get("historical") or {}
        url = str(source.get("full_csv_url", "")).strip()
        if not url:
            raise ConfigError("source.full_csv_url is required")
        tmp = Path(section.get("tmp_dir") or DEFAULT_TMP_DIR)
        if not tmp.is_absolute():
            tmp = REPO_ROOT / tmp
        return cls(
            url=url,
            timeout_seconds=float(section.get("timeout_seconds", cls.timeout_seconds)),
            max_retries=int(section.get("max_retries", cls.max_retries)),
            backoff_base_seconds=float(
                section.get("backoff_base_seconds", cls.backoff_base_seconds)
            ),
            chunk_bytes=int(section.get("chunk_bytes", cls.chunk_bytes)),
            tmp_dir=tmp,
            expected_member=str(section.get("expected_member", cls.expected_member)),
            required_columns=tuple(section.get("required_columns", cls.required_columns)),
        )

    @property
    def policy(self) -> RetryPolicy:
        return RetryPolicy(self.timeout_seconds, self.max_retries, self.backoff_base_seconds)


@dataclass
class LoadResult:
    status: str  # "loaded" | "skipped_up_to_date" | "skipped_identical" | "dry_run"
    run_id: str | None = None
    manifest_key: str | None = None
    manifest: dict[str, Any] | None = None
    reason: str | None = None
    plan: dict[str, Any] | None = None


# --------------------------------------------------------------------------------------
# Archive and CSV checks (structure only - no cleaning)
# --------------------------------------------------------------------------------------
def validate_archive(zip_path: Path, expected_member: str) -> zipfile.ZipInfo:
    """Confirm the file is a ZIP containing exactly one file: the expected CSV."""
    if not zipfile.is_zipfile(zip_path):
        raise HistoricalError(f"{zip_path.name} is not a valid ZIP archive (corrupted download?)")
    try:
        with zipfile.ZipFile(zip_path) as archive:
            members = [i for i in archive.infolist() if not i.is_dir()]
    except zipfile.BadZipFile as exc:
        raise HistoricalError(f"ZIP central directory is damaged: {exc}") from exc
    names = [m.filename for m in members]
    if names != [expected_member]:
        raise HistoricalError(f"Expected exactly one member {expected_member!r}, found {names}")
    if members[0].file_size == 0:
        raise HistoricalError(f"Member {expected_member!r} is empty")
    return members[0]


def extract_member(
    zip_path: Path, member: str, destination: Path, chunk_bytes: int = DEFAULT_CHUNK_BYTES
) -> tuple[int, str]:
    """Stream one member to ``destination``; return ``(bytes, sha256)``.

    Python's zipfile checks the member CRC-32 when the stream ends, so a corrupted archive raises
    here instead of producing a silently wrong CSV.
    """
    digest = hashlib.sha256()
    total = 0
    try:
        with zipfile.ZipFile(zip_path) as archive, archive.open(member) as src:
            with destination.open("wb") as out:
                while chunk := src.read(chunk_bytes):
                    out.write(chunk)
                    digest.update(chunk)
                    total += len(chunk)
    except (zipfile.BadZipFile, zlib.error, EOFError) as exc:
        destination.unlink(missing_ok=True)
        raise HistoricalError(
            f"ZIP content is corrupted (CRC or compression error): {exc}"
        ) from exc
    return total, digest.hexdigest()


def read_csv_structure(csv_path: Path, required_columns: tuple[str, ...]) -> tuple[list[str], int]:
    """Return ``(header, data_row_count)`` by streaming the file with the csv module.

    The csv module (not a line count) is used because quoted fields may contain newlines.
    Memory use is constant. Rows are counted, never parsed into Python objects we keep.
    """
    csv.field_size_limit(CSV_FIELD_LIMIT)
    try:
        with csv_path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                raise HistoricalError("CSV has no header row")
            present = {name.strip().lower() for name in header}
            missing = [c for c in required_columns if c.strip().lower() not in present]
            if missing:
                raise HistoricalError(
                    f"CSV header is missing required column(s) {missing}; found {header}"
                )
            count = sum(1 for _ in reader)
    except csv.Error as exc:
        raise HistoricalError(f"CSV structure error: {exc}") from exc
    if count == 0:
        raise HistoricalError("CSV contains a header but no data rows")
    return [name.strip() for name in header], count


def ensure_free_space(directory: Path, needed_bytes: int | None, what: str) -> None:
    """Fail early with a clear message instead of a half-written file."""
    if not needed_bytes:
        return
    directory.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(directory).free
    if free < needed_bytes * DISK_SAFETY_FACTOR:
        raise HistoricalError(
            f"Not enough free disk space in {directory} for {what}: "
            f"need about {needed_bytes / 1e9:.1f} GB, have {free / 1e9:.1f} GB"
        )


# --------------------------------------------------------------------------------------
# Comparing with what is already stored
# --------------------------------------------------------------------------------------
def same_source_version(manifest: dict[str, Any], source: SourceInfo | None) -> bool:
    """True if a stored manifest was produced from this exact version of the remote file."""
    if source is None:
        return False
    if manifest.get("source_url") != source.url:
        return False
    if source.etag and manifest.get("source_etag"):
        return manifest["source_etag"] == source.etag
    return bool(
        source.last_modified
        and source.content_length
        and manifest.get("source_last_modified") == source.last_modified
        and manifest.get("source_content_length") == source.content_length
    )


# --------------------------------------------------------------------------------------
class HistoricalLoader:
    def __init__(
        self,
        store: RawStore,
        layout: Layout,
        settings: HistoricalSettings,
        client: HttpClient | None = None,
        *,
        clock=utc_now,
        version: str | None = None,
    ) -> None:
        self.store = store
        self.layout = layout
        self.settings = settings
        self.client = client or HttpClient(settings.policy)
        self.clock = clock
        self.version = version or code_version()

    # ---- helpers -----------------------------------------------------------------------
    def _work_dir(self) -> Path:
        tag = hashlib.sha256(self.settings.url.encode()).hexdigest()[:12]
        path = self.settings.tmp_dir / f"full_load_{tag}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _head(self) -> SourceInfo | None:
        try:
            return head_source(self.client, self.settings.url)
        except HttpFailure as exc:
            LOG.warning("HEAD failed (%s); the download itself will report real problems", exc)
            return None

    # ---- main entry --------------------------------------------------------------------
    def run(
        self, *, force: bool = False, dry_run: bool = False, keep_temp: bool = False
    ) -> LoadResult:
        source = self._head()
        latest: RawRun | None = latest_complete_full_load(self.store, self.layout)

        plan = {
            "source_url": self.settings.url,
            "source_content_length": source.content_length if source else None,
            "source_etag": source.etag if source else None,
            "source_last_modified": source.last_modified if source else None,
            "accepts_ranges": source.accepts_ranges if source else None,
            "latest_complete_run_id": latest.run_id if latest else None,
        }

        if (
            latest
            and latest.document
            and not force
            and same_source_version(latest.document, source)
        ):
            LOG.info(
                "Source unchanged since %s; nothing to do (use --force to reload)", latest.run_id
            )
            return LoadResult(
                "skipped_up_to_date", reason=f"unchanged since {latest.run_id}", plan=plan
            )

        if dry_run:
            return LoadResult("dry_run", plan=plan, reason="no download or upload performed")

        started = self.clock()
        run_id = new_run_id(SOURCE_FULL, started)
        date = ingest_date_of(started)
        work = self._work_dir()

        ensure_free_space(work, source.content_length if source else None, "the ZIP download")
        try:
            result = download_resumable(
                self.client,
                self.settings.url,
                work / "complaints.csv.zip",
                chunk_bytes=self.settings.chunk_bytes,
            )
        except DownloadError as exc:
            raise HistoricalError(f"Download failed: {exc}") from exc
        finished = self.clock()

        if (
            latest
            and latest.document
            and not force
            and latest.document["archive_sha256"] == result.sha256
        ):
            LOG.info("Downloaded file is identical to %s; not storing a duplicate", latest.run_id)
            self._cleanup(work, keep_temp)
            return LoadResult(
                "skipped_identical", reason=f"same SHA-256 as {latest.run_id}", plan=plan
            )

        info = validate_archive(result.path, self.settings.expected_member)
        ensure_free_space(work, info.file_size, "the extracted CSV")
        csv_path = work / "complaints.csv"
        csv_bytes, csv_sha = extract_member(
            result.path, self.settings.expected_member, csv_path, self.settings.chunk_bytes
        )
        if csv_bytes != info.file_size:
            raise HistoricalError(f"Extracted {csv_bytes} bytes but ZIP announces {info.file_size}")
        header, record_count = read_csv_structure(csv_path, self.settings.required_columns)

        archive_key = self.layout.archive_key(run_id, date)
        extracted_key = self.layout.extracted_key(run_id, date)
        manifest_key = self.layout.manifest_key(run_id, date)
        manifest: dict[str, Any] = {
            "run_id": run_id,
            "source_url": self.settings.url,
            "download_started_at": iso_utc(started),
            "download_finished_at": iso_utc(finished),
            "archive_path": archive_key,
            "archive_bytes": result.size_bytes,
            "archive_sha256": result.sha256,
            "extracted_path": extracted_key,
            "extracted_bytes": csv_bytes,
            "extracted_sha256": csv_sha,
            "record_count": record_count,
            "header": header,
            "code_version": self.version,
            # Optional provenance fields (additive; see docs/data_contracts.md section 3).
            "source_etag": result.etag,
            "source_last_modified": result.last_modified,
            "source_content_length": source.content_length if source else result.size_bytes,
            "http_retry_count": self.client.retry_count,
            "download_resumed": result.resumed,
        }
        try:
            validate_manifest(manifest)  # contract check BEFORE any upload
        except ContractError as exc:
            raise HistoricalError(str(exc)) from exc

        try:
            meta = {"run-id": run_id}
            self.store.put_file(
                archive_key,
                result.path,
                content_type="application/zip",
                metadata={**meta, "sha256": result.sha256},
            )
            self.store.put_file(
                extracted_key,
                csv_path,
                content_type="text/csv",
                metadata={**meta, "sha256": csv_sha},
            )
            # The manifest is the completion marker: written only after both files are durable.
            self.store.put_bytes(
                manifest_key,
                (json.dumps(manifest, indent=2) + "\n").encode("utf-8"),
                content_type="application/json",
            )
        except StorageError as exc:
            raise HistoricalError(
                f"Upload failed ({exc}). No manifest was written, so run {run_id} is incomplete "
                "and will be ignored by downstream jobs. Re-run to create a new run."
            ) from exc

        LOG.info("Historical load %s complete: %d records", run_id, record_count)
        self._cleanup(work, keep_temp)
        return LoadResult(
            "loaded", run_id=run_id, manifest_key=manifest_key, manifest=manifest, plan=plan
        )

    def _cleanup(self, work: Path, keep: bool) -> None:
        if keep:
            return
        for name in ("complaints.csv.zip", "complaints.csv"):
            (work / name).unlink(missing_ok=True)


# --------------------------------------------------------------------------------------
# Cheap source inspection: verify header, date formats and size without the full download
# --------------------------------------------------------------------------------------
_LOCAL_HEADER = struct.Struct("<4s5H3L2H")


def parse_zip_prefix(data: bytes, max_output: int = 1_000_000) -> tuple[str, bytes]:
    """Decode the first bytes of a ZIP's first member from the *start* of the file.

    A ZIP local file header sits at the beginning, so the first part of a deflated CSV can be read
    without downloading the archive. Returns ``(member_name, decompressed_prefix)``.
    """
    if len(data) < _LOCAL_HEADER.size:
        raise HistoricalError("Not enough bytes to read the ZIP header")
    sig, _ver, flags, method, _t, _d, _crc, compressed_size, _us, name_len, extra_len = (
        _LOCAL_HEADER.unpack_from(data)
    )
    if sig != b"PK\x03\x04":
        raise HistoricalError("Source does not start with a ZIP local file header")
    name = data[30 : 30 + name_len].decode("utf-8", errors="replace")
    payload = data[30 + name_len + extra_len :]
    if method == 0:
        # Stored (uncompressed): stop at the member's own size when the header declares it
        # (bit 3 set means the size is only known later, in a trailing data descriptor).
        if compressed_size and not flags & 0x08:
            payload = payload[:compressed_size]
        return name, payload[:max_output]
    if method == 8:
        decompressor = zlib.decompressobj(-15)
        return name, decompressor.decompress(payload, max_output)
    raise HistoricalError(f"Unsupported ZIP compression method {method}")


def inspect_source(client: HttpClient, url: str, probe_bytes: int = 262_144) -> dict[str, Any]:
    """HEAD + first ``probe_bytes`` of the ZIP: size, validators, CSV header, date samples."""
    info = head_source(client, url)
    response = client.request(
        "GET",
        url,
        headers={"Range": f"bytes=0-{probe_bytes - 1}", "Accept-Encoding": "identity"},
        stream=True,
    )
    try:
        data = b""
        for chunk in response.iter_content(chunk_size=65536):
            data += chunk
            if len(data) >= probe_bytes:
                break
    finally:
        response.close()
    name, prefix = parse_zip_prefix(data[:probe_bytes])
    text = prefix.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(text.splitlines()[:50]))
    header = [h.strip() for h in rows[0]] if rows else []
    date_cols = [i for i, h in enumerate(header) if "date" in h.lower()]
    samples = {header[i]: [r[i] for r in rows[1:4] if i < len(r)] for i in date_cols}
    return {
        "url": url,
        "content_length_bytes": info.content_length,
        "content_length_gb": round(info.content_length / 1e9, 3) if info.content_length else None,
        "etag": info.etag,
        "last_modified": info.last_modified,
        "accepts_ranges": info.accepts_ranges,
        "zip_member": name,
        "csv_header": header,
        "csv_column_count": len(header),
        "date_column_samples": samples,
        "http_status_of_probe": client.last_status,
    }


# --------------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.historical",
        description="Download the full CFPB complaints CSV ZIP into the MinIO raw zone.",
    )
    parser.add_argument("--sink", choices=["minio", "local"], default="minio")
    parser.add_argument("--out", help="output folder when --sink local")
    parser.add_argument("--config", help="settings YAML (default: config/settings.yaml)")
    parser.add_argument("--url", help="override source.full_csv_url")
    parser.add_argument("--tmp-dir", help="local scratch folder (needs ZIP + CSV size free)")
    parser.add_argument(
        "--dry-run", action="store_true", help="HEAD + plan only; no download/upload"
    )
    parser.add_argument("--force", action="store_true", help="load even if the source is unchanged")
    parser.add_argument("--keep-temp", action="store_true", help="keep local ZIP/CSV after upload")
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="read only the first 256 KB: size, CSV header, date formats",
    )
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
        settings_dict = load_settings(args.config) if args.config else load_settings()
        settings = HistoricalSettings.from_settings(settings_dict)
        if args.url:
            settings = HistoricalSettings(**{**settings.__dict__, "url": args.url})
        if args.tmp_dir:
            settings = HistoricalSettings(**{**settings.__dict__, "tmp_dir": Path(args.tmp_dir)})
        client = HttpClient(settings.policy)

        if args.inspect:
            print(json.dumps(inspect_source(client, settings.url), indent=2))
            return 0

        # Even a dry run reads the store (to find the latest completed load); it never writes.
        store = make_store(args.sink, args.out)
        loader = HistoricalLoader(store, Layout.from_settings(settings_dict), settings, client)
        result = loader.run(force=args.force, dry_run=args.dry_run, keep_temp=args.keep_temp)
    except (ConfigError, StorageError) as exc:
        print(f"CONFIGURATION/STORAGE ERROR: {exc}", file=sys.stderr)
        return 2
    except (HistoricalError, HttpFailure) as exc:
        print(f"HISTORICAL LOAD FAILED: {exc}", file=sys.stderr)
        return 1

    summary = {k: v for k, v in vars(result).items() if v is not None and k != "manifest"}
    if result.manifest:
        summary["record_count"] = result.manifest["record_count"]
        summary["archive_sha256"] = result.manifest["archive_sha256"]
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
