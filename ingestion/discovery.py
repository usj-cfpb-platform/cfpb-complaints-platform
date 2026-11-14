"""Find **completed** raw batches, so downstream jobs (Spark) never read half-written data.

Completion rule (one sentence for Ali's Spark jobs):

* a **historical load** is complete when its ``manifest.json`` exists, is valid, and the ZIP and
  CSV it names exist with the recorded sizes (the manifest is always written last);
* an **API run** is complete when its ``run_metadata.json`` exists, is valid, has
  ``outcome == "success"`` and every page it lists exists with the recorded size
  (``run_metadata.json`` is always written last).

Everything else (pages without metadata, ``partial`` / ``failed`` runs, size mismatches) is
reported as *incomplete* and must not be processed.

CLI (read-only):  ``python -m ingestion.discovery [--kind full|api|all] [--json]``
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from typing import Any

from common.config import load_settings
from ingestion.contracts import ContractError, validate_manifest, validate_run_metadata
from ingestion.layout import (
    MANIFEST_NAME,
    RUN_METADATA_NAME,
    SOURCE_API,
    SOURCE_FULL,
    Layout,
    parse_run_prefix,
)
from ingestion.storage import RawStore, StorageError, make_store

LOG = logging.getLogger("ingestion.discovery")


@dataclass
class RawRun:
    """One batch in the raw zone."""

    source: str  # "full" or "api"
    run_id: str
    ingest_date: str
    prefix: str
    complete: bool
    reason: str | None = None  # why it is incomplete
    marker_key: str | None = None  # manifest.json or run_metadata.json
    data_keys: list[str] = field(default_factory=list)  # objects Spark should read
    record_count: int | None = None
    document: dict[str, Any] | None = None


def _group_by_run(store: RawStore, prefix: str) -> dict[str, dict[str, int]]:
    """Map run prefix -> {object key: size} for every object under ``prefix``."""
    runs: dict[str, dict[str, int]] = {}
    for obj in store.list_objects(prefix):
        parsed = parse_run_prefix(obj.key)
        if not parsed:
            continue
        date, run_id = parsed
        run_prefix = f"{prefix}/ingest_date={date}/run_id={run_id}"
        runs.setdefault(run_prefix, {})[obj.key] = obj.size
    return runs


def _load_json(store: RawStore, key: str) -> dict[str, Any]:
    document = json.loads(store.get_bytes(key))
    if not isinstance(document, dict):
        raise ContractError(f"{key} is not a JSON object")
    return document


def _check_full_load(
    store: RawStore, run_prefix: str, objects: dict[str, int]
) -> tuple[bool, str | None, dict[str, Any] | None]:
    marker = f"{run_prefix}/{MANIFEST_NAME}"
    if marker not in objects:
        return False, "no manifest.json (load not finished)", None
    try:
        manifest = _load_json(store, marker)
        validate_manifest(manifest)
    except (ContractError, ValueError, StorageError) as exc:
        return False, f"invalid manifest: {exc}", None
    for key_field, size_field in (
        ("archive_path", "archive_bytes"),
        ("extracted_path", "extracted_bytes"),
    ):
        key, expected = manifest[key_field], manifest[size_field]
        if objects.get(key) != expected:
            return False, f"{key} missing or size differs from manifest", manifest
    return True, None, manifest


def _check_api_run(
    store: RawStore, run_prefix: str, objects: dict[str, int]
) -> tuple[bool, str | None, dict[str, Any] | None]:
    marker = f"{run_prefix}/{RUN_METADATA_NAME}"
    if marker not in objects:
        return False, "no run_metadata.json (run not finished)", None
    try:
        metadata = _load_json(store, marker)
        validate_run_metadata(metadata)
    except (ContractError, ValueError, StorageError) as exc:
        return False, f"invalid run metadata: {exc}", None
    if metadata["outcome"] != "success":
        return False, f"outcome is {metadata['outcome']!r}, not 'success'", metadata
    for item in metadata["raw_objects"]:
        if objects.get(item["path"]) != item["bytes"]:
            return False, f"{item['path']} missing or size differs from metadata", metadata
    return True, None, metadata


def discover(store: RawStore, layout: Layout, kind: str = "all") -> list[RawRun]:
    """List every batch in the raw zone with its completeness status (oldest first)."""
    results: list[RawRun] = []
    wanted = {SOURCE_FULL, SOURCE_API} if kind == "all" else {kind}

    if SOURCE_FULL in wanted:
        for run_prefix, objects in _group_by_run(store, layout.full_prefix).items():
            date, run_id = parse_run_prefix(run_prefix) or ("", "")
            ok, reason, doc = _check_full_load(store, run_prefix, objects)
            results.append(
                RawRun(
                    source=SOURCE_FULL,
                    run_id=run_id,
                    ingest_date=date,
                    prefix=run_prefix,
                    complete=ok,
                    reason=reason,
                    marker_key=f"{run_prefix}/{MANIFEST_NAME}",
                    data_keys=[doc["extracted_path"]] if ok and doc else [],
                    record_count=doc.get("record_count") if ok and doc else None,
                    document=doc,
                )
            )
    if SOURCE_API in wanted:
        for run_prefix, objects in _group_by_run(store, layout.api_prefix).items():
            date, run_id = parse_run_prefix(run_prefix) or ("", "")
            ok, reason, doc = _check_api_run(store, run_prefix, objects)
            results.append(
                RawRun(
                    source=SOURCE_API,
                    run_id=run_id,
                    ingest_date=date,
                    prefix=run_prefix,
                    complete=ok,
                    reason=reason,
                    marker_key=f"{run_prefix}/{RUN_METADATA_NAME}",
                    data_keys=[i["path"] for i in doc["raw_objects"]] if ok and doc else [],
                    record_count=doc.get("record_count") if ok and doc else None,
                    document=doc,
                )
            )
    return sorted(results, key=lambda run: (run.run_id.split("_")[1:], run.source))


def completed(store: RawStore, layout: Layout, kind: str = "all") -> list[RawRun]:
    """Only the complete batches: the safe input list for Spark."""
    return [run for run in discover(store, layout, kind) if run.complete]


def latest_complete_full_load(store: RawStore, layout: Layout) -> RawRun | None:
    runs = completed(store, layout, SOURCE_FULL)
    return runs[-1] if runs else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.discovery", description="List raw-zone batches (read-only)."
    )
    parser.add_argument("--kind", choices=["full", "api", "all"], default="all")
    parser.add_argument("--incomplete", action="store_true", help="also list incomplete batches")
    parser.add_argument("--json", action="store_true", help="machine-readable output")
    parser.add_argument("--sink", choices=["minio", "local"], default="minio")
    parser.add_argument("--out", help="folder, with --sink local")
    parser.add_argument(
        "--config", default=None, help="settings YAML (default: config/settings.yaml)"
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(message)s", stream=sys.stderr
    )
    settings = load_settings(args.config) if args.config else load_settings()
    layout = Layout.from_settings(settings)
    store = make_store(args.sink, args.out)
    runs = discover(store, layout, args.kind)
    shown = runs if args.incomplete else [run for run in runs if run.complete]

    if args.json:
        slim = [{k: v for k, v in asdict(run).items() if k != "document"} for run in shown]
        print(json.dumps(slim, indent=2))
    else:
        for run in shown:
            status = "COMPLETE  " if run.complete else "INCOMPLETE"
            extra = "" if run.complete else f"  ({run.reason})"
            print(f"{status} {run.source:<4} {run.run_id}  records={run.record_count}{extra}")
        print(
            f"{sum(r.complete for r in runs)} complete, "
            f"{sum(not r.complete for r in runs)} incomplete"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
