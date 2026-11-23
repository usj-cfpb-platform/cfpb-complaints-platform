"""Pure functions used by the loader (no MongoDB or Spark needed, so they are unit-tested).

* :func:`to_document` turns a curated Parquet row into a MongoDB document;
* :func:`classify` decides insert / update / unchanged by comparing ``record_hash``;
* :func:`check_quality_gate` refuses to load when the Spark quality gate failed;
* :func:`build_reconciliation` writes the raw -> curated -> MongoDB count check.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any


class LoadRefused(RuntimeError):
    """Raised when the inputs must not be loaded (failed gate, missing outputs, bad data)."""


# --------------------------------------------------------------------------- documents


def _to_datetime(value: Any) -> Any:
    """BSON has no date-only type: dates become UTC midnight, naive timestamps are UTC."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    return value


DATE_FIELDS = ("date_received", "date_sent_to_company", "ingested_at")
INT_FIELDS = ("option", "year", "month", "routing_lag_days")


def to_document(row: Mapping[str, Any], load_run_id: str, loaded_at: datetime) -> dict[str, Any]:
    """Curated row -> MongoDB document. Original CFPB labels are copied unchanged."""
    doc: dict[str, Any] = dict(row)
    for name in DATE_FIELDS:
        if name in doc:
            doc[name] = _to_datetime(doc[name])
    for name in INT_FIELDS:
        if doc.get(name) is not None:
            doc[name] = int(doc[name])
    for name in ("tags", "quality_flags"):
        if doc.get(name) is not None:
            doc[name] = list(doc[name])
    if not doc.get("complaint_id"):
        raise LoadRefused("curated row without complaint_id")
    doc["complaint_id"] = str(doc["complaint_id"])
    doc["load_run_id"] = load_run_id
    doc["loaded_at"] = loaded_at
    return doc


# --------------------------------------------------------------------------- classification


@dataclass
class Classified:
    inserts: list[dict[str, Any]] = field(default_factory=list)
    updates: list[dict[str, Any]] = field(default_factory=list)
    unchanged: int = 0


def classify(
    docs: Iterable[dict[str, Any]],
    existing_hashes: Mapping[str, str],
    *,
    force_update: bool = False,
) -> Classified:
    """Split a batch by comparing each ``record_hash`` with the one already stored.

    New id -> insert; different hash (or ``force_update``) -> update; same hash -> unchanged.
    A repeated id inside one batch means the curated layer is not unique: refuse to load.
    """
    result = Classified()
    seen: set[str] = set()
    for doc in docs:
        cid = doc["complaint_id"]
        if cid in seen:
            raise LoadRefused(f"duplicate complaint_id {cid!r} in curated input")
        seen.add(cid)
        stored = existing_hashes.get(cid)
        if stored is None:
            result.inserts.append(doc)
        elif force_update or stored != doc.get("record_hash"):
            result.updates.append(doc)
        else:
            result.unchanged += 1
    return result


# --------------------------------------------------------------------------- quality gate


def latest_report_key(keys: Iterable[str]) -> str | None:
    """Newest ``quality/run_id=spark_<UTC>_<hex>/quality_report.json`` (ids sort by time)."""
    reports = [k for k in keys if k.endswith("/quality_report.json") and "run_id=spark_" in k]
    return max(reports, default=None)


def check_quality_gate(report: Mapping[str, Any]) -> None:
    """Load only the output of a run whose gate did not fail and that wrote its outputs."""
    status = (report.get("gate") or {}).get("status")
    if status is None:
        raise LoadRefused("quality report has no gate status")
    if str(status).lower() == "fail":
        failed = (report.get("gate") or {}).get("failed_checks")
        raise LoadRefused(
            f"latest quality gate FAILED ({failed}); run processing.job again before loading"
        )
    if not report.get("outputs_written"):
        raise LoadRefused("latest Spark run did not write curated outputs")


# --------------------------------------------------------------------------- reconciliation


def build_reconciliation(
    *,
    load_run_id: str,
    report: Mapping[str, Any],
    curated_rows_read: int,
    distinct_ids_read: int,
    inserted: int,
    updated: int,
    unchanged: int,
    stale_found: int,
    stale_deleted: int,
    mongo_documents: int,
    started_at: datetime,
    finished_at: datetime,
) -> dict[str, Any]:
    """Raw -> curated -> MongoDB counts for one load, with explicit pass/fail checks."""
    rec = report.get("reconciliation") or {}
    curated_reported = rec.get("curated")
    checks = {
        "curated_read_equals_reported": curated_rows_read == curated_reported,
        "curated_ids_unique": distinct_ids_read == curated_rows_read,
        "loaded_operations_balance": inserted + updated + unchanged == curated_rows_read,
        "mongo_equals_curated": mongo_documents == curated_rows_read,
    }
    return {
        "load_run_id": load_run_id,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "option": report.get("option"),
        "quality_run_id": report.get("run_id"),
        "quality_gate": (report.get("gate") or {}).get("status"),
        "raw": {
            "raw_rows": rec.get("raw_rows"),
            "rejected_identity": rec.get("rejected_identity"),
            "filtered_out_of_scope": rec.get("filtered_out_of_scope"),
            "rejected_date": rec.get("rejected_date"),
            "duplicates_removed": rec.get("duplicates_removed"),
            "filtered_out_after_dedup": rec.get("filtered_out_after_dedup"),
        },
        "curated": {"reported_by_spark": curated_reported, "read_by_loader": curated_rows_read},
        "mongodb": {
            "inserted": inserted,
            "updated": updated,
            "unchanged": unchanged,
            "stale_found": stale_found,
            "stale_deleted": stale_deleted,
            "documents_after_load": mongo_documents,
        },
        "checks": checks,
        "balanced": all(checks.values()),
    }
