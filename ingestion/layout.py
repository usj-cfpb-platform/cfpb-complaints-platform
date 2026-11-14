"""Object-key layout of the raw zone, exactly as defined in ``docs/data_contracts.md``.

All key construction lives here so that no collector builds a path by hand.
Dates in paths are UTC. A rerun always gets a new ``run_id``.

    raw/cfpb/full_load/ingest_date=YYYY-MM-DD/run_id=<id>/complaints.csv.zip
    raw/cfpb/full_load/ingest_date=YYYY-MM-DD/run_id=<id>/extracted/complaints.csv
    raw/cfpb/full_load/ingest_date=YYYY-MM-DD/run_id=<id>/manifest.json        (completion marker)
    raw/cfpb/api/ingest_date=YYYY-MM-DD/run_id=<id>/response_NNNN.json
    raw/cfpb/api/ingest_date=YYYY-MM-DD/run_id=<id>/run_metadata.json          (completion marker)
    state/high_water_mark.json
"""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

SOURCE_FULL = "full"
SOURCE_API = "api"

RUN_ID_RE = re.compile(r"^(?P<source>full|api)_(?P<ts>[0-9]{8}T[0-9]{6}Z)_(?P<rand>[0-9a-f]{6})$")
RUN_PREFIX_RE = re.compile(
    r"ingest_date=(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})/run_id=(?P<run>[^/]+)"
)

ARCHIVE_NAME = "complaints.csv.zip"
EXTRACTED_NAME = "extracted/complaints.csv"
MANIFEST_NAME = "manifest.json"
RUN_METADATA_NAME = "run_metadata.json"
STATE_KEY = "state/high_water_mark.json"


def utc_now() -> datetime:
    """Current time, timezone-aware, UTC, whole seconds (run ids have 1 s resolution)."""
    return datetime.now(UTC).replace(microsecond=0)


def iso_utc(moment: datetime) -> str:
    """ISO 8601 with a trailing ``Z`` (the format used in the contract examples)."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id(source: str, now: datetime | None = None, rand: str | None = None) -> str:
    """Build ``<source>_<UTC timestamp>_<6 hex chars>`` (contract section 2)."""
    if source not in {SOURCE_FULL, SOURCE_API}:
        raise ValueError(f"source must be 'full' or 'api', got {source!r}")
    moment = (now or utc_now()).astimezone(UTC)
    suffix = rand if rand is not None else secrets.token_hex(3)
    run_id = f"{source}_{moment:%Y%m%dT%H%M%SZ}_{suffix}"
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"Invalid run id produced: {run_id!r}")
    return run_id


def ingest_date_of(moment: datetime) -> str:
    """UTC calendar date used in ``ingest_date=`` path segments."""
    return moment.astimezone(UTC).strftime("%Y-%m-%d")


def parse_run_prefix(key: str) -> tuple[str, str] | None:
    """Return ``(ingest_date, run_id)`` found in an object key, or None."""
    match = RUN_PREFIX_RE.search(key)
    return (match.group("date"), match.group("run")) if match else None


@dataclass(frozen=True)
class Layout:
    """Raw-zone prefixes, read from ``config/settings.yaml`` (never hard-coded in jobs)."""

    full_prefix: str = "raw/cfpb/full_load"
    api_prefix: str = "raw/cfpb/api"
    state_key: str = STATE_KEY

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> Layout:
        storage = settings.get("storage") or {}
        return cls(
            full_prefix=str(storage.get("raw_full_load_prefix", cls.full_prefix)).strip("/"),
            api_prefix=str(storage.get("raw_api_prefix", cls.api_prefix)).strip("/"),
            state_key=str(storage.get("high_water_mark_key", cls.state_key)).strip("/"),
        )

    # ---- historical (full load) -------------------------------------------------------
    def full_run_prefix(self, run_id: str, ingest_date: str) -> str:
        return f"{self.full_prefix}/ingest_date={ingest_date}/run_id={run_id}"

    def archive_key(self, run_id: str, ingest_date: str) -> str:
        return f"{self.full_run_prefix(run_id, ingest_date)}/{ARCHIVE_NAME}"

    def extracted_key(self, run_id: str, ingest_date: str) -> str:
        return f"{self.full_run_prefix(run_id, ingest_date)}/{EXTRACTED_NAME}"

    def manifest_key(self, run_id: str, ingest_date: str) -> str:
        return f"{self.full_run_prefix(run_id, ingest_date)}/{MANIFEST_NAME}"

    # ---- incremental (API) ------------------------------------------------------------
    def api_run_prefix(self, run_id: str, ingest_date: str) -> str:
        return f"{self.api_prefix}/ingest_date={ingest_date}/run_id={run_id}"

    def page_key(self, run_id: str, ingest_date: str, page_number: int) -> str:
        if page_number < 1:
            raise ValueError("page numbers start at 1")
        return f"{self.api_run_prefix(run_id, ingest_date)}/response_{page_number:04d}.json"

    def run_metadata_key(self, run_id: str, ingest_date: str) -> str:
        return f"{self.api_run_prefix(run_id, ingest_date)}/{RUN_METADATA_NAME}"
