"""Incremental collector: CFPB Complaint Search API -> raw JSON pages + run metadata.

Run:  ``python -m ingestion.incremental --help``

What the official OpenAPI spec (cfpb/ccdb5-api ``swagger-config.yaml``) guarantees, and what this
collector therefore does:

* ``GET <base>/`` with ``date_received_min`` (inclusive) / ``date_received_max`` (**exclusive**).
  The API can filter on the *submission* date (``date_received``) and the date sent to the company,
  but **not** on the date a complaint was published or changed (``created_date`` is only a sort
  key). A plain date filter therefore cannot, by itself, find every newly published record.
  Complaints are published with a delay, so the collector re-requests a bounded **lookback
  window** ``[high_water_mark - lookback_days, today]`` on every run; duplicates are removed
  downstream by ``complaint_id`` (``docs/data_contracts.md`` section 5).
* ``size`` is at most 100. Deep paging uses ``search_after`` (``k_id`` cursors), because ``frm``
  is capped at 100000 and was reported to repeat page 1 (decision D-004).
* No authentication; rate limits are not documented, so requests are sequential and polite.

Safety rules:

* every page is stored **unmodified** before anything else is done with it;
* a **completeness check** compares unique records fetched with the API's own total hit count;
* ``run_metadata.json`` is written last and is the completion marker;
* the high-water mark advances **only** when ``outcome == "success"``, after everything is stored.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from common.config import ConfigError, load_settings, require_assigned_domain
from ingestion.contracts import ContractError, validate_run_metadata
from ingestion.layout import SOURCE_API, Layout, ingest_date_of, iso_utc, new_run_id, utc_now
from ingestion.net import HttpClient, HttpFailure, RetryPolicy
from ingestion.storage import RawStore, StorageError, make_store
from ingestion.version import code_version

LOG = logging.getLogger("ingestion.incremental")

MAX_PAGE_SIZE = 100  # OpenAPI: size min 1, max 100
TRIGGERS = ("schedule", "manual", "test")
SOURCE_META_KEYS = (
    "license",
    "last_updated",
    "last_indexed",
    "total_record_count",
    "is_data_stale",
    "has_data_issue",
)
_DATE_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2})")


class IncrementalError(RuntimeError):
    """The incremental run failed."""


class InvalidResponse(IncrementalError):
    """The API answered with a structure we do not understand."""


class PaginationError(IncrementalError):
    """Paging could not continue safely (cursor missing/not advancing, or counts disagree)."""


# --------------------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class IncrementalSettings:
    endpoint: str
    timeout_seconds: float = 60.0
    max_retries: int = 5
    backoff_base_seconds: float = 2.0
    lookback_days: int = 45
    page_size: int = MAX_PAGE_SIZE
    sort: str = "created_date_desc"
    max_pages: int = 2000
    request_delay_seconds: float = 0.2
    date_max_exclusive: bool = True  # per the OpenAPI spec; flip if a live test proves otherwise

    def __post_init__(self) -> None:
        if not 1 <= self.page_size <= MAX_PAGE_SIZE:
            raise ConfigError(f"page_size must be between 1 and {MAX_PAGE_SIZE} (API limit)")
        if self.lookback_days < 0:
            raise ConfigError("lookback_days must be >= 0")
        if self.max_pages < 1:
            raise ConfigError("max_pages must be >= 1")

    @classmethod
    def from_settings(cls, settings: dict[str, Any]) -> IncrementalSettings:
        source = settings.get("source") or {}
        section = settings.get("incremental") or {}
        endpoint = str(source.get("api_base_url", "")).strip()
        if not endpoint:
            raise ConfigError("source.api_base_url is required")
        return cls(
            endpoint=endpoint,
            timeout_seconds=float(section.get("timeout_seconds", cls.timeout_seconds)),
            max_retries=int(section.get("max_retries", cls.max_retries)),
            backoff_base_seconds=float(
                section.get("backoff_base_seconds", cls.backoff_base_seconds)
            ),
            lookback_days=int(section.get("lookback_days", cls.lookback_days)),
            page_size=int(section.get("page_size", cls.page_size)),
            sort=str(section.get("sort", cls.sort)),
            max_pages=int(section.get("max_pages", cls.max_pages)),
            request_delay_seconds=float(
                section.get("request_delay_seconds", cls.request_delay_seconds)
            ),
            date_max_exclusive=bool(section.get("date_max_exclusive", cls.date_max_exclusive)),
        )

    @property
    def policy(self) -> RetryPolicy:
        return RetryPolicy(self.timeout_seconds, self.max_retries, self.backoff_base_seconds)


# --------------------------------------------------------------------------------------
# High-water mark and window
# --------------------------------------------------------------------------------------
def parse_date_received(value: Any) -> str | None:
    """Normalise an API date to ``YYYY-MM-DD`` (accepts ISO date/datetime or epoch ms/s)."""
    if isinstance(value, str):
        match = _DATE_PREFIX.match(value.strip())
        return match.group(1) if match else None
    if isinstance(value, int | float) and not isinstance(value, bool):
        seconds = value / 1000 if abs(value) > 1e11 else value
        try:
            return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%d")
        except (OverflowError, OSError, ValueError):
            return None
    return None


@dataclass(frozen=True)
class HighWaterMark:
    """Newest complaint seen: ``date_received`` then ``complaint_id`` (contract section 4)."""

    date_received: str
    complaint_id: str

    @property
    def order(self) -> tuple[str, int, str]:
        numeric = int(self.complaint_id) if self.complaint_id.isdigit() else -1
        return (self.date_received, numeric, self.complaint_id)

    def as_dict(self) -> dict[str, str]:
        return {"date_received": self.date_received, "complaint_id": self.complaint_id}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HighWaterMark:
        parsed = parse_date_received(data.get("date_received"))
        cid = str(data.get("complaint_id", "")).strip()
        if not parsed or not cid:
            raise IncrementalError(f"Invalid high-water mark document: {data!r}")
        return cls(parsed, cid)


class HighWaterMarkStore:
    """``state/high_water_mark.json``: the only raw-zone object that may be overwritten."""

    def __init__(self, store: RawStore, key: str) -> None:
        self.store = store
        self.key = key

    def read(self) -> HighWaterMark | None:
        if not self.store.exists(self.key):
            return None
        try:
            document = json.loads(self.store.get_bytes(self.key))
            return HighWaterMark.from_dict(document)
        except (ValueError, IncrementalError) as exc:
            # Never silently reset to "no state": that would hide a corrupted checkpoint.
            raise IncrementalError(f"High-water mark {self.key} is unreadable: {exc}") from exc

    def advance(self, candidate: HighWaterMark, run_id: str, now: datetime) -> bool:
        """Move the mark forward (never backward). Returns True if it was written."""
        current = self.read()
        if current is not None and candidate.order <= current.order:
            LOG.info("High-water mark unchanged (%s >= %s)", current, candidate)
            return False
        document = {**candidate.as_dict(), "updated_at": iso_utc(now), "run_id": run_id}
        self.store.put_bytes(
            self.key,
            (json.dumps(document, indent=2) + "\n").encode("utf-8"),
            content_type="application/json",
            overwrite=True,
        )
        return True


@dataclass(frozen=True)
class Window:
    date_min: date  # inclusive
    date_max: date  # inclusive (what the run intends to cover)
    lookback_days: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "date_received_min": self.date_min.isoformat(),
            "date_received_max": self.date_max.isoformat(),
            "lookback_days": self.lookback_days,
        }


def compute_window(
    previous: HighWaterMark | None,
    today: date,
    lookback_days: int,
    date_min: date | None = None,
    date_max: date | None = None,
) -> Window:
    """``[HWM - lookback_days, today]``; explicit dates override. No state: ``today - lookback``."""
    upper = date_max or today
    if date_min is not None:
        lower = date_min
    elif previous is not None:
        anchor = date.fromisoformat(previous.date_received)
        if anchor > upper:
            # A checkpoint in the future (clock skew, manual edit) must not empty the window and
            # block collection: anchor at the upper bound instead.
            LOG.warning(
                "High-water mark %s is after %s; anchoring the window at %s", anchor, upper, upper
            )
            anchor = upper
        lower = anchor - timedelta(days=lookback_days)
    else:
        lower = upper - timedelta(days=lookback_days)
    if lower > upper:
        raise ConfigError(f"Empty window: date_min {lower} is after date_max {upper}")
    return Window(lower, upper, lookback_days)


def build_params(
    window: Window,
    products: tuple[str, ...] | list[str],
    page_size: int,
    sort: str,
    max_exclusive: bool,
    search_after: str | None = None,
) -> list[tuple[str, str]]:
    """Query parameters as repeated pairs (``product`` may occur several times)."""
    sent_max = window.date_max + timedelta(days=1) if max_exclusive else window.date_max
    params: list[tuple[str, str]] = [
        ("date_received_min", window.date_min.isoformat()),
        ("date_received_max", sent_max.isoformat()),
        ("size", str(page_size)),
        ("sort", sort),
        ("no_aggs", "true"),
        ("no_highlight", "true"),
    ]
    params.extend(("product", product) for product in products)
    if search_after:
        params.append(("search_after", search_after))
    return params


# --------------------------------------------------------------------------------------
# Page parsing and cursors
# --------------------------------------------------------------------------------------
@dataclass
class Page:
    payload: dict[str, Any]
    hits: list[dict[str, Any]]
    ids: list[str]
    total: int | None
    total_is_exact: bool


def parse_page(body: bytes) -> Page:
    """Validate the response structure. Raises ``InvalidResponse`` for anything unexpected."""
    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise InvalidResponse(f"Response is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise InvalidResponse("Response root is not a JSON object")
    outer = payload.get("hits")
    if not isinstance(outer, dict) or not isinstance(outer.get("hits"), list):
        raise InvalidResponse("Response has no hits.hits list")
    ids: list[str] = []
    for hit in outer["hits"]:
        source = hit.get("_source") if isinstance(hit, dict) else None
        if not isinstance(source, dict):
            raise InvalidResponse("A hit has no _source object")
        complaint_id = source.get("complaint_id", hit.get("_id"))
        if complaint_id is None or str(complaint_id).strip() == "":
            raise InvalidResponse("A hit has no complaint_id")
        ids.append(str(complaint_id).strip())
    total = outer.get("total")
    relation = "eq"
    if isinstance(total, dict):
        relation = str(total.get("relation", "eq"))
        total = total.get("value")
    total_int = total if isinstance(total, int) and not isinstance(total, bool) else None
    return Page(payload, outer["hits"], ids, total_int, total_int is not None and relation == "eq")


def next_cursor(page: Page, page_number: int) -> tuple[str | None, str | None]:
    """Cursor for the page after ``page_number`` -> ``(cursor, source_name)``.

    1. ``_meta.break_points[<next page number>]`` (the documented ``search_after`` source);
    2. otherwise the ``sort`` values of the last hit, joined with ``_`` (the ``k_id`` form).
    If neither exists the caller fails loudly rather than guess.
    """
    meta = page.payload.get("_meta")
    points = meta.get("break_points") if isinstance(meta, dict) else None
    if isinstance(points, dict):
        value = points.get(str(page_number + 1))
        if isinstance(value, str) and value.strip():
            return value.strip(), "break_points"
        # The live API has been reported to return the sort values as a list,
        # e.g. "2": [1794435841000, "32548220"] -> "1794435841000_32548220".
        if isinstance(value, list) and value:
            return "_".join(str(v) for v in value), "break_points"
    last = page.hits[-1] if page.hits else None
    sort_values = last.get("sort") if isinstance(last, dict) else None
    if isinstance(sort_values, list) and sort_values:
        return "_".join(str(v) for v in sort_values), "hit_sort"
    return None, None


def newest_record(page: Page) -> HighWaterMark | None:
    """Highest (date_received, complaint_id) on a page, or None if no date could be parsed."""
    best: HighWaterMark | None = None
    for hit, complaint_id in zip(page.hits, page.ids, strict=True):
        parsed = parse_date_received(hit["_source"].get("date_received"))
        if not parsed:
            continue
        candidate = HighWaterMark(parsed, complaint_id)
        if best is None or candidate.order > best.order:
            best = candidate
    return best


# --------------------------------------------------------------------------------------
# Collector
# --------------------------------------------------------------------------------------
@dataclass
class RunResult:
    outcome: str  # success | partial | failed | dry_run
    run_id: str
    metadata: dict[str, Any]
    hwm_advanced: bool = False

    @property
    def exit_code(self) -> int:
        return {"success": 0, "dry_run": 0, "partial": 3}.get(self.outcome, 1)


@dataclass
class _Progress:
    raw_objects: list[dict[str, Any]] = field(default_factory=list)
    seen_ids: set[str] = field(default_factory=set)
    record_count: int = 0
    pages_fetched: int = 0
    last_cursor: str | None = None
    cursor_sources: dict[str, int] = field(default_factory=dict)
    newest: HighWaterMark | None = None
    source_meta: dict[str, Any] = field(default_factory=dict)
    expected_total: int | None = None
    total_is_exact: bool = False
    exhausted: bool = False


class IncrementalCollector:
    def __init__(
        self,
        store: RawStore,
        layout: Layout,
        settings: IncrementalSettings,
        products: tuple[str, ...] | list[str],
        client: HttpClient | None = None,
        hwm_store: HighWaterMarkStore | None = None,
        *,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
        version: str | None = None,
    ) -> None:
        self.store = store
        self.layout = layout
        self.settings = settings
        self.products = tuple(products)
        self.client = client or HttpClient(settings.policy)
        self.hwm_store = hwm_store
        self.clock = clock
        self._sleep = sleep
        self.version = version or code_version()

    # ---- one run ------------------------------------------------------------------------
    def run(
        self,
        *,
        trigger: str = "manual",
        scheduled_for: str | None = None,
        date_min: date | None = None,
        date_max: date | None = None,
        max_pages: int | None = None,
        dry_run: bool = False,
    ) -> RunResult:
        if trigger not in TRIGGERS:
            raise ConfigError(f"trigger must be one of {TRIGGERS}")
        started = self.clock()
        run_id = new_run_id(SOURCE_API, started)
        ingest_date = ingest_date_of(started)
        previous = self.hwm_store.read() if self.hwm_store else None
        window = compute_window(
            previous, started.date(), self.settings.lookback_days, date_min, date_max
        )
        page_limit = min(max_pages or self.settings.max_pages, self.settings.max_pages)
        LOG.info(
            "Run %s window %s..%s products=%d",
            run_id,
            window.date_min,
            window.date_max,
            len(self.products),
        )

        progress = _Progress()
        error: Exception | None = None
        try:
            self._collect(run_id, ingest_date, window, page_limit, progress, dry_run)
        except (IncrementalError, HttpFailure, StorageError) as exc:
            error = exc
            LOG.error("Run %s interrupted: %s", run_id, exc)

        if error is None and not progress.exhausted and not dry_run:
            error = PaginationError(
                f"Stopped at the page limit ({page_limit}) with more data available; "
                "raise --max-pages or narrow the window"
            )
        if error is None and progress.exhausted:
            error = self._completeness_error(progress)

        outcome = (
            "success" if error is None else ("partial" if progress.pages_fetched else "failed")
        )
        if dry_run:
            outcome = "dry_run" if error is None else outcome

        candidate = progress.newest
        if previous and (candidate is None or previous.order >= candidate.order):
            candidate = previous  # the mark never moves backward
        metadata = self._metadata(
            run_id,
            trigger,
            scheduled_for,
            started,
            window,
            progress,
            outcome,
            previous,
            candidate if outcome == "success" else previous,
            error,
        )
        if dry_run:
            return RunResult(outcome, run_id, metadata)

        try:
            validate_run_metadata(metadata)
            self.store.put_bytes(
                self.layout.run_metadata_key(run_id, ingest_date),
                (json.dumps(metadata, indent=2) + "\n").encode("utf-8"),
                content_type="application/json",
            )
        except (ContractError, StorageError) as exc:
            raise IncrementalError(
                f"Could not store run_metadata.json for {run_id} ({exc}). The run is incomplete, "
                "downstream jobs will ignore it, and the high-water mark was NOT advanced."
            ) from exc

        advanced = False
        if outcome == "success" and self.hwm_store and candidate is not None:
            advanced = self.hwm_store.advance(candidate, run_id, self.clock())
        return RunResult(outcome, run_id, metadata, advanced)

    # ---- paging -------------------------------------------------------------------------
    def _collect(
        self,
        run_id: str,
        ingest_date: str,
        window: Window,
        page_limit: int,
        progress: _Progress,
        dry_run: bool,
    ) -> None:
        cursor: str | None = None
        for page_number in range(1, page_limit + 1):
            params = build_params(
                window,
                self.products,
                self.settings.page_size,
                self.settings.sort,
                self.settings.date_max_exclusive,
                cursor,
            )
            response = self.client.request("GET", self.settings.endpoint, params=params)
            body = response.content  # raw bytes exactly as received
            response.close()
            page = parse_page(body)

            if page_number == 1:
                meta = page.payload.get("_meta")
                if isinstance(meta, dict):
                    progress.source_meta = {k: meta[k] for k in SOURCE_META_KEYS if k in meta}
                progress.expected_total = page.total
                progress.total_is_exact = page.total_is_exact
                if progress.source_meta.get("is_data_stale") or progress.source_meta.get(
                    "has_data_issue"
                ):
                    LOG.warning("CFPB reports stale data or a data issue: %s", progress.source_meta)

            if not page.hits and page_number > 1:
                progress.exhausted = True
                return

            new_ids = [i for i in page.ids if i not in progress.seen_ids]
            if page_number > 1 and page.hits and not new_ids:
                raise PaginationError(
                    f"Page {page_number} contained only records already fetched: the cursor "
                    f"{cursor!r} did not advance"
                )

            if not dry_run:
                key = self.layout.page_key(run_id, ingest_date, page_number)
                digest = hashlib.sha256(body).hexdigest()
                self.store.put_bytes(
                    key,
                    body,
                    content_type="application/json",
                    metadata={"sha256": digest, "run-id": run_id},
                )
                progress.raw_objects.append(
                    {"path": key, "bytes": len(body), "sha256": digest, "records": len(page.hits)}
                )
            progress.pages_fetched += 1
            progress.record_count += len(page.hits)
            progress.seen_ids.update(page.ids)
            newest = newest_record(page)
            if newest and (progress.newest is None or newest.order > progress.newest.order):
                progress.newest = newest
            LOG.info(
                "Page %d: %d records (%d unique so far)",
                page_number,
                len(page.hits),
                len(progress.seen_ids),
            )

            finished = len(page.hits) < self.settings.page_size or not page.hits
            if (
                progress.total_is_exact
                and progress.expected_total is not None
                and len(progress.seen_ids) >= progress.expected_total
            ):
                finished = True
            if finished:
                progress.exhausted = True
                return

            cursor, source = next_cursor(page, page_number)
            if cursor is None or source is None:
                raise PaginationError(
                    "A full page was returned but no search_after cursor could be derived "
                    "(no _meta.break_points entry and no sort values on the last hit)"
                )
            progress.cursor_sources[source] = progress.cursor_sources.get(source, 0) + 1
            progress.last_cursor = cursor
            if self.settings.request_delay_seconds:
                self._sleep(self.settings.request_delay_seconds)

    @staticmethod
    def _completeness_error(progress: _Progress) -> Exception | None:
        """Compare unique records fetched with the API's own total for this query."""
        if not progress.total_is_exact or progress.expected_total is None:
            return None
        if len(progress.seen_ids) < progress.expected_total:
            return PaginationError(
                f"Fetched {len(progress.seen_ids)} unique records but the API reported "
                f"{progress.expected_total} for this query: pagination skipped records"
            )
        return None

    # ---- metadata -----------------------------------------------------------------------
    def _metadata(
        self,
        run_id: str,
        trigger: str,
        scheduled_for: str | None,
        started: datetime,
        window: Window,
        progress: _Progress,
        outcome: str,
        previous: HighWaterMark | None,
        next_mark: HighWaterMark | None,
        error: Exception | None,
    ) -> dict[str, Any]:
        s = self.settings
        sent_max = window.date_max + timedelta(days=1) if s.date_max_exclusive else window.date_max
        metadata: dict[str, Any] = {
            "run_id": run_id,
            "trigger": trigger,
            "scheduled_for": scheduled_for,
            "started_at": iso_utc(started),
            "finished_at": iso_utc(self.clock()),
            "endpoint": s.endpoint,
            "request_params": {
                "date_received_min": window.date_min.isoformat(),
                "date_received_max": sent_max.isoformat(),
                "size": s.page_size,
                "sort": s.sort,
                "product": list(self.products),
                "no_aggs": True,
                "no_highlight": True,
            },
            "window": window.as_dict(),
            "pagination": {
                "page_size": s.page_size,
                "pages_fetched": len(progress.raw_objects),
                "last_search_after": progress.last_cursor,
                "cursor_sources": progress.cursor_sources,
            },
            "http": {
                "final_status": self.client.last_status,
                "retry_count": self.client.retry_count,
            },
            "outcome": outcome,
            "raw_objects": progress.raw_objects,
            "record_count": progress.record_count,
            "high_water_mark": {
                "previous": previous.as_dict() if previous else None,
                "next": next_mark.as_dict() if next_mark else None,
            },
            "source_meta": progress.source_meta,
            "code_version": self.version,
            "completeness": {
                "expected_total": progress.expected_total,
                "expected_total_is_exact": progress.total_is_exact,
                "unique_records_fetched": len(progress.seen_ids),
            },
        }
        if error is not None:
            metadata["error"] = {"type": type(error).__name__, "message": str(error)}
        return metadata


# --------------------------------------------------------------------------------------
def _date_arg(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {text!r}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.incremental",
        description="Collect recent CFPB complaints (lookback window) as raw JSON pages.",
    )
    parser.add_argument("--sink", choices=["minio", "local"], default="minio")
    parser.add_argument("--out", help="output folder when --sink local")
    parser.add_argument("--config", help="settings YAML (default: config/settings.yaml)")
    parser.add_argument("--trigger", choices=TRIGGERS, default="manual")
    parser.add_argument("--scheduled-for", help="planned time (ISO 8601) for scheduled runs")
    parser.add_argument("--date-min", type=_date_arg, help="window start (YYYY-MM-DD, inclusive)")
    parser.add_argument("--date-max", type=_date_arg, help="window end (YYYY-MM-DD, inclusive)")
    parser.add_argument("--lookback-days", type=int, help="override incremental.lookback_days")
    parser.add_argument("--page-size", type=int, help=f"records per request (1-{MAX_PAGE_SIZE})")
    parser.add_argument("--max-pages", type=int, help="stop after N pages (run is then 'partial')")
    parser.add_argument(
        "--product",
        action="append",
        help="CFPB product label (repeatable); overrides domain.products",
    )
    parser.add_argument(
        "--no-state",
        action="store_true",
        help="do not read/write the high-water mark (stateless runner)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch up to --max-pages (default 1) and print a summary; store nothing",
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
        settings = IncrementalSettings.from_settings(settings_dict)
        overrides = {
            k: v
            for k, v in {"lookback_days": args.lookback_days, "page_size": args.page_size}.items()
            if v is not None
        }
        if overrides:
            settings = IncrementalSettings(**{**settings.__dict__, **overrides})
        # Never run on an empty filter: ~680k complaints per 30 days across all products.
        products = (
            tuple(args.product) if args.product else require_assigned_domain(settings_dict).products
        )
        layout = Layout.from_settings(settings_dict)
        store = make_store(args.sink, args.out)
        hwm_store = None if args.no_state else HighWaterMarkStore(store, layout.state_key)
        collector = IncrementalCollector(store, layout, settings, products, hwm_store=hwm_store)
        result = collector.run(
            trigger=args.trigger,
            scheduled_for=args.scheduled_for,
            date_min=args.date_min,
            date_max=args.date_max,
            max_pages=args.max_pages or (1 if args.dry_run else None),
            dry_run=args.dry_run,
        )
    except (ConfigError, StorageError) as exc:
        print(f"CONFIGURATION/STORAGE ERROR: {exc}", file=sys.stderr)
        return 2
    except IncrementalError as exc:
        print(f"INCREMENTAL RUN FAILED: {exc}", file=sys.stderr)
        return 1

    md = result.metadata
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "outcome": result.outcome,
                "window": md["window"],
                "pages_fetched": md["pagination"]["pages_fetched"],
                "record_count": md["record_count"],
                "completeness": md["completeness"],
                "hwm_advanced": result.hwm_advanced,
                "high_water_mark": md["high_water_mark"],
                "error": md.get("error"),
            },
            indent=2,
        )
    )
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
