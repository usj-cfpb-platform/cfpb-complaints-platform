"""Incremental collector: window, pagination, retries, checkpoints, safe reruns, bad responses."""

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import requests
import yaml
from ingest_helpers import (
    ApiSim,
    FlakyStore,
    MockServer,
    Resp,
    ScriptedSession,
    make_records,
    no_sleep,
)

from common.config import ConfigError, load_settings
from ingestion.contracts import validate_run_metadata
from ingestion.discovery import discover
from ingestion.incremental import (
    HighWaterMark,
    HighWaterMarkStore,
    IncrementalCollector,
    IncrementalError,
    IncrementalSettings,
    InvalidResponse,
    build_params,
    compute_window,
    main,
    newest_record,
    next_cursor,
    parse_date_received,
    parse_page,
)
from ingestion.layout import Layout
from ingestion.net import HttpClient, RetryPolicy
from ingestion.storage import LocalStore, StorageError

NOW = datetime(2026, 11, 13, 9, 0, 0, tzinfo=UTC)
ENDPOINT = "https://api.example.org/search/"
LAYOUT = Layout()


# ---------------------------------------------------------------- helpers -----------------
def make_collector(
    tmp_path: Path,
    *,
    sim: ApiSim | None = None,
    script: list | None = None,
    store=None,
    products=("Product A", "Product B"),
    page_size: int = 10,
    lookback_days: int = 45,
    max_retries: int = 3,
    with_state: bool = True,
):
    store = store or LocalStore(tmp_path / "zone")
    session = ScriptedSession(script, sim)
    settings = IncrementalSettings(
        endpoint=ENDPOINT,
        timeout_seconds=5,
        max_retries=max_retries,
        backoff_base_seconds=0,
        lookback_days=lookback_days,
        page_size=page_size,
        request_delay_seconds=0,
    )
    client = HttpClient(RetryPolicy(5, max_retries, 0), session=session, sleep=no_sleep)
    hwm = HighWaterMarkStore(store, LAYOUT.state_key) if with_state else None
    collector = IncrementalCollector(
        store,
        LAYOUT,
        settings,
        products,
        client,
        hwm,
        clock=lambda: NOW,
        sleep=no_sleep,
        version="abc1234",
    )
    return collector, store, session


def stored_pages(store, result) -> list[dict]:
    return [json.loads(store.get_bytes(item["path"])) for item in result.metadata["raw_objects"]]


# ---------------------------------------------------------------- pure functions ----------
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-11-12", "2026-11-12"),
        ("2026-11-12T00:00:00-05:00", "2026-11-12"),
        ("2026-11-12T10:30:00Z", "2026-11-12"),
        (1_794_441_600_000, "2026-11-12"),  # epoch milliseconds
        (1_794_441_600, "2026-11-12"),  # epoch seconds
        ("", None),
        ("not a date", None),
        (None, None),
        (True, None),
    ],
)
def test_parse_date_received_accepts_the_formats_the_api_may_use(value, expected) -> None:
    assert parse_date_received(value) == expected


def test_window_without_state_is_today_minus_lookback() -> None:
    window = compute_window(None, date(2026, 11, 13), 45)
    assert (window.date_min, window.date_max) == (date(2026, 9, 29), date(2026, 11, 13))


def test_window_with_state_is_high_water_mark_minus_lookback() -> None:
    mark = HighWaterMark("2026-11-12", "32540001")
    window = compute_window(mark, date(2026, 11, 13), 45)
    assert (window.date_min, window.date_max) == (date(2026, 9, 28), date(2026, 11, 13))
    assert window.lookback_days == 45


def test_explicit_dates_override_the_computed_window() -> None:
    window = compute_window(
        HighWaterMark("2026-11-12", "1"), date(2026, 11, 13), 45, date(2026, 1, 1), date(2026, 2, 1)
    )
    assert (window.date_min, window.date_max) == (date(2026, 1, 1), date(2026, 2, 1))


def test_future_checkpoint_does_not_empty_the_window() -> None:
    window = compute_window(HighWaterMark("2026-12-31", "1"), date(2026, 11, 13), 45)
    assert (window.date_min, window.date_max) == (date(2026, 9, 29), date(2026, 11, 13))


def test_empty_window_is_rejected() -> None:
    with pytest.raises(ConfigError):
        compute_window(None, date(2026, 11, 13), 5, date_min=date(2026, 12, 1))


def test_request_params_follow_the_openapi_contract() -> None:
    window = compute_window(None, date(2026, 11, 13), 10)
    params = build_params(window, ["Product A", "Product B"], 100, "created_date_desc", True, "1_2")
    assert ("date_received_min", "2026-11-03") in params
    assert ("date_received_max", "2026-11-14") in params  # exclusive upper bound: +1 day
    assert ("size", "100") in params and ("sort", "created_date_desc") in params
    assert ("no_aggs", "true") in params and ("no_highlight", "true") in params
    assert [v for k, v in params if k == "product"] == ["Product A", "Product B"]
    assert ("search_after", "1_2") in params
    inclusive = build_params(window, [], 10, "x", False)
    assert ("date_received_max", "2026-11-13") in inclusive
    assert not any(k == "search_after" for k, _ in inclusive)


def test_high_water_mark_ordering_uses_numeric_ids() -> None:
    assert HighWaterMark("2026-11-12", "100").order > HighWaterMark("2026-11-12", "99").order
    assert HighWaterMark("2026-11-13", "1").order > HighWaterMark("2026-11-12", "999").order


def test_parse_page_validates_structure() -> None:
    good = {
        "hits": {
            "total": {"value": 2, "relation": "eq"},
            "hits": [
                {"_id": "7", "_source": {"complaint_id": 7}},
                {"_source": {"complaint_id": "8"}},
            ],
        }
    }
    page = parse_page(json.dumps(good).encode())
    assert page.ids == ["7", "8"] and page.total == 2 and page.total_is_exact


def test_parse_page_uses_hit_id_when_source_has_no_complaint_id() -> None:
    page = parse_page(json.dumps({"hits": {"hits": [{"_id": "55", "_source": {}}]}}).encode())
    assert page.ids == ["55"]


def test_parse_page_accepts_plain_integer_total_and_flags_inexact_totals() -> None:
    assert parse_page(b'{"hits": {"total": 5, "hits": []}}').total == 5
    inexact = parse_page(b'{"hits": {"total": {"value": 10000, "relation": "gte"}, "hits": []}}')
    assert not inexact.total_is_exact


@pytest.mark.parametrize(
    "body",
    [
        b"<html>Service unavailable</html>",
        b"[]",
        b'{"hits": {}}',
        b'{"hits": {"hits": "nope"}}',
        b'{"nothing": true}',
        b'{"hits": {"hits": [{"no_source": 1}]}}',
        b'{"hits": {"hits": [{"_source": {"product": "x"}}]}}',
        b'{"hits": {"hits": [{"_source": {"complaint_id": ""}}]}}',
    ],
)
def test_parse_page_rejects_unexpected_structures(body: bytes) -> None:
    with pytest.raises(InvalidResponse):
        parse_page(body)


def test_cursor_prefers_break_points_then_last_hit_sort() -> None:
    hits = [{"_source": {"complaint_id": "1"}, "sort": [111, "1"]}]
    with_points = parse_page(
        json.dumps({"_meta": {"break_points": {"2": "999_9"}}, "hits": {"hits": hits}}).encode()
    )
    assert next_cursor(with_points, 1) == ("999_9", "break_points")
    without = parse_page(json.dumps({"hits": {"hits": hits}}).encode())
    assert next_cursor(without, 1) == ("111_1", "hit_sort")
    nothing = parse_page(b'{"hits": {"hits": [{"_source": {"complaint_id": "1"}}]}}')
    assert next_cursor(nothing, 1) == (None, None)


def test_newest_record_ignores_unparsable_dates() -> None:
    page = parse_page(
        json.dumps(
            {
                "hits": {
                    "hits": [
                        {"_source": {"complaint_id": "5", "date_received": "2026-10-01"}},
                        {"_source": {"complaint_id": "9", "date_received": "garbage"}},
                        {"_source": {"complaint_id": "6", "date_received": "2026-10-03"}},
                    ]
                }
            }
        ).encode()
    )
    assert newest_record(page) == HighWaterMark("2026-10-03", "6")


def test_settings_validation_and_committed_values() -> None:
    with pytest.raises(ConfigError):
        IncrementalSettings(endpoint=ENDPOINT, page_size=101)  # API maximum is 100
    with pytest.raises(ConfigError):
        IncrementalSettings(endpoint=ENDPOINT, lookback_days=-1)
    settings = IncrementalSettings.from_settings(load_settings())
    assert settings.page_size == 100 and settings.timeout_seconds >= 60
    assert settings.date_max_exclusive is True


# ---------------------------------------------------------------- successful runs ---------
def test_multi_page_run_collects_every_record_and_writes_valid_metadata(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25))
    collector, store, session = make_collector(tmp_path, sim=sim)
    result = collector.run(trigger="test")

    assert result.outcome == "success" and result.exit_code == 0
    md = result.metadata
    validate_run_metadata(md)  # satisfies config/schemas/run_metadata.schema.json
    assert md["pagination"]["pages_fetched"] == 3 == len(md["raw_objects"])
    assert [o["records"] for o in md["raw_objects"]] == [10, 10, 5]
    assert md["record_count"] == 25
    assert md["completeness"] == {
        "expected_total": 25,
        "expected_total_is_exact": True,
        "unique_records_fetched": 25,
    }
    ids = [
        h["_source"]["complaint_id"] for p in stored_pages(store, result) for h in p["hits"]["hits"]
    ]
    assert len(ids) == len(set(ids)) == 25
    assert md["window"] == {
        "date_received_min": "2026-09-29",
        "date_received_max": "2026-11-13",
        "lookback_days": 45,
    }
    assert md["request_params"]["product"] == ["Product A", "Product B"]
    assert md["code_version"] == "abc1234" and md["trigger"] == "test"
    assert md["pagination"]["cursor_sources"] == {"hit_sort": 2}
    assert md["pagination"]["last_search_after"] is not None
    assert md["source_meta"]["license"] == "CC0"


def test_raw_pages_are_stored_unmodified_with_matching_checksums(tmp_path: Path) -> None:
    sim = ApiSim(make_records(12))
    collector, store, _ = make_collector(tmp_path, sim=sim)
    result = collector.run()
    for item in result.metadata["raw_objects"]:
        data = store.get_bytes(item["path"])
        assert hashlib.sha256(data).hexdigest() == item["sha256"]
        assert len(data) == item["bytes"]
    first = json.loads(store.get_bytes(result.metadata["raw_objects"][0]["path"]))
    assert first == ApiSim(make_records(12)).answer(sim.calls[0])  # identical to the API answer


def test_page_keys_follow_the_documented_layout(tmp_path: Path) -> None:
    collector, _, _ = make_collector(tmp_path, sim=ApiSim(make_records(15)))
    result = collector.run()
    run_id = result.run_id
    assert [o["path"] for o in result.metadata["raw_objects"]] == [
        f"raw/cfpb/api/ingest_date=2026-11-13/run_id={run_id}/response_0001.json",
        f"raw/cfpb/api/ingest_date=2026-11-13/run_id={run_id}/response_0002.json",
    ]


def test_exact_multiple_of_page_size_needs_no_extra_request(tmp_path: Path) -> None:
    sim = ApiSim(make_records(20))
    collector, _, _ = make_collector(tmp_path, sim=sim)
    result = collector.run()
    assert result.outcome == "success" and len(sim.calls) == 2  # total says we are done


def test_inexact_total_falls_back_to_reading_until_an_empty_page(tmp_path: Path) -> None:
    sim = ApiSim(make_records(20), exact_total=False)
    collector, _, _ = make_collector(tmp_path, sim=sim)
    result = collector.run()
    assert result.outcome == "success" and result.metadata["record_count"] == 20
    assert result.metadata["completeness"]["expected_total_is_exact"] is False


def test_empty_result_is_a_valid_successful_run(tmp_path: Path) -> None:
    sim = ApiSim([])
    collector, store, _ = make_collector(tmp_path, sim=sim)
    result = collector.run()
    assert result.outcome == "success"
    md = result.metadata
    validate_run_metadata(md)
    assert md["record_count"] == 0 and md["pagination"]["pages_fetched"] == 1
    assert md["high_water_mark"] == {"previous": None, "next": None}
    assert not store.exists(LAYOUT.state_key)  # nothing to checkpoint


def test_break_points_are_used_when_the_api_provides_them(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25), emit_break_points=True)
    collector, _, _ = make_collector(tmp_path, sim=sim)
    result = collector.run()
    assert result.outcome == "success"
    assert result.metadata["pagination"]["cursor_sources"] == {"break_points": 2}


def test_stale_data_flag_is_recorded_but_does_not_fail_the_run(tmp_path: Path) -> None:
    collector, _, _ = make_collector(tmp_path, sim=ApiSim(make_records(3), is_data_stale=True))
    result = collector.run()
    assert result.outcome == "success"
    assert result.metadata["source_meta"]["is_data_stale"] is True


def test_epoch_dates_in_records_still_produce_a_checkpoint(tmp_path: Path) -> None:
    records = make_records(3)
    page = {
        "hits": {
            "total": {"value": 3, "relation": "eq"},
            "hits": [
                {
                    "_id": r["complaint_id"],
                    "_source": {**r, "date_received": 1_794_441_600_000 + i * 86_400_000},
                }
                for i, r in enumerate(records)
            ],
        }
    }
    collector, store, _ = make_collector(tmp_path, script=[page])
    result = collector.run()
    assert result.metadata["high_water_mark"]["next"]["date_received"] == "2026-11-14"


# ---------------------------------------------------------------- checkpoints / reruns ----
def test_high_water_mark_advances_only_after_a_successful_run(tmp_path: Path) -> None:
    collector, store, _ = make_collector(tmp_path, sim=ApiSim(make_records(25)))
    result = collector.run()
    assert result.hwm_advanced
    saved = json.loads(store.get_bytes(LAYOUT.state_key))
    assert (saved["date_received"], saved["complaint_id"]) == ("2026-10-25", "1024")
    assert saved["run_id"] == result.run_id
    assert result.metadata["high_water_mark"] == {
        "previous": None,
        "next": {"date_received": "2026-10-25", "complaint_id": "1024"},
    }


def test_repeated_run_creates_new_objects_and_leaves_the_checkpoint_alone(tmp_path: Path) -> None:
    collector, store, _ = make_collector(tmp_path, sim=ApiSim(make_records(25)))
    first = collector.run()
    state_before = store.get_bytes(LAYOUT.state_key)
    second = collector.run()

    assert first.run_id != second.run_id
    keys1 = {o["path"] for o in first.metadata["raw_objects"]}
    keys2 = {o["path"] for o in second.metadata["raw_objects"]}
    assert keys1.isdisjoint(keys2)  # a rerun never overwrites or reuses raw objects
    assert second.metadata["record_count"] == 25  # overlap is intended; Spark dedups by id
    assert not second.hwm_advanced
    assert store.get_bytes(LAYOUT.state_key) == state_before
    assert second.metadata["high_water_mark"]["previous"] == {
        "date_received": "2026-10-25",
        "complaint_id": "1024",
    }
    assert len([r for r in discover(store, LAYOUT) if r.complete]) == 2


def test_checkpoint_never_moves_backward(tmp_path: Path) -> None:
    collector, store, _ = make_collector(tmp_path, sim=ApiSim(make_records(25)))
    HighWaterMarkStore(store, LAYOUT.state_key).advance(
        HighWaterMark("2026-12-31", "99999"), "api_20260101T000000Z_aaaaaa", NOW
    )
    result = collector.run()
    assert result.outcome == "success" and not result.hwm_advanced
    saved = json.loads(store.get_bytes(LAYOUT.state_key))
    assert saved["date_received"] == "2026-12-31"
    assert result.metadata["high_water_mark"]["next"]["date_received"] == "2026-12-31"


def test_window_is_checkpoint_minus_lookback_and_requests_follow_it(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25))
    collector, store, _ = make_collector(tmp_path, sim=sim, lookback_days=10)
    HighWaterMarkStore(store, LAYOUT.state_key).advance(
        HighWaterMark("2026-10-20", "1019"), "api_20260101T000000Z_aaaaaa", NOW
    )
    result = collector.run()
    first_call = dict(sim.calls[0])
    assert first_call["date_received_min"] == "2026-10-10"
    assert first_call["date_received_max"] == "2026-11-14"
    assert result.metadata["window"]["date_received_min"] == "2026-10-10"
    # late-arriving records inside the lookback are re-collected, older ones are not
    assert result.metadata["record_count"] == 16  # 2026-10-10 .. 2026-10-25


def test_explicit_window_for_backfills(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25))
    collector, _, _ = make_collector(tmp_path, sim=sim)
    result = collector.run(date_min=date(2026, 10, 1), date_max=date(2026, 10, 5))
    assert result.metadata["record_count"] == 5
    assert result.metadata["window"]["date_received_max"] == "2026-10-05"


def test_stateless_mode_ignores_and_never_writes_the_checkpoint(tmp_path: Path) -> None:
    collector, store, _ = make_collector(tmp_path, sim=ApiSim(make_records(5)), with_state=False)
    result = collector.run()
    assert result.outcome == "success" and not result.hwm_advanced
    assert not store.exists(LAYOUT.state_key)


def test_corrupted_checkpoint_is_an_error_not_a_silent_reset(tmp_path: Path) -> None:
    collector, store, _ = make_collector(tmp_path, sim=ApiSim(make_records(5)))
    store.put_bytes(LAYOUT.state_key, b"{not json", overwrite=True)
    with pytest.raises(IncrementalError, match="unreadable"):
        collector.run()


def test_high_water_mark_store_basics(tmp_path: Path) -> None:
    store = LocalStore(tmp_path)
    hwm = HighWaterMarkStore(store, "state/high_water_mark.json")
    assert hwm.read() is None
    assert hwm.advance(HighWaterMark("2026-10-01", "5"), "api_x", NOW)
    assert not hwm.advance(HighWaterMark("2026-10-01", "5"), "api_x", NOW)  # equal: no change
    assert not hwm.advance(HighWaterMark("2026-09-01", "9"), "api_x", NOW)  # older: no change
    assert hwm.advance(HighWaterMark("2026-10-01", "6"), "api_y", NOW)
    assert hwm.read() == HighWaterMark("2026-10-01", "6")


# ---------------------------------------------------------------- HTTP problems -----------
def test_500_then_success_is_retried_and_counted_in_metadata(tmp_path: Path) -> None:
    sim = ApiSim(make_records(5))
    collector, _, session = make_collector(tmp_path, sim=sim, script=[500, 503])
    result = collector.run()
    assert result.outcome == "success"
    assert result.metadata["http"] == {"final_status": 200, "retry_count": 2}
    assert len(session.calls) == 3


def test_timeout_is_retried(tmp_path: Path) -> None:
    sim = ApiSim(make_records(5))
    collector, _, _ = make_collector(tmp_path, sim=sim, script=[requests.Timeout("slow")])
    result = collector.run()
    assert result.outcome == "success" and result.metadata["http"]["retry_count"] == 1


def test_404_fails_fast_and_records_a_failed_run(tmp_path: Path) -> None:
    collector, store, session = make_collector(tmp_path, script=[404])
    result = collector.run()
    assert result.outcome == "failed" and result.exit_code == 1
    assert len(session.calls) == 1
    md = result.metadata
    validate_run_metadata(md)
    assert md["error"]["type"] == "HttpStatusError" and md["http"]["final_status"] == 404
    assert not store.exists(LAYOUT.state_key)
    assert [r.complete for r in discover(store, LAYOUT)] == [False]


def test_persistent_server_error_gives_up_after_bounded_retries(tmp_path: Path) -> None:
    collector, _, session = make_collector(tmp_path, script=[500] * 10, max_retries=2)
    result = collector.run()
    assert result.outcome == "failed"
    assert len(session.calls) == 3  # 1 attempt + 2 retries, never infinite
    assert result.metadata["error"]["type"] == "RetriesExhausted"


def test_failure_on_page_two_is_partial_and_does_not_move_the_checkpoint(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25))
    first_page = sim.answer(
        build_params(
            compute_window(None, NOW.date(), 45),
            ["Product A", "Product B"],
            10,
            "created_date_desc",
            True,
        )
    )
    sim.calls.clear()
    collector, store, _ = make_collector(tmp_path, sim=sim, script=[first_page] + [500] * 10)
    result = collector.run()
    assert result.outcome == "partial" and result.exit_code == 3
    assert result.metadata["pagination"]["pages_fetched"] == 1
    assert not store.exists(LAYOUT.state_key)
    assert not result.hwm_advanced
    assert result.metadata["high_water_mark"]["next"] is None  # unchanged (no previous either)
    assert [r.complete for r in discover(store, LAYOUT)] == [False]  # ignored by Spark


@pytest.mark.parametrize(
    "body",
    [b"<html>maintenance</html>", {"hits": {}}, {"unexpected": "shape"}, b"null"],
)
def test_unexpected_response_structure_fails_without_checkpoint(tmp_path: Path, body) -> None:
    collector, store, _ = make_collector(tmp_path, script=[body])
    result = collector.run()
    assert result.outcome == "failed"
    assert result.metadata["error"]["type"] == "InvalidResponse"
    assert result.metadata["raw_objects"] == [] and not store.exists(LAYOUT.state_key)


# ---------------------------------------------------------------- pagination problems -----
def test_missing_cursor_stops_loudly_instead_of_guessing(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25), emit_sort=False)
    collector, store, _ = make_collector(tmp_path, sim=sim)
    result = collector.run()
    assert result.outcome == "partial"
    assert result.metadata["error"]["type"] == "PaginationError"
    assert "cursor" in result.metadata["error"]["message"]
    assert not store.exists(LAYOUT.state_key)


def test_a_cursor_that_does_not_advance_is_detected(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25), ignore_search_after=True)  # buggy API repeats page 1
    collector, _, session = make_collector(tmp_path, sim=sim)
    result = collector.run()
    assert result.outcome == "partial"
    assert "did not advance" in result.metadata["error"]["message"]
    assert len(session.calls) == 2  # no endless loop


def test_skipped_records_are_caught_by_the_completeness_check(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25), emit_break_points=True, skip_one_per_page=True)
    collector, store, _ = make_collector(tmp_path, sim=sim)
    result = collector.run()
    assert result.outcome == "partial"
    assert "skipped records" in result.metadata["error"]["message"]
    assert result.metadata["completeness"]["unique_records_fetched"] < 25
    assert not store.exists(LAYOUT.state_key)  # a run that lost data never moves the checkpoint


def test_page_limit_makes_the_run_partial(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25))
    collector, store, _ = make_collector(tmp_path, sim=sim)
    result = collector.run(max_pages=2)
    assert result.outcome == "partial"
    assert result.metadata["pagination"]["pages_fetched"] == 2
    assert "page limit" in result.metadata["error"]["message"]
    assert not store.exists(LAYOUT.state_key)


# ---------------------------------------------------------------- interrupted runs ---------
def test_storage_error_on_page_two_leaves_a_partial_run_and_no_checkpoint(tmp_path: Path) -> None:
    def rule(key: str):
        return StorageError("MinIO unavailable") if key.endswith("response_0002.json") else None

    store = FlakyStore(tmp_path / "zone", rule)
    collector, _, _ = make_collector(tmp_path, sim=ApiSim(make_records(25)), store=store)
    result = collector.run()
    assert result.outcome == "partial"
    assert result.metadata["error"]["type"] == "StorageError"
    assert not store.exists(LAYOUT.state_key)


def test_crash_mid_run_leaves_pages_without_metadata_so_the_batch_is_incomplete(
    tmp_path: Path,
) -> None:
    def rule(key: str):
        return RuntimeError("process killed") if key.endswith("response_0002.json") else None

    store = FlakyStore(tmp_path / "zone", rule)
    collector, _, _ = make_collector(tmp_path, sim=ApiSim(make_records(25)), store=store)
    with pytest.raises(RuntimeError, match="process killed"):
        collector.run()

    runs = discover(store, LAYOUT)
    assert len(runs) == 1 and not runs[0].complete
    assert "no run_metadata.json" in runs[0].reason
    assert not store.exists(LAYOUT.state_key)  # next run starts from the old checkpoint: no loss

    # recovery: storage is healthy again, the next run is a normal complete run
    healthy = LocalStore(tmp_path / "zone")
    collector2, _, _ = make_collector(tmp_path, sim=ApiSim(make_records(25)), store=healthy)
    result = collector2.run()
    assert result.outcome == "success"
    assert len([r for r in discover(healthy, LAYOUT) if r.complete]) == 1


def test_failure_to_store_run_metadata_never_advances_the_checkpoint(tmp_path: Path) -> None:
    def rule(key: str):
        return StorageError("disk full") if key.endswith("run_metadata.json") else None

    store = FlakyStore(tmp_path / "zone", rule)
    collector, _, _ = make_collector(tmp_path, sim=ApiSim(make_records(25)), store=store)
    with pytest.raises(IncrementalError, match="NOT advanced"):
        collector.run()
    assert not store.exists(LAYOUT.state_key)
    assert not any(r.complete for r in discover(store, LAYOUT))


# ---------------------------------------------------------------- dry run / validation -----
def test_dry_run_fetches_one_page_and_stores_nothing(tmp_path: Path) -> None:
    sim = ApiSim(make_records(25))
    collector, store, _ = make_collector(tmp_path, sim=sim)
    result = collector.run(dry_run=True, max_pages=1)
    assert result.outcome == "dry_run" and result.exit_code == 0
    assert len(sim.calls) == 1
    assert list(store.list_objects("")) == []


def test_trigger_must_be_known(tmp_path: Path) -> None:
    collector, _, _ = make_collector(tmp_path, sim=ApiSim([]))
    with pytest.raises(ConfigError):
        collector.run(trigger="cron")


def test_scheduled_for_is_recorded(tmp_path: Path) -> None:
    collector, _, _ = make_collector(tmp_path, sim=ApiSim(make_records(3)))
    result = collector.run(trigger="schedule", scheduled_for="2026-11-13T03:17:00Z")
    assert result.metadata["scheduled_for"] == "2026-11-13T03:17:00Z"
    validate_run_metadata(result.metadata)


# ---------------------------------------------------------------- command line ------------
def serve_api(sim: ApiSim):
    def respond(req) -> Resp:
        params = [(k, v) for k, values in req.query.items() for v in values]
        body = json.dumps(sim.answer(params)).encode()
        return Resp(200, {"Content-Type": "application/json"}, body)

    return respond


def write_config(tmp_path: Path, url: str, products: list[str] | None = None) -> Path:
    config = {
        "domain": {"slug": "demo" if products else "unassigned", "products": products or []},
        "source": {"full_csv_url": "http://unused/f.zip", "api_base_url": url},
        "incremental": {
            "timeout_seconds": 5,
            "max_retries": 1,
            "backoff_base_seconds": 0,
            "page_size": 10,
            "request_delay_seconds": 0,
            "lookback_days": 100000 // 365 * 4,
        },
    }
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return path


def test_cli_refuses_to_run_without_an_assigned_domain(tmp_path: Path, capsys) -> None:
    config = write_config(tmp_path, "http://127.0.0.1:9/")
    code = main(["--sink", "local", "--out", str(tmp_path / "z"), "--config", str(config)])
    assert code == 2
    assert "Domain not assigned" in capsys.readouterr().err


def test_cli_runs_end_to_end_with_explicit_products(tmp_path: Path, capsys) -> None:
    sim = ApiSim(make_records(25, first_day=date(2026, 11, 1)))
    with MockServer(serve_api(sim)) as server:
        config = write_config(tmp_path, server.url + "/")
        out = tmp_path / "collected"
        code = main(
            [
                "--sink",
                "local",
                "--out",
                str(out),
                "--config",
                str(config),
                "--no-state",
                "--product",
                "Product A",
                "--trigger",
                "test",
                "--date-min",
                "2026-11-01",
                "--date-max",
                "2026-11-30",
            ]
        )
    summary = json.loads(capsys.readouterr().out)
    assert code == 0 and summary["outcome"] == "success"
    assert summary["record_count"] == 13  # only 'Product A' records
    assert [r.complete for r in discover(LocalStore(out), LAYOUT)] == [True]


def test_cli_returns_failure_and_partial_exit_codes(tmp_path: Path, capsys) -> None:
    config = write_config(tmp_path, "http://x/", ["Product A"])
    with MockServer(lambda req: Resp(404)) as server:
        config = write_config(tmp_path, server.url + "/", ["Product A"])
        base = [
            "--sink",
            "local",
            "--out",
            str(tmp_path / "z1"),
            "--config",
            str(config),
            "--no-state",
        ]
        assert main(base) == 1
    sim = ApiSim(make_records(25, first_day=date(2026, 11, 1)))
    with MockServer(serve_api(sim)) as server:
        config = write_config(tmp_path, server.url + "/", ["Product A", "Product B"])
        argv = [
            "--sink",
            "local",
            "--out",
            str(tmp_path / "z2"),
            "--config",
            str(config),
            "--no-state",
            "--date-min",
            "2026-11-01",
            "--date-max",
            "2026-11-30",
        ]
        assert main([*argv, "--max-pages", "1"]) == 3


def test_cli_dry_run_stores_nothing(tmp_path: Path, capsys) -> None:
    sim = ApiSim(make_records(25, first_day=date(2026, 11, 1)))
    with MockServer(serve_api(sim)) as server:
        config = write_config(tmp_path, server.url + "/", ["Product A", "Product B"])
        out = tmp_path / "z"
        code = main(
            [
                "--sink",
                "local",
                "--out",
                str(out),
                "--config",
                str(config),
                "--no-state",
                "--dry-run",
                "--date-min",
                "2026-11-01",
                "--date-max",
                "2026-11-30",
            ]
        )
        assert server.count() == 1
    assert code == 0 and json.loads(capsys.readouterr().out)["outcome"] == "dry_run"
    assert list(LocalStore(out).list_objects("")) == []
