"""Turn 4 pure logic: documents, classification, quality gate, reconciliation, query builders."""

from datetime import UTC, date, datetime

import pytest

from database import queries, schema
from database.benchmark import summarize_explain
from database.documents import (
    LoadRefused,
    build_reconciliation,
    check_quality_gate,
    classify,
    latest_report_key,
    to_document,
)

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)


def _row(cid: str = "1", h: str = "h1", **extra) -> dict:
    row = {
        "complaint_id": cid,
        "date_received": date(2024, 3, 5),
        "date_sent_to_company": None,
        "ingested_at": datetime(2026, 11, 22, 17, 0),
        "product": "Checking or savings account",
        "option": 15,
        "segment": "deposit_accounts",
        "year": 2024,
        "month": 3,
        "year_month": "2024-03",
        "tags": ("Older American",),
        "record_hash": h,
        "source": "full_load",
        "source_run_id": "full_x",
    }
    row.update(extra)
    return row


# --------------------------------------------------------------------------- documents


def test_dates_become_utc_datetimes_and_lineage_is_added() -> None:
    doc = to_document(_row(), "load_1", NOW)
    assert doc["date_received"] == datetime(2024, 3, 5, tzinfo=UTC)
    assert doc["ingested_at"].tzinfo is UTC
    assert doc["date_sent_to_company"] is None
    assert doc["tags"] == ["Older American"]
    assert doc["load_run_id"] == "load_1" and doc["loaded_at"] == NOW
    assert doc["source_run_id"] == "full_x"


def test_document_has_every_field_the_validator_requires() -> None:
    doc = to_document(_row(), "load_1", NOW)
    missing = [f for f in schema.REQUIRED_FIELDS if f not in doc]
    assert missing == []


def test_complaint_id_is_required_and_stringified() -> None:
    assert to_document(_row(cid=123), "l", NOW)["complaint_id"] == "123"
    with pytest.raises(LoadRefused):
        to_document(_row(cid=None), "l", NOW)


# --------------------------------------------------------------------------- classify


def test_classify_insert_update_unchanged() -> None:
    docs = [_row("1", "a"), _row("2", "b"), _row("3", "c")]
    split = classify(docs, {"2": "b", "3": "OLD"})
    assert [d["complaint_id"] for d in split.inserts] == ["1"]
    assert [d["complaint_id"] for d in split.updates] == ["3"]
    assert split.unchanged == 1


def test_classify_second_identical_load_changes_nothing() -> None:
    docs = [_row("1", "a"), _row("2", "b")]
    split = classify(docs, {"1": "a", "2": "b"})
    assert (len(split.inserts), len(split.updates), split.unchanged) == (0, 0, 2)


def test_classify_force_update_rewrites_existing() -> None:
    split = classify([_row("1", "a")], {"1": "a"}, force_update=True)
    assert len(split.updates) == 1


def test_classify_refuses_duplicate_ids() -> None:
    with pytest.raises(LoadRefused):
        classify([_row("1"), _row("1")], {})


# --------------------------------------------------------------------------- quality gate


def test_latest_report_is_the_newest_spark_run() -> None:
    keys = [
        "quality/run_id=spark_20261122T170908Z_8b65fe/quality_report.json",
        "quality/run_id=spark_20261122T172858Z_9a2657/quality_report.json",
        "quality/run_id=spark_20261121T090000Z_aaaaaa/other.json",
    ]
    assert latest_report_key(keys) == keys[1]
    assert latest_report_key([]) is None


@pytest.mark.parametrize("status", ["pass", "warn"])
def test_gate_pass_or_warn_is_loadable(status: str) -> None:
    check_quality_gate({"gate": {"status": status}, "outputs_written": True})


@pytest.mark.parametrize(
    "report",
    [
        {"gate": {"status": "fail", "failed_checks": ["uniqueness.x"]}, "outputs_written": False},
        {"gate": {"status": "warn"}, "outputs_written": False},
        {"outputs_written": True},
    ],
)
def test_gate_refuses(report: dict) -> None:
    with pytest.raises(LoadRefused):
        check_quality_gate(report)


# --------------------------------------------------------------------------- reconciliation


def _rec(**overrides):
    args = dict(
        load_run_id="load_1",
        report={
            "run_id": "spark_1",
            "option": 15,
            "gate": {"status": "warn"},
            "reconciliation": {"raw_rows": 1000, "curated": 3},
        },
        curated_rows_read=3,
        distinct_ids_read=3,
        inserted=1,
        updated=1,
        unchanged=1,
        stale_found=0,
        stale_deleted=0,
        mongo_documents=3,
        started_at=NOW,
        finished_at=NOW,
    )
    args.update(overrides)
    return build_reconciliation(**args)


def test_reconciliation_balanced() -> None:
    rec = _rec()
    assert rec["balanced"] is True
    assert rec["raw"]["raw_rows"] == 1000 and rec["curated"]["reported_by_spark"] == 3


@pytest.mark.parametrize(
    "override",
    [{"mongo_documents": 4}, {"curated_rows_read": 2}, {"unchanged": 0}, {"distinct_ids_read": 2}],
)
def test_reconciliation_detects_imbalance(override: dict) -> None:
    assert _rec(**override)["balanced"] is False


# --------------------------------------------------------------------------- queries


def test_build_match_maps_filters_to_index_fields() -> None:
    match = queries.build_match(
        {
            "segment": ["deposit_accounts"],
            "state": ["CA", "NY"],
            "channel": "Web",
            "date_from": "2024-01-01",
            "date_to": "2024-12-31",
            "company": None,
        }
    )
    assert match["segment"] == "deposit_accounts"
    assert match["state"] == {"$in": ["CA", "NY"]}
    assert match["submitted_via"] == "Web"
    assert match["date_received"] == {
        "$gte": datetime(2024, 1, 1, tzinfo=UTC),
        "$lt": datetime(2025, 1, 1, tzinfo=UTC),  # date_to is inclusive
    }
    assert "company" not in match


def test_empty_filters_match_everything() -> None:
    assert queries.build_match(None) == {} and queries.build_match({"state": []}) == {}


@pytest.mark.parametrize("name", sorted(queries.CATALOGUE))
def test_every_query_starts_with_the_index_friendly_match(name: str) -> None:
    pipeline = queries.CATALOGUE[name]({"segment": "deposit_accounts"})
    assert pipeline[0] == {"$match": {"segment": "deposit_accounts"}}


def test_catalogue_has_at_least_five_queries() -> None:
    assert len(queries.CATALOGUE) >= 5


def test_timely_rate_uses_valid_yes_no_denominator() -> None:
    text = repr(queries.timely_rate())
    assert "'$timely_yes', {'$add': ['$timely_yes', '$timely_no']}" in text


# --------------------------------------------------------------------------- explain


def test_summarize_explain_classic_shape() -> None:
    doc = {
        "stages": [
            {
                "$cursor": {
                    "queryPlanner": {
                        "winningPlan": {
                            "stage": "FETCH",
                            "inputStage": {"stage": "IXSCAN", "indexName": "ix_segment_date"},
                        }
                    },
                    "executionStats": {
                        "nReturned": 10,
                        "totalDocsExamined": 10,
                        "totalKeysExamined": 10,
                        "executionTimeMillis": 3,
                    },
                }
            }
        ]
    }
    s = summarize_explain(doc)
    assert s["access"] == "IXSCAN" and s["index"] == "ix_segment_date"
    assert s["docs_examined"] == 10


def test_summarize_explain_sbe_collscan_shape() -> None:
    doc = {
        "queryPlanner": {
            "winningPlan": {"queryPlan": {"stage": "GROUP", "inputStage": {"stage": "COLLSCAN"}}}
        },
        "executionStats": {
            "totalDocsExamined": 700000,
            "totalKeysExamined": 0,
            "executionTimeMillis": 900,
            "nReturned": 2,
        },
    }
    s = summarize_explain(doc)
    assert s["access"] == "COLLSCAN" and s["docs_examined"] == 700000 and s["index"] == "-"


def test_summarize_explain_reports_only_the_winning_index() -> None:
    doc = {
        "queryPlanner": {
            "winningPlan": {"queryPlan": {"stage": "IXSCAN", "indexName": "ix_segment_state_date"}},
            "rejectedPlans": [
                {"queryPlan": {"stage": "IXSCAN", "indexName": "ix_segment_date"}},
                {"queryPlan": {"stage": "IXSCAN", "indexName": "ix_segment_issue_date"}},
            ],
        },
        "executionStats": {"totalDocsExamined": 7973, "executionTimeMillis": 30},
    }
    assert summarize_explain(doc)["index"] == "ix_segment_state_date"
