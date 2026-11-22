"""Turn 3 logic that needs no Spark: quality gate, scope configuration, CSV header mapping."""

from __future__ import annotations

from pathlib import Path

import pytest

from common.config import load_settings
from processing import quality
from processing.schema import SchemaError, validate_csv_header
from processing.transform import Scope

LIVE_HEADER = [
    "Date received", "Product", "Sub-product", "Issue", "Sub-issue", "Company public response",
    "Company", "State", "ZIP code", "Tags", "Submitted via", "Date sent to company",
    "Company response to consumer", "Timely response?", "Complaint ID",
]  # fmt: skip


# --------------------------------------------------------------------------- quality gate


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0.0, quality.PASS), (0.02, quality.WARN), (0.2, quality.FAIL), (None, quality.FAIL)],
)
def test_judge_thresholds(value, expected) -> None:
    assert quality.judge(value, warn_above=0.01, fail_above=0.1) == expected


def test_duplicate_curated_ids_fail_the_gate() -> None:
    rules = {d: {} for d in quality.DIMENSIONS}
    rules["uniqueness"] = {"curated_duplicate_ids": {"warn_above": 0, "fail_above": 0}}
    checks = quality.evaluate({"uniqueness": {"curated_duplicate_ids": 3}}, rules)
    assert quality.gate(checks) == {
        "status": quality.FAIL,
        "failed_checks": ["uniqueness.curated_duplicate_ids"],
        "warned_checks": [],
    }


def test_unmeasured_check_fails_instead_of_passing() -> None:
    rules = {d: {} for d in quality.DIMENSIONS}
    rules["freshness"] = {"days_since_latest_record": {"warn_above": 30, "fail_above": 120}}
    (check,) = quality.evaluate({}, rules)
    assert check.status == quality.FAIL
    assert "not measured" in check.detail


def test_committed_rules_cover_all_six_dimensions() -> None:
    rules = quality.load_rules()
    assert set(quality.DIMENSIONS) <= set(rules)
    assert rules["uniqueness"]["curated_duplicate_ids"]["fail_above"] == 0
    # Missing optional ZIP codes produce a warning, never a failure (brief 6.3).
    assert rules["completeness"]["zip_code_missing_rate"]["fail_above"] >= 1.0


def test_rules_file_without_a_dimension_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "rules.yaml"
    path.write_text("version: 1\nuniqueness: {}\n", encoding="utf-8")
    with pytest.raises(quality.RulesError):
        quality.load_rules(path)


# --------------------------------------------------------------------------- scope


def _settings(**domain) -> dict:
    base = {
        "option": 15,
        "slug": "s",
        "products": ["A old", "A new", "B"],
        "segments": {"a": ["A old", "A new"], "b": ["B"]},
    }
    base.update(domain)
    return {"domain": base}


def test_scope_includes_historical_labels() -> None:
    scope = Scope.from_settings(_settings())
    assert scope.products == ("A old", "A new", "B")
    assert scope.option == 15


@pytest.mark.parametrize(
    "domain",
    [
        {"segments": {"a": ["A old"], "b": ["B"]}},  # a product without a segment
        {"segments": {"a": ["A old", "A new", "B"], "b": ["B"]}},  # label in two segments
        {"option": None},
        {"products": []},
    ],
)
def test_invalid_scope_is_rejected(domain) -> None:
    with pytest.raises(ValueError):
        Scope.from_settings(_settings(**domain))


def test_committed_settings_define_option_15_scope() -> None:
    scope = Scope.from_settings(load_settings())
    assert scope.option == 15
    assert set(scope.segments) == {"deposit_accounts", "transfers_payments"}
    assert all(scope.segments.values())


# --------------------------------------------------------------------------- CSV header


def test_live_15_column_header_maps_by_name() -> None:
    rename = validate_csv_header(LIVE_HEADER)
    assert rename["Complaint ID"] == "complaint_id"
    assert rename["Timely response?"] == "timely"
    assert len(rename) == 15


def test_older_17_column_header_is_accepted() -> None:
    header = LIVE_HEADER[:10] + ["Consumer consent provided?"] + LIVE_HEADER[10:]
    header.insert(-1, "Consumer disputed?")
    assert validate_csv_header(header)["Consumer disputed?"] == "consumer_disputed"


def test_header_with_bom_is_accepted() -> None:
    assert "Date received" in validate_csv_header(["﻿Date received"] + LIVE_HEADER[1:])


@pytest.mark.parametrize(
    "header",
    [
        [c for c in LIVE_HEADER if c != "Complaint ID"],  # required column missing
        LIVE_HEADER + ["Brand new column"],  # unknown column
        LIVE_HEADER + ["Product"],  # duplicate
    ],
)
def test_bad_headers_are_rejected(header) -> None:
    with pytest.raises(SchemaError):
        validate_csv_header(header)
