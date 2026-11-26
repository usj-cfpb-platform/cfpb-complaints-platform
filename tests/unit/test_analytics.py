"""Turn 5 analysis: statistics and orchestration against a fake query runner (no MongoDB)."""

import math

import pytest

from analytics import measures as m
from analytics.run_analysis import collect, render_markdown

A, B = "deposit_accounts", "transfers_payments"


def test_pct_none_on_empty_denominator():
    assert m.pct(1, 0) is None
    assert m.pct(1, 4) == 25.0


def test_wilson_known_value():
    lo, hi = m.wilson_interval(50, 100)
    assert (lo, hi) == pytest.approx((40.38, 59.62), abs=0.01)
    assert m.wilson_interval(0, 0) is None
    lo0, hi0 = m.wilson_interval(0, 50)
    assert lo0 == 0.0 and hi0 > 0


def test_two_proportion_gap_identical_and_different():
    same = m.two_proportion_gap(50, 100, 500, 1000)
    assert same["gap_pp"] == 0 and same["p_value"] == pytest.approx(1.0)
    diff = m.two_proportion_gap(900, 1000, 800, 1000)
    assert diff["gap_pp"] == 10.0
    assert diff["p_value"] < 1e-6
    lo, hi = diff["ci95_pp"]
    assert lo < 10.0 < hi
    assert m.two_proportion_gap(1, 0, 1, 5) is None


def test_two_proportion_degenerate_all_success():
    assert m.two_proportion_gap(10, 10, 10, 10)["p_value"] == 1.0
    assert not math.isnan(m.two_proportion_gap(10, 10, 0, 10)["p_value"])


def test_sum_timely_resums_counts():
    rows = [
        {"complaints": 10, "timely_yes": 9, "timely_no": 1},
        {"complaints": 100, "timely_yes": 50, "timely_no": 50},
    ]
    s = m.sum_timely(rows)
    assert m.pct(s["timely_yes"], s["timely_yes"] + s["timely_no"]) == 53.64  # not (90+50)/2


def test_share_gaps_absent_category_and_reliability():
    rows = [
        {"segment": A, "issue": "x", "complaints": 80, "share_pct": 80.0},
        {"segment": A, "issue": "y", "complaints": 20, "share_pct": 20.0},
        {"segment": B, "issue": "y", "complaints": 5, "share_pct": 50.0},
        {"segment": B, "issue": "z", "complaints": 5, "share_pct": 50.0},
    ]
    gaps = {g["issue"]: g for g in m.share_gaps(rows, "issue", A, B)}
    assert gaps["x"]["gap_pp"] == 80.0 and gaps["x"]["reliable"] is True
    assert gaps["z"]["gap_pp"] == -50.0 and gaps["z"]["reliable"] is False
    assert gaps["y"]["reliable"] is False


def _fake_run(pipeline):
    """Recognise the pipeline by its shape; return rows matching ``database.queries`` output."""
    stages = str(pipeline)
    if "year_month" in stages and "timely" not in stages:
        return [
            {"segment": A, "year_month": "2023-01", "complaints": 100},
            {"segment": B, "year_month": "2024-02", "complaints": 40},
        ]
    if "timely_yes" in stages:
        return [
            {"segment": A, "complaints": 100, "timely_yes": 95, "timely_no": 5},
            {"segment": B, "complaints": 40, "timely_yes": 30, "timely_no": 10},
        ]
    if "routing_lag_status" in stages:
        return [
            {"segment": A, "lag_valid": 90, "lag_median_days": 1},
            {"segment": B, "lag_valid": 30, "lag_median_days": 2},
        ]
    return [
        {
            "segment": A,
            "issue": "i",
            "channel": "Web",
            "company_response": "r",
            "complaints": 100,
            "share_pct": 100.0,
        },
        {
            "segment": B,
            "issue": "i",
            "channel": "Web",
            "company_response": "r",
            "complaints": 40,
            "share_pct": 100.0,
        },
    ]


def test_collect_and_render_without_database():
    result = collect(_fake_run, A, B, "2023-01-01", "2024-12-31")
    assert result["volume"] == {A: 100, B: 40}
    assert [y["year"] for y in result["timely_by_year"]] == ["2023", "2024"]
    assert result["timely_overall"]["gap"]["gap_pp"] == 20.0
    assert result["filters"] == {"date_from": "2023-01-01", "date_to": "2024-12-31"}
    md = render_markdown(result, {"latest_date_received": "2026-11-20"})
    assert "Timely company response" in md and "routing" in md.lower()
    assert "| 2023 |" in md and "| 2024 |" in md
