"""Aggregation pipelines over ``complaints`` shared by the dashboard (Turn 6) and analysis (Turn 5).

Every pipeline starts with :func:`build_match`, so the dashboard filters map directly onto the
compound indexes in :mod:`database.schema` (segment + one category + date range).

Rules applied everywhere (brief V2, section 7):
* counts are distinct Complaint IDs (one document per ID, enforced by the unique index);
* shares are computed within each segment, so the two sides of option 15 compare fairly;
* timely % = valid Yes / (valid Yes + valid No); missing values are reported separately;
* groups with fewer than ``min_cell`` complaints are flagged ``below_min_cell`` (privacy, noise).

Run ``python -m database.queries`` to print every query for the demo.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

from database.schema import COMPLAINTS

MIN_CELL = 10


def _as_list(value: Any) -> list[Any] | None:
    if value is None or value == "" or value == []:
        return None
    if isinstance(value, list | tuple | set):
        return list(value)
    return [value]


def _day(value: date | datetime | str) -> datetime:
    if isinstance(value, str):
        value = date.fromisoformat(value)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def build_match(filters: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Dashboard filters -> ``$match``. ``date_to`` is inclusive (whole day)."""
    filters = filters or {}
    match: dict[str, Any] = {}
    for key, fld in (
        ("segment", "segment"),
        ("product", "product"),
        ("issue", "issue"),
        ("state", "state"),
        ("company", "company"),
        ("channel", "submitted_via"),
    ):
        values = _as_list(filters.get(key))
        if values is not None:
            match[fld] = values[0] if len(values) == 1 else {"$in": values}
    date_range: dict[str, Any] = {}
    if filters.get("date_from"):
        date_range["$gte"] = _day(filters["date_from"])
    if filters.get("date_to"):
        date_range["$lt"] = _day(filters["date_to"]) + timedelta(days=1)
    if date_range:
        match["date_received"] = date_range
    return match


def _share_within_segment(min_cell: int) -> list[dict[str, Any]]:
    return [
        {
            "$setWindowFields": {
                "partitionBy": "$segment",
                "output": {"segment_total": {"$sum": "$complaints"}},
            }
        },
        {
            "$addFields": {
                "share_pct": {
                    "$round": [
                        {"$multiply": [100, {"$divide": ["$complaints", "$segment_total"]}]},
                        2,
                    ]
                },
                "below_min_cell": {"$lt": ["$complaints", min_cell]},
            }
        },
    ]


def _count_by(
    filters: Mapping[str, Any] | None, field: str, label: str, min_cell: int
) -> list[dict[str, Any]]:
    return [
        {"$match": build_match(filters)},
        {
            "$group": {
                "_id": {"segment": "$segment", label: {"$ifNull": [f"${field}", "Missing"]}},
                "complaints": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "segment": "$_id.segment",
                label: f"$_id.{label}",
                "complaints": 1,
            }
        },
        *_share_within_segment(min_cell),
        {"$sort": {"segment": 1, "complaints": -1}},
    ]


# --------------------------------------------------------------------------- the catalogue


def monthly_trend(filters: Mapping[str, Any] | None = None, min_cell: int = MIN_CELL) -> list:
    """Q1. Complaints per segment and month (trend view)."""
    return [
        {"$match": build_match(filters)},
        {
            "$group": {
                "_id": {"segment": "$segment", "year_month": "$year_month"},
                "complaints": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "segment": "$_id.segment",
                "year_month": "$_id.year_month",
                "complaints": 1,
                "below_min_cell": {"$lt": ["$complaints", min_cell]},
            }
        },
        {"$sort": {"segment": 1, "year_month": 1}},
    ]


def issue_mix(filters: Mapping[str, Any] | None = None, min_cell: int = MIN_CELL) -> list:
    """Q2. Issue composition with counts and shares within each segment."""
    return _count_by(filters, "issue", "issue", min_cell)


def state_distribution(filters: Mapping[str, Any] | None = None, min_cell: int = MIN_CELL) -> list:
    """Q3. Complaints by state (Missing kept visible), shares within segment."""
    return _count_by(filters, "state", "state", min_cell)


def channel_mix(filters: Mapping[str, Any] | None = None, min_cell: int = MIN_CELL) -> list:
    """Q4. Submission channel mix within segment."""
    return _count_by(filters, "submitted_via", "channel", min_cell)


def company_response_mix(
    filters: Mapping[str, Any] | None = None, min_cell: int = MIN_CELL
) -> list:
    """Q5. Distribution of company response categories within segment."""
    return _count_by(filters, "company_response", "company_response", min_cell)


def timely_rate(filters: Mapping[str, Any] | None = None) -> list:
    """Q6. Timely % = Yes / (Yes + No); missing or invalid counted separately (never averaged)."""
    return [
        {"$match": build_match(filters)},
        {
            "$group": {
                "_id": "$segment",
                "complaints": {"$sum": 1},
                "timely_yes": {"$sum": {"$cond": [{"$eq": ["$timely", True]}, 1, 0]}},
                "timely_no": {"$sum": {"$cond": [{"$eq": ["$timely", False]}, 1, 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "segment": "$_id",
                "complaints": 1,
                "timely_yes": 1,
                "timely_no": 1,
                "timely_missing_or_invalid": {
                    "$subtract": ["$complaints", {"$add": ["$timely_yes", "$timely_no"]}]
                },
                "timely_pct": {
                    "$cond": [
                        {"$gt": [{"$add": ["$timely_yes", "$timely_no"]}, 0]},
                        {
                            "$round": [
                                {
                                    "$multiply": [
                                        100,
                                        {
                                            "$divide": [
                                                "$timely_yes",
                                                {"$add": ["$timely_yes", "$timely_no"]},
                                            ]
                                        },
                                    ]
                                },
                                2,
                            ]
                        },
                        None,
                    ]
                },
            }
        },
        {"$sort": {"segment": 1}},
    ]


def routing_lag_summary(filters: Mapping[str, Any] | None = None) -> list:
    """Q7. Receipt-to-referral days (CFPB routing, NOT company resolution time)."""
    valid_lag = {"$cond": [{"$eq": ["$routing_lag_status", "ok"]}, "$routing_lag_days", None]}
    return [
        {"$match": build_match(filters)},
        {
            "$group": {
                "_id": "$segment",
                "complaints": {"$sum": 1},
                "lag_valid": {"$sum": {"$cond": [{"$eq": ["$routing_lag_status", "ok"]}, 1, 0]}},
                "lag_negative": {
                    "$sum": {"$cond": [{"$eq": ["$routing_lag_status", "negative"]}, 1, 0]}
                },
                "lag_mean_days": {"$avg": valid_lag},
                "lag_median_days": {"$median": {"input": valid_lag, "method": "approximate"}},
                "lag_p90_days": {
                    "$percentile": {"input": valid_lag, "p": [0.9], "method": "approximate"}
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "segment": "$_id",
                "complaints": 1,
                "lag_valid": 1,
                "lag_negative": 1,
                "lag_missing_or_invalid": {
                    "$subtract": ["$complaints", {"$add": ["$lag_valid", "$lag_negative"]}]
                },
                "lag_mean_days": {"$round": ["$lag_mean_days", 2]},
                "lag_median_days": 1,
                "lag_p90_days": {"$arrayElemAt": ["$lag_p90_days", 0]},
            }
        },
        {"$sort": {"segment": 1}},
    ]


def top_companies(
    filters: Mapping[str, Any] | None = None, limit: int = 10, min_cell: int = MIN_CELL
) -> list:
    """Q8. Companies with the largest share of complaints in the selection.

    A share of complaints, NOT a quality ranking: company size and customer base drive volume.
    """
    return [
        *_count_by(filters, "company", "company", min_cell),
        {
            "$setWindowFields": {
                "partitionBy": "$segment",
                "sortBy": {"complaints": -1},
                "output": {"rank": {"$rank": {}}},
            }
        },
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"segment": 1, "rank": 1}},
    ]


CATALOGUE = {
    "monthly_trend": monthly_trend,
    "issue_mix": issue_mix,
    "state_distribution": state_distribution,
    "channel_mix": channel_mix,
    "company_response_mix": company_response_mix,
    "timely_rate": timely_rate,
    "routing_lag_summary": routing_lag_summary,
    "top_companies": top_companies,
}


def run(db: Any, pipeline: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return list(db[COMPLAINTS].aggregate(list(pipeline), allowDiskUse=True))


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print the results of the query catalogue.")
    parser.add_argument("--query", choices=sorted(CATALOGUE), help="one query (default: all)")
    parser.add_argument("--segment", action="append", help="filter, repeatable")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--state", action="append")
    parser.add_argument("--limit", type=int, default=15, help="rows printed per query")
    parser.add_argument("--show-pipeline", action="store_true")
    args = parser.parse_args(argv)

    from database.mongo import get_db

    db = get_db()
    filters = {
        "segment": args.segment,
        "date_from": args.date_from,
        "date_to": args.date_to,
        "state": args.state,
    }
    names = [args.query] if args.query else list(CATALOGUE)
    for name in names:
        pipeline = CATALOGUE[name](filters)
        print(f"\n=== {name}: {CATALOGUE[name].__doc__.splitlines()[0]}")
        if args.show_pipeline:
            print(json.dumps(pipeline, indent=2, default=str))
        rows = run(db, pipeline)
        for row in rows[: args.limit]:
            print(json.dumps(row, default=str))
        if len(rows) > args.limit:
            print(f"... {len(rows) - args.limit} more rows")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
