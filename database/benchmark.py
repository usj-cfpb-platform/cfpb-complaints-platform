"""Index evidence for brief V2 6.4: the same queries before and after the dashboard indexes.

    docker compose run --rm spark python -m database.benchmark

For each representative dashboard query it runs ``explain`` (executionStats) and times
``--runs`` executions, first WITHOUT the dashboard indexes (only the unique ``complaint_id``
index remains), then WITH them. Results: a Markdown table on stdout and in
``docs/evidence/index_benchmark.md`` plus the raw numbers in ``index_benchmark.json``.
The indexes are always recreated at the end, even if a query fails.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.config import REPO_ROOT
from database import queries, schema

EVIDENCE_DIR = REPO_ROOT / "docs" / "evidence"

# Representative dashboard requests: segment + date range, plus one category filter.
CASES: list[tuple[str, Any, dict[str, Any]]] = [
    (
        "Monthly trend, transfers, 2023-2025",
        queries.monthly_trend,
        {"segment": "transfers_payments", "date_from": "2023-01-01", "date_to": "2025-12-31"},
    ),
    (
        "Issue mix, deposits, California, 2024",
        queries.issue_mix,
        {
            "segment": "deposit_accounts",
            "state": "CA",
            "date_from": "2024-01-01",
            "date_to": "2024-12-31",
        },
    ),
    (
        "Company response, deposits, one product, 2025",
        queries.company_response_mix,
        {
            "segment": "deposit_accounts",
            "product": "Checking or savings account",
            "date_from": "2025-01-01",
            "date_to": "2025-12-31",
        },
    ),
    (
        "Timely %, transfers, Web channel, 2022-2025",
        queries.timely_rate,
        {
            "segment": "transfers_payments",
            "channel": "Web",
            "date_from": "2022-01-01",
            "date_to": "2025-12-31",
        },
    ),
    (
        "Routing lag, transfers, last 12 months",
        queries.routing_lag_summary,
        {"segment": "transfers_payments", "date_from": "2025-11-01", "date_to": "2026-10-31"},
    ),
]


def _walk(node: Any) -> Any:
    if isinstance(node, Mapping):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def summarize_explain(explain: Mapping[str, Any]) -> dict[str, Any]:
    """Pull the comparable numbers out of an explain document.

    MongoDB 7 returns either top-level ``executionStats`` (pipeline pushed into the slot-based
    engine) or ``stages[0].$cursor.executionStats`` (classic engine); both shapes are handled.
    """
    stats: Mapping[str, Any] = {}
    plan_stages: list[str] = []
    for node in _walk(explain):
        if not stats and isinstance(node.get("executionStats"), Mapping):
            stats = node["executionStats"]
        if isinstance(node.get("winningPlan"), Mapping):
            plan_stages += [
                str(n["stage"])
                for n in _walk(node["winningPlan"])
                if isinstance(n.get("stage"), str)
            ]
    index_names = sorted(
        {str(n["indexName"]) for n in _walk(explain) if isinstance(n.get("indexName"), str)}
    )
    if "IXSCAN" in plan_stages:
        access = "IXSCAN"
    elif "COLLSCAN" in plan_stages:
        access = "COLLSCAN"
    else:
        access = plan_stages[-1] if plan_stages else "unknown"
    return {
        "access": access,
        "index": ", ".join(index_names) or "-",
        "docs_examined": stats.get("totalDocsExamined"),
        "keys_examined": stats.get("totalKeysExamined"),
        "returned": stats.get("nReturned"),
        "explain_ms": stats.get("executionTimeMillis"),
    }


def explain(db: Any, pipeline: list[dict[str, Any]]) -> dict[str, Any]:
    return db.command(
        {
            "explain": {"aggregate": schema.COMPLAINTS, "pipeline": pipeline, "cursor": {}},
            "verbosity": "executionStats",
        }
    )


def time_runs(db: Any, pipeline: list[dict[str, Any]], runs: int) -> float:
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        queries.run(db, pipeline)
        times.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(times), 1)


def measure(db: Any, runs: int) -> list[dict[str, Any]]:
    results = []
    for title, builder, filters in CASES:
        pipeline = builder(filters)
        results.append(
            {
                "query": title,
                "filters": filters,
                **summarize_explain(explain(db, pipeline)),
                "median_ms": time_runs(db, pipeline, runs),
            }
        )
    return results


def to_markdown(before: list[dict[str, Any]], after: list[dict[str, Any]], meta: dict) -> str:
    lines = [
        "# Index benchmark (before / after dashboard indexes)",
        "",
        f"Generated {meta['generated_at']} on {meta['documents']} documents in `complaints`, "
        f"median of {meta['runs']} runs per query. Same data and same query conditions in both "
        "columns; only the dashboard indexes differ (`complaint_id` unique index kept).",
        "",
        "| Query | Before: plan | Docs examined | Median ms | After: plan (index) "
        "| Docs examined | Keys examined | Median ms | Speed-up |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for b, a in zip(before, after, strict=True):
        speed = f"{b['median_ms'] / a['median_ms']:.1f}x" if a["median_ms"] else "-"
        lines.append(
            f"| {b['query']} | {b['access']} | {b['docs_examined']} | {b['median_ms']} "
            f"| {a['access']} ({a['index']}) | {a['docs_examined']} | {a['keys_examined']} "
            f"| {a['median_ms']} | {speed} |"
        )
    lines += [
        "",
        "Reading: without the indexes MongoDB scans every complaint (COLLSCAN) for each request. "
        "With the compound index (segment + category + date_received, Equality-Sort-Range order) "
        "it reads only the index range of the selected segment and period, so documents examined "
        "drop to the matching complaints.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Before/after index benchmark.")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out", default=str(EVIDENCE_DIR))
    args = parser.parse_args(argv)

    from database.mongo import get_db

    db = get_db()
    documents = db[schema.COMPLAINTS].estimated_document_count()
    if documents == 0:
        print("No documents in complaints: run database.load first.", file=sys.stderr)
        return 2
    try:
        schema.drop_dashboard_indexes(db)
        before = measure(db, args.runs)
    finally:
        schema.create_dashboard_indexes(db)
    after = measure(db, args.runs)

    meta = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "documents": documents,
        "runs": args.runs,
    }
    markdown = to_markdown(before, after, meta)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "index_benchmark.md").write_text(markdown, encoding="utf-8")
    (out / "index_benchmark.json").write_text(
        json.dumps({**meta, "before": before, "after": after}, indent=2, default=str),
        encoding="utf-8",
    )
    print(markdown)
    print(f"Written to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
