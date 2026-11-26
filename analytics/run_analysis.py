"""Turn 5 - reproducible analysis for option 15 (access to deposited and transferred funds).

Question (``config/settings.yaml``): how do reported delays and problems accessing funds differ
between deposit accounts and transfers/payments?

Reads MongoDB only through ``database.queries`` (brief 5, ``docs/data_contracts.md`` section 8)
and writes a JSON and a Markdown result file. Run on the laptop after the Turn 4 load::

    docker compose run --rm spark python -m analytics.run_analysis
    # optional: --date-from 2018-01-01 --date-to 2025-12-31 --out-dir docs/evidence

Segment names come from ``config/settings.yaml``; no company or category is hard-coded.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from analytics import measures as m
from database import queries

Runner = Callable[[Sequence[Mapping[str, Any]]], list[dict[str, Any]]]

DEFAULT_OUT = Path("docs/evidence")


def collect(
    run: Runner, seg_a: str, seg_b: str, date_from: str | None = None, date_to: str | None = None
) -> dict[str, Any]:
    """Run every query and compute the Turn 5 measures. ``run`` executes one pipeline."""
    base: dict[str, Any] = {"segment": [seg_a, seg_b]}
    if date_from:
        base["date_from"] = date_from
    if date_to:
        base["date_to"] = date_to

    trend = run(queries.monthly_trend(base))
    timely_rows = run(queries.timely_rate(base))
    by_seg = m.by_segment(timely_rows)
    issues = run(queries.issue_mix(base))
    responses = run(queries.company_response_mix(base))
    channels = run(queries.channel_mix(base))
    lag = m.by_segment(run(queries.routing_lag_summary(base)))

    years = sorted({r["year_month"][:4] for r in trend})
    yearly = []
    for year in years:
        f = {**base, "date_from": f"{year}-01-01", "date_to": f"{year}-12-31"}
        rows = m.by_segment(run(queries.timely_rate(f)))
        entry = m.compare_timely(rows.get(seg_a), rows.get(seg_b))
        entry["year"] = year
        yearly.append(entry)

    volume: dict[str, int] = {}
    for r in trend:
        volume[r["segment"]] = volume.get(r["segment"], 0) + int(r["complaints"])

    return {
        "segments": [seg_a, seg_b],
        "filters": {k: v for k, v in base.items() if k != "segment"},
        "volume": volume,
        "timely_overall": m.compare_timely(by_seg.get(seg_a), by_seg.get(seg_b)),
        "timely_by_year": yearly,
        "routing_lag": {s: dict(lag[s]) if s in lag else None for s in (seg_a, seg_b)},
        "issue_top": {s: m.top_n(issues, s, "issue") for s in (seg_a, seg_b)},
        "issue_gaps": m.share_gaps(issues, "issue", seg_a, seg_b)[:10],
        "response_gaps": m.share_gaps(responses, "company_response", seg_a, seg_b),
        "channel_gaps": m.share_gaps(channels, "channel", seg_a, seg_b),
    }


def _fmt(value: Any) -> str:
    return "n/a" if value is None else f"{value:,}" if isinstance(value, int) else str(value)


def render_markdown(result: Mapping[str, Any], freshness: Mapping[str, Any] | None = None) -> str:
    a, b = result["segments"]
    lines = [f"# Turn 5 analysis results: {a} vs {b}", ""]
    if freshness:
        lines += [
            f"Data freshness: latest complaint received {freshness.get('latest_date_received')}, "
            f"serving layer refreshed {freshness.get('serving_refreshed_at')}.",
            "",
        ]
    lines += [f"Filters: {result['filters'] or 'none (all dates)'}", ""]
    lines += ["## Volume", "", "| Segment | Complaints |", "|---|---:|"]
    lines += [f"| {s} | {_fmt(result['volume'].get(s, 0))} |" for s in (a, b)]

    t = result["timely_overall"]
    lines += ["", "## Timely company response (Yes / (Yes + No))", ""]
    lines += [
        "| Segment | Valid | Timely % | 95 % CI | Missing/invalid |",
        "|---|---:|---:|---|---:|",
    ]
    for s, key in ((a, "a"), (b, "b")):
        x = t[key]
        lines.append(
            f"| {s} | {_fmt(x['valid'])} | {_fmt(x['timely_pct'])} | {_fmt(x['ci95_pct'])} "
            f"| {_fmt(x['missing_or_invalid'])} |"
        )
    g = t["gap"]
    if g:
        lines += [
            "",
            f"Gap {a} minus {b}: {g['gap_pp']} pp, 95 % CI {g['ci95_pp']}, p = {g['p_value']:.3g}.",
        ]

    lines += ["", "### By year (counts re-summed, never averaged)", ""]
    lines += [f"| Year | {a} % | {b} % | Gap (pp) |", "|---|---:|---:|---:|"]
    for y in result["timely_by_year"]:
        gap = y["gap"]["gap_pp"] if y["gap"] else None
        lines.append(
            f"| {y['year']} | {_fmt(y['a']['timely_pct'])} | {_fmt(y['b']['timely_pct'])} "
            f"| {_fmt(gap)} |"
        )

    lines += ["", "## Routing lag (CFPB receipt to referral, not resolution time)", ""]
    lines += ["| Segment | Valid | Mean | Median | P90 |", "|---|---:|---:|---:|---:|"]
    for s in (a, b):
        r = result["routing_lag"].get(s) or {}
        lines.append(
            f"| {s} | {_fmt(r.get('lag_valid'))} | {_fmt(r.get('lag_mean_days'))} "
            f"| {_fmt(r.get('lag_median_days'))} | {_fmt(r.get('lag_p90_days'))} |"
        )

    for title, key, label in (
        ("Largest issue-share gaps", "issue_gaps", "issue"),
        ("Company response mix gaps", "response_gaps", "company_response"),
        ("Submission channel gaps", "channel_gaps", "channel"),
    ):
        lines += ["", f"## {title} (share within segment, pp = {a} minus {b})", ""]
        lines += [f"| {label} | {a} % | {b} % | Gap (pp) | Reliable |", "|---|---:|---:|---:|---|"]
        for r in result[key]:
            lines.append(
                f"| {r[label]} | {r[f'{a}_share_pct']} | {r[f'{b}_share_pct']} "
                f"| {r['gap_pp']} | {'yes' if r['reliable'] else 'no (small cell)'} |"
            )
    return "\n".join(lines) + "\n"


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Turn 5 analysis on MongoDB.")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--name", default="turn5_analysis_results")
    args = parser.parse_args(argv)

    from common.config import load_settings
    from database.mongo import get_db

    segments = list(load_settings()["domain"]["segments"])
    if len(segments) != 2:
        print(f"expected exactly two segments in config, found {segments}", file=sys.stderr)
        return 2
    db = get_db()
    result = collect(
        lambda p: queries.run(db, p), segments[0], segments[1], args.date_from, args.date_to
    )
    last = db.pipeline_runs.find_one(sort=[("finished_at", -1)]) or {}
    freshness = last.get("freshness")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / f"{args.name}.json").write_text(
        json.dumps(result, indent=2, default=str), encoding="utf-8"
    )
    md = render_markdown(result, freshness)
    (args.out_dir / f"{args.name}.md").write_text(md, encoding="utf-8")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
