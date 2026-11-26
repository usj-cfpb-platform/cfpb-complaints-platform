"""Pure statistics for the Turn 5 analysis (no database, no I/O).

Everything here takes plain numbers or the row dicts returned by ``database.queries`` so that it
can be unit-tested without MongoDB. Rules (brief V2 section 7, ``docs/data_contracts.md`` s. 8):

* proportions are always recomputed from summed counts, never averaged across months or years;
* a difference between two segments is reported in percentage points, with a confidence interval;
* a small difference on a huge sample can be "significant" and still be irrelevant, so every
  comparison also carries the effect size (the gap itself), never only a p-value.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

Z_95 = 1.959963984540054  # two-sided 95 % normal quantile


def pct(numerator: float, denominator: float) -> float | None:
    """Percentage rounded to 2 decimals; ``None`` (not 0) when the denominator is empty."""
    if not denominator:
        return None
    return round(100.0 * numerator / denominator, 2)


def wilson_interval(successes: int, n: int, z: float = Z_95) -> tuple[float, float] | None:
    """Wilson score interval for a proportion, in percent. ``None`` when ``n`` is 0."""
    if n <= 0:
        return None
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return round(100 * max(0.0, centre - half), 2), round(100 * min(1.0, centre + half), 2)


def two_proportion_gap(s1: int, n1: int, s2: int, n2: int) -> dict[str, Any] | None:
    """Gap ``p1 - p2`` in percentage points, 95 % CI (unpooled) and two-sided z-test p-value."""
    if n1 <= 0 or n2 <= 0:
        return None
    p1, p2 = s1 / n1, s2 / n2
    gap = p1 - p2
    se = math.sqrt(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2)
    pooled = (s1 + s2) / (n1 + n2)
    se_pooled = math.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2))
    if se_pooled == 0:
        p_value = 1.0 if gap == 0 else 0.0
    else:
        z = gap / se_pooled
        p_value = math.erfc(abs(z) / math.sqrt(2))
    return {
        "gap_pp": round(100 * gap, 2),
        "ci95_pp": (round(100 * (gap - Z_95 * se), 2), round(100 * (gap + Z_95 * se), 2)),
        "p_value": p_value,
    }


def by_segment(rows: Iterable[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {r["segment"]: r for r in rows}


def timely_summary(row: Mapping[str, Any] | None) -> dict[str, Any]:
    """Timely % with Wilson CI from one ``timely_rate`` row (Yes / (Yes + No))."""
    row = row or {}
    yes, no = int(row.get("timely_yes", 0)), int(row.get("timely_no", 0))
    valid = yes + no
    total = int(row.get("complaints", 0))
    return {
        "complaints": total,
        "timely_yes": yes,
        "timely_no": no,
        "valid": valid,
        "missing_or_invalid": total - valid,
        "timely_pct": pct(yes, valid),
        "ci95_pct": wilson_interval(yes, valid),
    }


def sum_timely(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """Add yearly counts back together (never average yearly percentages)."""
    out = {"complaints": 0, "timely_yes": 0, "timely_no": 0}
    for r in rows:
        for key in out:
            out[key] += int(r.get(key, 0))
    return out


def compare_timely(a: Mapping[str, Any] | None, b: Mapping[str, Any] | None) -> dict[str, Any]:
    """Timely-rate gap ``a - b`` (percentage points) with CI and p-value."""
    sa, sb = timely_summary(a), timely_summary(b)
    gap = two_proportion_gap(sa["timely_yes"], sa["valid"], sb["timely_yes"], sb["valid"])
    return {"a": sa, "b": sb, "gap": gap}


def share_gaps(
    rows: Iterable[Mapping[str, Any]], label: str, seg_a: str, seg_b: str, min_cell: int = 10
) -> list[dict[str, Any]]:
    """Per category: share in each segment and gap ``a - b`` in percentage points.

    ``rows`` come from ``issue_mix`` / ``company_response_mix`` / ``channel_mix`` (share inside
    segment). A category absent from one segment has share 0 there. A gap is flagged
    ``reliable`` only when both sides reach ``min_cell`` complaints or the category is absent.
    """
    cells: dict[str, dict[str, Mapping[str, Any]]] = {}
    for r in rows:
        cells.setdefault(r[label], {})[r["segment"]] = r
    out = []
    for cat, per in cells.items():
        a, b = per.get(seg_a), per.get(seg_b)
        ca, cb = int(a["complaints"]) if a else 0, int(b["complaints"]) if b else 0
        sa = float(a["share_pct"]) if a else 0.0
        sb = float(b["share_pct"]) if b else 0.0
        out.append(
            {
                label: cat,
                f"{seg_a}_complaints": ca,
                f"{seg_b}_complaints": cb,
                f"{seg_a}_share_pct": sa,
                f"{seg_b}_share_pct": sb,
                "gap_pp": round(sa - sb, 2),
                "reliable": (ca >= min_cell or ca == 0) and (cb >= min_cell or cb == 0),
            }
        )
    out.sort(key=lambda r: abs(r["gap_pp"]), reverse=True)
    return out


def top_n(rows: Sequence[Mapping[str, Any]], segment: str, label: str, n: int = 5) -> list[dict]:
    seg = [dict(r) for r in rows if r["segment"] == segment]
    seg.sort(key=lambda r: r["complaints"], reverse=True)
    return [
        {label: r[label], "complaints": r["complaints"], "share_pct": r["share_pct"]}
        for r in seg[:n]
    ]
