"""Quality gate: judge the measured metrics against ``config/quality_rules.yaml``.

Pure Python (no Spark): the job measures, this module decides PASS / WARN / FAIL. That keeps the
rules testable in CI without Java, and makes the decision logic easy to explain.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from common.config import REPO_ROOT

DEFAULT_RULES_PATH = REPO_ROOT / "config" / "quality_rules.yaml"

PASS, WARN, FAIL = "pass", "warn", "fail"
DIMENSIONS = (
    "uniqueness",
    "completeness",
    "validity",
    "consistency",
    "freshness",
    "reconciliation",
)


class RulesError(ValueError):
    """The rules file is missing a dimension or a threshold."""


@dataclass
class Check:
    dimension: str
    name: str
    value: float | int | None
    warn_above: float
    fail_above: float
    status: str
    detail: str = ""


def load_rules(path: Path | str = DEFAULT_RULES_PATH) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as handle:
        rules = yaml.safe_load(handle) or {}
    missing = [d for d in DIMENSIONS if d not in rules]
    if missing:
        raise RulesError(f"Quality rules file has no section for: {missing}")
    return rules


def judge(value: float | int | None, warn_above: float, fail_above: float) -> str:
    """FAIL above ``fail_above``, WARN above ``warn_above``, else PASS.

    A value that could not be measured (None) is a FAIL: a check that silently did not run must
    not let the run through.
    """
    if value is None:
        return FAIL
    if value > fail_above:
        return FAIL
    if value > warn_above:
        return WARN
    return PASS


def evaluate(metrics: Mapping[str, Mapping[str, Any]], rules: Mapping[str, Any]) -> list[Check]:
    """One ``Check`` per threshold found in both the rules and the metrics.

    ``metrics[dimension][name]`` is either a number or ``{"value": number, "detail": str}``.
    A threshold with no matching metric becomes a FAIL ("not measured").
    """
    checks: list[Check] = []
    for dimension in DIMENSIONS:
        section = rules.get(dimension) or {}
        measured = metrics.get(dimension) or {}
        for name, rule in section.items():
            if not isinstance(rule, Mapping) or "fail_above" not in rule:
                continue  # lists and settings such as required_fields
            if "warn_above" not in rule:
                raise RulesError(f"{dimension}.{name} needs warn_above and fail_above")
            entry = measured.get(name)
            if isinstance(entry, Mapping):
                value, detail = entry.get("value"), str(entry.get("detail", ""))
            else:
                value, detail = entry, ""
            if name not in measured:
                detail = "not measured by this run"
            checks.append(
                Check(
                    dimension=dimension,
                    name=name,
                    value=value,
                    warn_above=float(rule["warn_above"]),
                    fail_above=float(rule["fail_above"]),
                    status=judge(value, float(rule["warn_above"]), float(rule["fail_above"])),
                    detail=detail,
                )
            )
    return checks


def gate(checks: list[Check]) -> dict[str, Any]:
    """Overall decision: any FAIL stops the run; WARNs are listed."""
    failed = [f"{c.dimension}.{c.name}" for c in checks if c.status == FAIL]
    warned = [f"{c.dimension}.{c.name}" for c in checks if c.status == WARN]
    status = FAIL if failed else WARN if warned else PASS
    return {"status": status, "failed_checks": failed, "warned_checks": warned}


def checks_by_dimension(checks: list[Check]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {d: [] for d in DIMENSIONS}
    for check in checks:
        grouped[check.dimension].append(asdict(check))
    return grouped


def rate(part: int, whole: int) -> float:
    """Share with a safe zero denominator (0 rows -> 0.0)."""
    return round(part / whole, 6) if whole else 0.0
