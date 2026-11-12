"""Probe the CFPB Complaint API: is it reachable from this machine, and what does it return?

Standard library only, so it runs anywhere (laptop or GitHub Actions) without installs.
Saves the raw JSON response unchanged, plus a small metadata file, into an output folder.

Usage:
    python -m scripts.api_probe --out probe_output
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

API_BASE_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
USER_AGENT = "cfpb-complaints-platform/0.1 (USJ-ESIB student project; batch research use)"


def build_probe_url(base_url: str, today: date, window_days: int = 30, size: int = 5) -> str:
    """Build a small, date-bounded search request (no aggregations, few records)."""
    params = {
        "date_received_min": (today - timedelta(days=window_days)).isoformat(),
        "date_received_max": today.isoformat(),
        "size": str(size),
        "sort": "created_date_desc",
        "no_aggs": "true",
        "no_highlight": "true",
    }
    return f"{base_url}?{urllib.parse.urlencode(params)}"


def total_hits(payload: dict[str, Any]) -> int | None:
    """Read the total hit count; tolerate both `{"value": n}` and plain `n` shapes."""
    total = payload.get("hits", {}).get("total")
    if isinstance(total, dict):
        total = total.get("value")
    return total if isinstance(total, int) else None


def record_fields(payload: dict[str, Any]) -> list[str]:
    """Return the field names of the first record (names only, no values)."""
    hits = payload.get("hits", {}).get("hits", [])
    if not hits:
        return []
    source = hits[0].get("_source", hits[0])
    return sorted(source.keys()) if isinstance(source, dict) else []


def fetch(url: str, timeout_s: int, max_attempts: int) -> tuple[int, bytes, int, float]:
    """GET with bounded retries and exponential backoff. Returns (status, body, attempts, secs)."""
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        started = time.monotonic()
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
            )
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                body = response.read()
                return response.status, body, attempt, time.monotonic() - started
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code < 500 and exc.code != 429:
                raise  # client errors will not fix themselves
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        if attempt < max_attempts:
            wait = 2**attempt
            print(f"attempt {attempt} failed ({last_error!r}); retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"API unreachable after {max_attempts} attempts: {last_error!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="probe_output", help="output folder")
    parser.add_argument("--base-url", default=API_BASE_URL)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--attempts", type=int, default=4)
    args = parser.parse_args(argv)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    url = build_probe_url(args.base_url, started_at.date())

    try:
        status, body, attempts, seconds = fetch(url, args.timeout, args.attempts)
    except Exception as exc:  # report and fail clearly
        print(f"PROBE FAILED: {exc!r}")
        failure = {"url": url, "started_at": started_at.isoformat(), "error": repr(exc)}
        (out / "probe_metadata.json").write_text(json.dumps(failure, indent=2), encoding="utf-8")
        return 1

    (out / "response.json").write_bytes(body)  # raw, unmodified
    payload = json.loads(body)
    metadata = {
        "url": url,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "http_status": status,
        "attempts": attempts,
        "response_seconds": round(seconds, 2),
        "response_bytes": len(body),
        "response_sha256": hashlib.sha256(body).hexdigest(),
        "total_hits_in_window": total_hits(payload),
        "records_returned": len(payload.get("hits", {}).get("hits", [])),
        "record_fields": record_fields(payload),
        "top_level_keys": sorted(payload.keys()),
        "meta": payload.get("_meta"),
    }
    (out / "probe_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata, indent=2))
    print("PROBE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
