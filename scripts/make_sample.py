"""Create the small development sample in ``data/sample/`` (issue "Development sample").

Tests and CI never need the 19-million-row dataset. By default the sample is **synthetic**:
deterministic, generic product/company names, no real people or firms, and it contains the edge
cases Spark must survive:

* a complaint with a **missing state**,
* a complaint with an **invalid date**,
* a **duplicate complaint_id** (same id, changed company response),
* a row whose quoted field **contains a newline**.

Outputs (committed, small):
    data/sample/complaints_sample.csv      CSV in the historical-file layout
    data/sample/api_response_0001.json     API pages in the search-API layout (2 pages)
    data/sample/api_response_0002.json

Run:    python -m scripts.make_sample
Option: python -m scripts.make_sample --from-zip complaints.csv.zip --rows 200
        (copies the first N real rows from the downloaded ZIP instead of synthetic ones)
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import random
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from common.config import REPO_ROOT

DEFAULT_OUT = REPO_ROOT / "data" / "sample"
# Header of the historical CSV as documented in docs/examples/manifest.example.json.
# To be confirmed against the real file with `python -m ingestion.historical --inspect`.
EXAMPLE_MANIFEST = REPO_ROOT / "docs" / "examples" / "manifest.example.json"
SEED = 20261114
PRODUCTS = ["Product A", "Product B", "Product C"]
ISSUES = ["Issue one", "Issue two", "Issue three"]
STATES = ["CA", "NY", "TX", "FL", "IL"]
RESPONSES = ["Closed with explanation", "Closed with monetary relief", "In progress"]
BASE_DATE = date(2026, 10, 1)


def csv_header() -> list[str]:
    manifest = json.loads(EXAMPLE_MANIFEST.read_text(encoding="utf-8"))
    return list(manifest["header"])


def synthetic_records(count: int = 40, seed: int = SEED) -> list[dict[str, Any]]:
    """Deterministic records in API (snake_case) form, including the edge cases."""
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []
    for index in range(count):
        received = BASE_DATE + timedelta(days=index % 30)
        records.append(
            {
                "complaint_id": str(9_000_000 + index),
                "date_received": received.isoformat(),
                "date_sent_to_company": (received + timedelta(days=rng.randint(0, 3))).isoformat(),
                "product": rng.choice(PRODUCTS),
                "sub_product": None,
                "issue": rng.choice(ISSUES),
                "sub_issue": None,
                "company": f"Company {rng.randint(1, 5)}",
                "company_public_response": None,
                "company_response": rng.choice(RESPONSES),
                "state": rng.choice(STATES),
                "zip_code": f"{rng.randint(100, 999)}XX",
                "tags": None,
                "submitted_via": rng.choice(["Web", "Phone", "Referral"]),
                "timely": rng.choice(["Yes", "No"]),
            }
        )
    # --- edge cases ---------------------------------------------------------------------
    records[3]["state"] = None  # missing state
    records[7]["date_received"] = "2026-13-45"  # invalid date
    records[10]["issue"] = "Issue with a\nline break"  # newline inside a quoted CSV field
    duplicate = dict(records[12])
    duplicate["company_response"] = "Closed with relief"  # same complaint_id, changed content
    records.append(duplicate)
    return records


def csv_rows(records: list[dict[str, Any]]) -> list[list[str]]:
    """Map API-form records onto the historical CSV column order."""
    mapping = {
        "Date received": "date_received",
        "Product": "product",
        "Sub-product": "sub_product",
        "Issue": "issue",
        "Sub-issue": "sub_issue",
        "Company public response": "company_public_response",
        "Company": "company",
        "State": "state",
        "ZIP code": "zip_code",
        "Tags": "tags",
        "Consumer consent provided?": None,
        "Submitted via": "submitted_via",
        "Date sent to company": "date_sent_to_company",
        "Company response to consumer": "company_response",
        "Timely response?": "timely",
        "Consumer disputed?": None,
        "Complaint ID": "complaint_id",
    }
    header = csv_header()
    rows = []
    for rec in records:
        row = []
        for column in header:
            key = mapping.get(column)
            value = rec.get(key) if key else None
            row.append("" if value is None else str(value))
        rows.append(row)
    return rows


def api_page(records: list[dict[str, Any]], total: int, page_number: int) -> dict[str, Any]:
    """A response in the search-API layout (``hits.hits[]._source`` + ``_meta``)."""
    hits = []
    for rec in records:
        received = rec["date_received"]
        try:
            epoch_ms = int(datetime.fromisoformat(received).replace(tzinfo=UTC).timestamp() * 1000)
        except ValueError:
            epoch_ms = 0
        hits.append(
            {
                "_index": "complaint-public-v2",
                "_id": rec["complaint_id"],
                "_score": None,
                "_source": rec,
                "sort": [epoch_ms, rec["complaint_id"]],
            }
        )
    return {
        "_meta": {
            "license": "CC0",
            "last_updated": "2026-11-13T12:00:00-05:00",
            "last_indexed": "2026-11-13T12:00:00-05:00",
            "total_record_count": total,
            "is_data_stale": False,
            "has_data_issue": False,
            "break_points": {},
        },
        "hits": {"total": {"value": total, "relation": "eq"}, "max_score": None, "hits": hits},
        "page": page_number,
    }


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def sample_from_zip(zip_path: Path, rows: int) -> tuple[list[str], list[list[str]]]:
    """First ``rows`` data rows of the real CSV, streamed (never loads the whole file)."""
    with zipfile.ZipFile(zip_path) as archive:
        name = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
        with archive.open(name) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
            header = next(reader)
            return header, [row for _, row in zip(range(rows), reader, strict=False)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--from-zip", help="sample real rows from a downloaded complaints ZIP")
    parser.add_argument("--rows", type=int, default=200)
    args = parser.parse_args(argv)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    records = synthetic_records()

    if args.from_zip:
        header, rows = sample_from_zip(Path(args.from_zip), args.rows)
        write_csv(out / "complaints_sample.csv", header, rows)
    else:
        write_csv(out / "complaints_sample.csv", csv_header(), csv_rows(records))

    half = len(records) // 2
    pages = [records[:half], records[half:]]
    for number, chunk in enumerate(pages, start=1):
        document = api_page(chunk, total=len(records), page_number=number)
        (out / f"api_response_{number:04d}.json").write_text(
            json.dumps(document, indent=2) + "\n", encoding="utf-8"
        )
    print(f"Sample written to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
