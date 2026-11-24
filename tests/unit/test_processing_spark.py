"""Spark transformation tests (brief section 10): invalid dates, missing values, domain filters,
historical labels, duplicate Complaint IDs, reconciliation and replay.

Run inside the Spark image, or wherever PySpark and Java are installed (skipped otherwise).
"""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from ingestion.storage import LocalStore  # noqa: E402
from processing import job, quality  # noqa: E402
from processing import transform as T  # noqa: E402
from processing.readers import InputBatch, read_batches, sample_batches  # noqa: E402

TODAY = date(2026, 11, 22)
STATES = job.load_states()
HEADER = [
    "Date received", "Product", "Sub-product", "Issue", "Sub-issue", "Company public response",
    "Company", "State", "ZIP code", "Tags", "Submitted via", "Date sent to company",
    "Company response to consumer", "Timely response?", "Complaint ID",
]  # fmt: skip
SCOPE = T.Scope.from_settings(
    {
        "domain": {
            "option": 15,
            "slug": "test",
            "products": [
                "Checking or savings account",
                "Bank account or service",
                "Money transfers",
            ],
            "segments": {
                "deposit_accounts": ["Checking or savings account", "Bank account or service"],
                "transfers_payments": ["Money transfers"],
            },
        }
    }
)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def row(cid, received="2026-10-01", product="Checking or savings account", **kw) -> dict:
    base = {
        "Date received": received,
        "Product": product,
        "Issue": "Managing an account",
        "Company": "Bank X",
        "State": "NY",
        "ZIP code": "100XX",
        "Submitted via": "Web",
        "Date sent to company": "2026-10-02",
        "Company response to consumer": "Closed with explanation",
        "Timely response?": "Yes",
        "Complaint ID": cid,
    }
    base.update(kw)
    return base


def write_csv(path: Path, rows: list[dict]) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=HEADER)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in HEADER})
    return path


def csv_batch(path: Path, run_id="full_t", ingested="2026-11-01T00:00:00Z", expected=None):
    with path.open(encoding="utf-8", newline="") as handle:
        count = sum(1 for _ in csv.reader(handle)) - 1
    return InputBatch(
        source="full_load",
        run_id=run_id,
        ingested_at=ingested,
        paths=[str(path)],
        header=HEADER,
        expected_records=count if expected is None else expected,
    )


def api_record(cid, response: str) -> dict:
    return {
        "complaint_id": cid,
        "date_received": "2026-10-01",
        "product": "Checking or savings account",
        "issue": "Managing an account",
        "company": "Bank X",
        "company_response": response,
    }


def api_batch(tmp_path: Path, records: list[dict], run_id: str, ingested: str) -> InputBatch:
    page = tmp_path / f"{run_id}.json"
    page.write_text(
        json.dumps({"hits": {"hits": [{"_source": r} for r in records]}}), encoding="utf-8"
    )
    return InputBatch(source="api", run_id=run_id, ingested_at=ingested, paths=[str(page)])


def run_job(spark, batches, tmp_path=None, scope=SCOPE, rules=None, inject_defect=0):
    output = None
    if tmp_path is not None:
        out = tmp_path / "out"
        out.mkdir(exist_ok=True)
        output = job.Output(out.resolve().as_uri(), LocalStore(out))
    return job.run(
        spark,
        batches,
        scope,
        rules or quality.load_rules(),
        STATES,
        today=TODAY,
        run_id=job.new_run_id(__import__("datetime").datetime(2026, 11, 22, 10, 0, 0)),
        output=output,
        inject_defect=inject_defect,
    )


def parsed(spark, batches):
    return T.parse(T.standardise(read_batches(spark, batches)), STATES, TODAY)


# --------------------------------------------------------------------------- parsing


def test_dates_valid_invalid_missing_and_other_formats(spark, tmp_path) -> None:
    path = write_csv(
        tmp_path / "d.csv",
        [
            row("1", "2026-10-01"),
            row("2", "2026-13-45"),  # impossible date
            row("3", ""),  # missing
            row("4", "10/05/2026"),  # US format of older files
            row("5", "2026-10-07T00:00:00-04:00"),  # API timestamp
            row("6", "2031-01-01"),  # in the future
            row("7", "2005-01-01"),  # before CFPB existed
        ],
    )
    got = {
        r["complaint_id"]: r["date_received"] for r in parsed(spark, [csv_batch(path)]).collect()
    }
    assert got == {
        "1": date(2026, 10, 1),
        "2": None,
        "3": None,
        "4": date(2026, 10, 5),
        "5": date(2026, 10, 7),
        "6": None,
        "7": None,
    }


def test_missing_values_trimmed_and_normalised(spark, tmp_path) -> None:
    tags = "Older American, Servicemember"
    path = write_csv(
        tmp_path / "m.csv",
        [
            row("1", **{"State": "  ny ", "Timely response?": "yes", "Tags": tags}),
            row("2", **{"State": "", "Timely response?": "", "ZIP code": "   "}),
            row("3", **{"State": "ZZ", "Timely response?": "Maybe"}),
        ],
    )
    rows = {r["complaint_id"]: r for r in parsed(spark, [csv_batch(path)]).collect()}
    assert rows["1"]["state"] == "NY" and rows["1"]["state_is_valid"] is True
    assert rows["1"]["timely"] is True
    assert rows["1"]["tags"] == ["Older American", "Servicemember"]
    assert rows["2"]["state"] is None and rows["2"]["state_is_valid"] is None
    assert rows["2"]["zip_code"] is None and rows["2"]["timely"] is None
    assert rows["3"]["state_is_valid"] is False
    assert rows["3"]["timely"] is None and rows["3"]["timely_raw"] == "Maybe"


def test_quoted_newline_and_malformed_row(spark, tmp_path) -> None:
    path = tmp_path / "q.csv"
    good = write_csv(tmp_path / "tmp.csv", [row("1", Issue="Line one\nline two")])
    text = good.read_text(encoding="utf-8") + 'bad,row,"only three fields"\n'
    path.write_text(text, encoding="utf-8")
    report = run_job(spark, [csv_batch(path)])
    assert report["reconciliation"]["raw_rows"] == 2
    assert report["reject_reasons"].get(T.REJECT_MALFORMED_ROW) == 1
    assert report["reconciliation"]["curated"] == 1


# --------------------------------------------------------------------------- filter, labels, dedup


def test_domain_filter_with_historical_labels_and_segments(spark, tmp_path) -> None:
    path = write_csv(
        tmp_path / "f.csv",
        [
            row("1", "2016-05-01", product="Bank account or service"),  # pre-2017 label
            row("2", product="Checking or savings account"),
            row("3", product="Money transfers"),
            row("4", product="Mortgage"),  # other option
            row("5", product=""),  # cannot be classified
        ],
    )
    report = run_job(spark, [csv_batch(path)], tmp_path)
    rec = report["reconciliation"]
    assert rec["curated"] == 3
    assert rec["filtered_out_of_scope"] == 1
    assert report["reject_reasons"] == {T.REJECT_MISSING_PRODUCT: 1}
    curated = spark.read.parquet(str(tmp_path / "out" / "curated" / "complaints" / "option=15"))
    seg = {r["complaint_id"]: (r["product"], r["segment"]) for r in curated.collect()}
    assert seg["1"] == ("Bank account or service", "deposit_accounts")  # original label kept
    assert seg["3"] == ("Money transfers", "transfers_payments")
    timeline = {e["product"]: e for e in report["label_timeline"]}
    assert timeline["Bank account or service"]["in_scope"] is True
    assert timeline["Mortgage"]["in_scope"] is False


def test_dedup_newest_ingestion_wins_then_api_over_full_load(spark, tmp_path) -> None:
    path = write_csv(
        tmp_path / "h.csv",
        [row("1", **{"Company response to consumer": "In progress"}), row("2"), row("3")],
    )
    full = csv_batch(path, ingested="2026-11-01T00:00:00Z")
    # complaint_id as an integer, as in the API spec: the reader must turn it into "1".
    newer = api_batch(
        tmp_path, [api_record(1, "Closed with explanation")], "api_new", "2026-11-10T00:00:00Z"
    )
    same_time = api_batch(
        tmp_path, [api_record("2", "Closed with relief")], "api_same", "2026-11-01T00:00:00Z"
    )
    report = run_job(spark, [full, newer, same_time], tmp_path)
    assert report["reconciliation"]["duplicates_removed"] == 2
    assert report["reconciliation"]["duplicate_conflicts"] == 2
    assert report["dimensions"]["uniqueness"][0]["value"] == 0
    curated = spark.read.parquet(str(tmp_path / "out" / "curated" / "complaints" / "option=15"))
    by_id = {r["complaint_id"]: r for r in curated.collect()}
    assert by_id["1"]["company_response"] == "Closed with explanation"  # newest ingestion
    assert by_id["1"]["source"] == "api"
    assert by_id["2"]["source"] == "api"  # tie on ingested_at -> api preferred
    assert by_id["3"]["source"] == "full_load"


def test_same_complaint_with_api_timestamps_is_not_a_conflict(spark, tmp_path) -> None:
    """DEF-08: the API writes dates as timestamps; only a real field change is a conflict."""
    path = write_csv(tmp_path / "f.csv", [row("7"), row("8", **{"Company public response": ""})])
    full = csv_batch(path, ingested="2026-11-01T00:00:00Z")
    same = {
        "date_received": "2026-10-01T00:20:56.000Z",
        "date_sent_to_company": "2026-10-02T00:33:21.000Z",
        "product": "Checking or savings account",
        "issue": "Managing an account",
        "company": "Bank X",
        "state": "NY",
        "zip_code": "100XX",
        "submitted_via": "Web",
        "company_response": "Closed with explanation",
        "timely": "Yes",
    }
    changed = {**same, "company_public_response": "Company has responded to the consumer"}
    api = api_batch(
        tmp_path,
        [{**same, "complaint_id": 7}, {**changed, "complaint_id": 8}],
        "api_ts",
        "2026-11-10T00:00:00Z",
    )
    report = run_job(spark, [full, api], tmp_path)
    assert report["reconciliation"]["duplicates_removed"] == 2
    assert report["reconciliation"]["duplicate_conflicts"] == 1  # only id 8 really changed
    curated = spark.read.parquet(str(tmp_path / "out" / "curated" / "complaints" / "option=15"))
    by_id = {r["complaint_id"]: r for r in curated.collect()}
    assert str(by_id["7"]["date_received"]) == "2026-10-01"
    assert by_id["8"]["company_public_response"] == "Company has responded to the consumer"


def test_routing_lag_negative_missing_and_invalid(spark, tmp_path) -> None:
    path = write_csv(
        tmp_path / "l.csv",
        [
            row("1", "2026-10-05", **{"Date sent to company": "2026-10-08"}),
            row("2", "2026-10-05", **{"Date sent to company": "2026-10-01"}),
            row("3", "2026-10-05", **{"Date sent to company": ""}),
            row("4", "2026-10-05", **{"Date sent to company": "not a date"}),
        ],
    )
    # 2 of 4 rows with bad referral dates would (correctly) fail the real gate; relax it here so
    # the test checks the interval logic itself.
    relaxed = quality.load_rules()
    for name in ("invalid_date_sent_rate", "negative_routing_lag_rate"):
        relaxed["validity"][name] = {"warn_above": 0, "fail_above": 1}
    report = run_job(spark, [csv_batch(path)], tmp_path, rules=relaxed)
    assert report["dimensions"]["validity"]  # measured
    curated = spark.read.parquet(str(tmp_path / "out" / "curated" / "complaints" / "option=15"))
    got = {
        r["complaint_id"]: (r["routing_lag_days"], r["routing_lag_status"], r["quality_flags"])
        for r in curated.collect()
    }
    assert got["1"] == (3, T.LAG_OK, [])
    assert got["2"] == (-4, T.LAG_NEGATIVE, [T.FLAG_NEGATIVE_LAG])
    assert got["3"] == (None, T.LAG_MISSING, [T.FLAG_MISSING_SENT_DATE])
    assert got["4"] == (None, T.LAG_INVALID, [T.FLAG_INVALID_SENT_DATE])


# --------------------------------------------------------------------------- whole job


def test_sample_reconciles_exactly_and_replay_is_not_inflated(spark, tmp_path) -> None:
    sample_scope = T.Scope.from_settings(job.SAMPLE_SETTINGS)
    batches = sample_batches(job.SAMPLE_DIR, "2026-11-22T00:00:00Z")
    first = run_job(spark, batches, tmp_path, scope=sample_scope)
    rec = first["reconciliation"]
    assert rec["unbalanced_rows"] == 0
    assert rec["raw_rows"] == 82  # 41 CSV rows + 41 API records
    assert first["gate"]["status"] != quality.FAIL
    # Replay the same input: the curated layer is rewritten, not appended.
    second = run_job(spark, batches, tmp_path, scope=sample_scope)
    curated = spark.read.parquet(str(tmp_path / "out" / "curated" / "complaints" / "option=0"))
    assert second["reconciliation"]["curated"] == rec["curated"] == curated.count()
    assert curated.select("complaint_id").distinct().count() == curated.count()


def test_manifest_mismatch_fails_the_gate_and_blocks_curated_write(spark, tmp_path) -> None:
    path = write_csv(tmp_path / "g.csv", [row("1"), row("2")])
    report = run_job(spark, [csv_batch(path, expected=5)], tmp_path)
    assert report["gate"]["status"] == quality.FAIL
    assert "reconciliation.raw_vs_manifest_difference" in report["gate"]["failed_checks"]
    assert report["outputs_written"] is False
    assert not (tmp_path / "out" / "curated").exists()
    reports = list((tmp_path / "out" / "quality").glob("run_id=*/quality_report.json"))
    assert len(reports) == 1  # the failure itself is still documented


def test_injected_duplicate_defect_is_caught_and_nothing_is_written(spark, tmp_path) -> None:
    path = write_csv(tmp_path / "x.csv", [row(str(i)) for i in range(1, 11)])
    report = run_job(spark, [csv_batch(path)], tmp_path, inject_defect=3)
    assert report["injected_defect"] == {"type": "duplicate_curated_ids", "rows": 3}
    assert report["gate"]["status"] == quality.FAIL
    assert "uniqueness.curated_duplicate_ids" in report["gate"]["failed_checks"]
    assert "reconciliation.unbalanced_rows" in report["gate"]["failed_checks"]
    assert report["outputs_written"] is False
    assert not (tmp_path / "out" / "curated").exists()


def test_empty_api_run_is_skipped() -> None:
    from types import SimpleNamespace

    from processing.readers import batches_from_discovery

    full = SimpleNamespace(
        source="full", run_id="full_1", data_keys=["raw/x.csv"], record_count=2,
        document={"header": ["Complaint ID"], "download_finished_at": "2026-11-22T00:00:00Z"},
    )  # fmt: skip
    empty = SimpleNamespace(
        source="api", run_id="api_1", data_keys=[], record_count=0, document={"finished_at": "x"}
    )
    batches = batches_from_discovery([full, empty], "datalake")
    assert [b.run_id for b in batches] == ["full_1"]


def test_csv_vs_parquet_benchmark_same_results(spark, tmp_path) -> None:
    from processing import benchmark

    path = write_csv(
        tmp_path / "b.csv",
        [row(str(i), Issue="line one\nline two" if i == 3 else "Managing an account")
         for i in range(1, 30)],
    )  # fmt: skip
    run_job(spark, [csv_batch(path)], tmp_path)
    curated = f"file://{tmp_path / 'out' / 'curated' / 'complaints' / 'option=15'}"
    out_dir = tmp_path / "bench"

    def size_of(sub: str) -> int:
        return sum(p.stat().st_size for p in (out_dir / sub).rglob("*.csv")) + sum(
            p.stat().st_size for p in (out_dir / sub).rglob("*.parquet")
        )

    report = benchmark.run(spark, curated, f"file://{out_dir}", size_of, repeats=1)
    assert report["rows"] == 29
    assert report["same_results_both_formats"] is True
    assert report["results"]["csv"]["queries"]["full_scan_count"]["result"] == 29
    assert report["results"]["csv"]["bytes"] > 0 and report["results"]["parquet"]["bytes"] > 0
    assert "| Size |" in benchmark.to_markdown(report)
