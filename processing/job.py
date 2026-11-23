"""Turn 3 Spark job: raw zone -> curated complaints, aggregates, quarantine and quality report.

Usage (inside the Spark container, the platform must be running):

    docker compose run --rm spark python -m processing.job --list-labels   # product labels found
    docker compose run --rm spark python -m processing.job --dry-run       # measure, write nothing
    docker compose run --rm spark python -m processing.job                 # full run
    docker compose run --rm spark python -m processing.job --sample        # committed sample, local

Outputs (bucket ``datalake``; ``--sample`` writes the same layout under ``data/processing_out/``):

    curated/complaints/option=<n>/year=YYYY/month=MM/   Snappy Parquet, rewritten every run
    curated/aggregates/option=<n>/<name>/               Snappy Parquet, rewritten every run
    quarantine/run_id=<id>/                             rejected rows with reject_reason
    quality/run_id=<id>/quality_report.json             6 dimensions, gate, reconciliation

Every run reprocesses all complete raw batches (newest historical load + every API run), so a
replay of the same input gives the same curated count: nothing is appended.

Exit codes: 0 pass or warn, 1 quality gate failed (curated data not written), 2 configuration.
"""

from __future__ import annotations

import argparse
import json
import logging
import secrets
import sys
from dataclasses import asdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from common.config import REPO_ROOT, ConfigError, load_settings
from processing import aggregates, quality
from processing import transform as T
from processing.readers import InputBatch, read_batches, sample_batches

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

LOG = logging.getLogger("processing.job")
STATES_PATH = REPO_ROOT / "config" / "reference" / "states.csv"
SAMPLE_DIR = REPO_ROOT / "data" / "sample"
SAMPLE_OUT = REPO_ROOT / "data" / "processing_out"

# The committed sample uses placeholder products, so --sample runs with this demo scope.
SAMPLE_SETTINGS = {
    "domain": {
        "option": 0,
        "slug": "sample",
        "products": ["Product A", "Product B", "Product C"],
        "segments": {"segment_ab": ["Product A", "Product B"], "segment_c": ["Product C"]},
    }
}


def new_run_id(now: datetime) -> str:
    return f"spark_{now.strftime('%Y%m%dT%H%M%SZ')}_{secrets.token_hex(3)}"


def load_states(path: Path = STATES_PATH) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line.split(",")[0].strip() for line in lines[1:] if line.strip()]


# --------------------------------------------------------------------------- outputs


class Output:
    """Where results go: MinIO (``s3a://bucket``) or a local folder, same key layout."""

    def __init__(self, base_uri: str, store: Any) -> None:
        self.base_uri = base_uri.rstrip("/")
        self.store = store  # ingestion.storage RawStore, for the JSON report

    def uri(self, key: str) -> str:
        return f"{self.base_uri}/{key}"

    def write_json(self, key: str, document: dict[str, Any]) -> None:
        data = json.dumps(document, indent=2, default=str).encode("utf-8")
        self.store.put_bytes(key, data, content_type="application/json")


# --------------------------------------------------------------------------- the job


def run(
    spark: SparkSession,
    batches: list[InputBatch],
    scope: T.Scope,
    rules: dict[str, Any],
    states: list[str],
    *,
    today: date,
    run_id: str,
    output: Output | None,
    min_cell: int = 10,
    code_version: str = "unknown",
    inject_defect: int = 0,
) -> dict[str, Any]:
    """Process the batches and return the quality report. Writes only if ``output`` is given
    and the gate does not fail."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    started_at = datetime.now(UTC)
    raw = T.parse(T.standardise(read_batches(spark, batches)), states, today)
    raw = raw.withColumn("reject_reason", T.reject_reason_identity(raw)).withColumn(
        "_in_scope", F.coalesce(T.in_scope(scope), F.lit(False))
    )

    # ---- pass 1 over the raw data: one small aggregate gives every raw-level count
    # (rows per batch, reject reasons, product label timeline, rows in scope). The 19-million-row
    # input is never cached: only this summary and the in-scope rows are kept.
    stats = (
        raw.groupBy("source_run_id", "product", "reject_reason", "_in_scope")
        .agg(
            F.count("*").alias("rows"),
            F.min("date_received").alias("first"),
            F.max("date_received").alias("last"),
        )
        .collect()
    )
    per_batch: dict[str, int] = {}
    for r in stats:
        per_batch[r["source_run_id"]] = per_batch.get(r["source_run_id"], 0) + r["rows"]
    raw_rows = sum(per_batch.values())
    manifest_diff = 0
    for batch in batches:
        if batch.source == "full_load" and batch.expected_records is not None:
            manifest_diff += abs(per_batch.get(batch.run_id, 0) - batch.expected_records)
    identified_rows = sum(r["rows"] for r in stats if r["reject_reason"] is None)
    labels = _label_timeline(stats, scope)

    # ---- pass 2: keep only what later steps need (identity rejects + in-scope rows)
    work = raw.where(F.col("reject_reason").isNotNull() | F.col("_in_scope")).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    # ---- step 3: identity rejects
    rejected_identity = work.where(F.col("reject_reason").isNotNull())

    # ---- step 4: scope. Row-level product filter; API rows are already filtered server-side to
    # the same products, so a complaint cannot be re-labelled out of scope in a newer version that
    # we hold. Sub-product and issue filters (if configured) are part of `in_scope`.
    scoped = work.where(F.col("reject_reason").isNull() & F.col("_in_scope"))
    scoped = scoped.withColumn("reject_reason", T.reject_reason_date()).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    scoped_rows = scoped.count()

    # ---- step 5: date rejects
    rejected_date = scoped.where(F.col("reject_reason").isNotNull())
    valid = T.rank_versions(T.with_record_hash(scoped.where(F.col("reject_reason").isNull())))
    valid = valid.persist(StorageLevel.MEMORY_AND_DISK)

    # ---- step 6-7: dedup, final scope
    losers = valid.where(F.col("version_rank") > 1)
    conflicts = losers.where(F.col("record_hash") != F.col("winner_hash")).withColumn(
        "reject_reason", F.lit(T.REJECT_DUPLICATE_CONFLICT)
    )
    winners = valid.where(F.col("version_rank") == 1)
    final = winners.where(T.in_scope(scope))
    curated = T.derive(final, scope).select(*T.CURATED_COLUMNS)
    if inject_defect:
        # Deliberate defect for the quality-gate demonstration (brief 6.3): simulate a broken
        # deduplication step by appending copies of existing curated records. The gate must catch
        # it (duplicate Complaint IDs, unbalanced reconciliation) and refuse to write.
        LOG.warning("INJECTING %d duplicate curated records (quality-gate demo)", inject_defect)
        curated = curated.unionByName(curated.orderBy("complaint_id").limit(inject_defect))
    curated = curated.persist(StorageLevel.MEMORY_AND_DISK)

    counts = {
        "raw_rows": raw_rows,
        "raw_rows_per_batch": per_batch,
        "rejected_identity": raw_rows - identified_rows,
        "filtered_out_of_scope": identified_rows - scoped_rows,
        "rejected_date": rejected_date.count(),
        "valid_in_scope": valid.count(),
        "duplicates_removed": losers.count(),
        "duplicate_conflicts": conflicts.count(),
        "filtered_out_after_dedup": winners.count() - final.count(),
        "curated": curated.count(),
    }
    counts["unbalanced_rows"] = counts["raw_rows"] - (
        counts["rejected_identity"]
        + counts["filtered_out_of_scope"]
        + counts["rejected_date"]
        + counts["duplicates_removed"]
        + counts["filtered_out_after_dedup"]
        + counts["curated"]
    )
    reject_reasons = {
        r["reject_reason"]: r["count"]
        for r in rejected_identity.unionByName(rejected_date)
        .groupBy("reject_reason")
        .count()
        .collect()
    }

    metrics, details = measure(curated, counts, reject_reasons, scoped_rows, rules, today, batches)
    metrics["reconciliation"]["raw_vs_manifest_difference"] = manifest_diff
    checks = quality.evaluate(metrics, rules)
    decision = quality.gate(checks)

    report: dict[str, Any] = {
        "run_id": run_id,
        "job": "processing.job",
        "code_version": code_version,
        "started_at": started_at.isoformat(),
        "run_date": today.isoformat(),
        "option": scope.option,
        "scope": {
            "segments": {k: list(v) for k, v in scope.segments.items()},
            "sub_products": list(scope.sub_products),
            "issues": list(scope.issues),
        },
        "rules_version": rules.get("version"),
        "injected_defect": (
            {"type": "duplicate_curated_ids", "rows": inject_defect} if inject_defect else None
        ),
        "inputs": [{k: v for k, v in asdict(b).items() if k != "header"} for b in batches],
        "gate": decision,
        "dimensions": quality.checks_by_dimension(checks),
        "reconciliation": counts,
        "reject_reasons": reject_reasons,
        "label_timeline": labels,
        **details,
    }

    if output is not None:
        quarantine = rejected_identity.unionByName(rejected_date, allowMissingColumns=True)
        quarantine = quarantine.select(*T.QUARANTINE_COLUMNS).unionByName(
            conflicts.select(*T.QUARANTINE_COLUMNS)
        )
        quarantine.coalesce(1).write.mode("errorifexists").parquet(
            output.uri(f"quarantine/run_id={run_id}")
        )
        if decision["status"] != quality.FAIL:
            write_curated(curated, aggregates.build(curated, min_cell), scope, output)
            report["outputs_written"] = True
        else:
            report["outputs_written"] = False
            LOG.error("Quality gate FAILED %s: curated data not written", decision["failed_checks"])
        report["finished_at"] = datetime.now(UTC).isoformat()
        output.write_json(f"quality/run_id={run_id}/quality_report.json", report)
    else:
        report["outputs_written"] = False
        report["finished_at"] = datetime.now(UTC).isoformat()

    for frame in (curated, valid, scoped, work):
        frame.unpersist()
    return report


def _label_timeline(stats: list[Any], scope: T.Scope) -> list[dict[str, Any]]:
    """Every product label found in the raw data, with first/last receipt date and row count.

    This is the evidence for historical label handling: CFPB renamed products in 2017, so the
    option's scope lists both the old and the new labels (see config/settings.yaml).
    """
    timeline: dict[str, dict[str, Any]] = {}
    for r in stats:
        if r["reject_reason"] is not None:
            continue
        entry = timeline.setdefault(
            r["product"],
            {
                "product": r["product"],
                "rows": 0,
                "first_date_received": None,
                "last_date_received": None,
                "in_scope": r["product"] in scope.products,
            },
        )
        entry["rows"] += r["rows"]
        if r["first"] is not None and (
            entry["first_date_received"] is None or r["first"] < entry["first_date_received"]
        ):
            entry["first_date_received"] = r["first"]
        if r["last"] is not None and (
            entry["last_date_received"] is None or r["last"] > entry["last_date_received"]
        ):
            entry["last_date_received"] = r["last"]
    return sorted(timeline.values(), key=lambda e: e["product"])


def measure(
    curated: DataFrame,
    counts: dict[str, Any],
    reject_reasons: dict[str, int],
    scoped_rows: int,
    rules: dict[str, Any],
    today: date,
    batches: list[InputBatch],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Measure every metric the rules refer to. Returns (metrics, extra report sections)."""
    from pyspark.sql import functions as F

    n = counts["curated"]
    rate = quality.rate
    completeness_rules = rules.get("completeness") or {}
    required = list(completeness_rules.get("required_fields") or [])
    optional_na = list(completeness_rules.get("not_applicable_fields") or [])
    not_in_source = list(completeness_rules.get("not_in_source_fields") or [])
    fields = [c for c in T.CURATED_COLUMNS if c not in ("quality_flags", "option", "record_hash")]

    missing_row = curated.select(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in fields],
        F.sum((F.col("state_is_valid") == F.lit(False)).cast("int")).alias("_invalid_state"),
        F.sum(F.array_contains("quality_flags", T.FLAG_INVALID_TIMELY).cast("int")).alias(
            "_invalid_timely"
        ),
        F.sum(F.array_contains("quality_flags", T.FLAG_INVALID_SENT_DATE).cast("int")).alias(
            "_invalid_sent"
        ),
        F.sum(F.array_contains("quality_flags", T.FLAG_NEGATIVE_LAG).cast("int")).alias(
            "_negative_lag"
        ),
        F.sum(F.col("segment").isNull().cast("int")).alias("_unmapped"),
        F.max("date_received").alias("_latest_record"),
        F.max("ingested_at").alias("_latest_ingestion"),
        F.countDistinct("complaint_id").alias("_distinct_ids"),
    ).first()
    row = missing_row.asDict() if missing_row else {}

    completeness_report = {
        c: {
            "missing": int(row.get(c) or 0),
            "missing_rate": rate(int(row.get(c) or 0), n),
            "applicability": (
                "required"
                if c in required
                else "not_in_source"
                if c in not_in_source
                else "not_always_applicable"
                if c in optional_na
                else "optional"
            ),
        }
        for c in fields
    }
    required_missing = max((completeness_report[c]["missing_rate"] for c in required), default=0.0)

    latest_record = row.get("_latest_record")
    latest_ingestion = row.get("_latest_ingestion")
    if latest_ingestion is None:
        stamps = [b.ingested_at for b in batches if b.ingested_at]
        latest_ingestion = max(stamps) if stamps else None
    ingestion_date = (
        latest_ingestion.date()
        if isinstance(latest_ingestion, datetime)
        else date.fromisoformat(str(latest_ingestion)[:10])
        if latest_ingestion
        else None
    )

    scoped_total = scoped_rows or 1
    metrics = {
        "uniqueness": {
            "curated_duplicate_ids": n - int(row.get("_distinct_ids") or 0),
            "curated_missing_ids": int(row.get("complaint_id") or 0),
        },
        "completeness": {
            "required_missing_rate": {
                "value": required_missing,
                "detail": f"highest missing rate among {required}",
            },
            "zip_code_missing_rate": completeness_report["zip_code"]["missing_rate"],
            "state_missing_rate": completeness_report["state"]["missing_rate"],
            "company_response_missing_rate": completeness_report["company_response"][
                "missing_rate"
            ],
        },
        "validity": {
            "invalid_date_received_rate": {
                "value": rate(
                    reject_reasons.get(T.REJECT_INVALID_DATE, 0)
                    + reject_reasons.get(T.REJECT_MISSING_DATE, 0),
                    scoped_total,
                ),
                "detail": "in-scope rows quarantined: missing or invalid receipt date",
            },
            "invalid_complaint_id_rate": rate(
                reject_reasons.get(T.REJECT_INVALID_ID, 0)
                + reject_reasons.get(T.REJECT_MISSING_ID, 0),
                counts["raw_rows"],
            ),
            "malformed_row_rate": rate(
                reject_reasons.get(T.REJECT_MALFORMED_ROW, 0), counts["raw_rows"]
            ),
            "invalid_state_rate": rate(int(row.get("_invalid_state") or 0), n),
            "invalid_timely_rate": rate(int(row.get("_invalid_timely") or 0), n),
            "invalid_date_sent_rate": rate(int(row.get("_invalid_sent") or 0), n),
            "negative_routing_lag_rate": rate(int(row.get("_negative_lag") or 0), n),
        },
        "consistency": {
            "unmapped_product_rate": rate(int(row.get("_unmapped") or 0), n),
        },
        "freshness": {
            "days_since_latest_record": {
                "value": (today - latest_record).days if latest_record else None,
                "detail": f"latest date_received {latest_record}",
            },
            "days_since_latest_ingestion": {
                "value": (today - ingestion_date).days if ingestion_date else None,
                "detail": f"latest ingestion {latest_ingestion}",
            },
        },
        "reconciliation": {
            "unbalanced_rows": abs(counts["unbalanced_rows"]),
        },
    }

    top = int((rules.get("consistency") or {}).get("report_top_combinations", 50))
    combinations = [
        r.asDict()
        for r in curated.groupBy("segment", "product", "sub_product", "issue", "sub_issue")
        .agg(
            F.count("*").alias("complaints"),
            F.min("date_received").alias("first_date_received"),
            F.max("date_received").alias("last_date_received"),
        )
        .orderBy(F.desc("complaints"))
        .limit(top)
        .collect()
    ]
    examples = {
        flag: [
            r.asDict()
            for r in curated.where(F.array_contains("quality_flags", flag))
            .select("complaint_id", "state", "date_received", "date_sent_to_company")
            .limit(5)
            .collect()
        ]
        for flag in (
            T.FLAG_INVALID_STATE,
            T.FLAG_INVALID_TIMELY,
            T.FLAG_INVALID_SENT_DATE,
            T.FLAG_NEGATIVE_LAG,
        )
    }
    details = {
        "completeness_by_field": completeness_report,
        "freshness": {
            "latest_date_received": latest_record,
            "latest_ingestion": latest_ingestion,
            "processed_at": datetime.now(UTC).isoformat(),
        },
        "top_combinations": combinations,
        "invalid_examples": examples,
    }
    return metrics, details


def write_curated(
    curated: DataFrame, aggs: dict[str, DataFrame], scope: T.Scope, output: Output
) -> None:
    """Rewrite the curated layer: the full result of this run replaces the previous one."""
    path = output.uri(f"curated/complaints/option={scope.option}")
    LOG.info("Writing curated records to %s", path)
    (
        curated.repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(path)
    )
    for name, frame in aggs.items():
        frame.coalesce(1).write.mode("overwrite").parquet(
            output.uri(f"curated/aggregates/option={scope.option}/{name}")
        )


# --------------------------------------------------------------------------- CLI


def _local_spark() -> SparkSession:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("cfpb-processing-sample")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _print_summary(report: dict[str, Any]) -> None:
    rec = report["reconciliation"]
    print(f"\nRun {report['run_id']}  option={report['option']}")
    print("Reconciliation:")
    for key in (
        "raw_rows",
        "rejected_identity",
        "filtered_out_of_scope",
        "rejected_date",
        "duplicates_removed",
        "filtered_out_after_dedup",
        "curated",
        "unbalanced_rows",
    ):
        print(f"  {key:<26}{rec[key]:>12,}")
    print("Checks:")
    for dimension, checks in report["dimensions"].items():
        for check in checks:
            print(f"  {check['status'].upper():<5} {dimension}.{check['name']} = {check['value']}")
    gate = report["gate"]
    print(f"\nQUALITY GATE: {gate['status'].upper()}", end="")
    print(f"  failed={gate['failed_checks']}" if gate["failed_checks"] else "")
    print(f"Outputs written: {report['outputs_written']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m processing.job", description=__doc__.split("\n")[0]
    )
    parser.add_argument("--sample", action="store_true", help="process data/sample locally")
    parser.add_argument("--dry-run", action="store_true", help="measure and report, write nothing")
    parser.add_argument("--list-labels", action="store_true", help="print product labels and stop")
    parser.add_argument("--rules", default=str(quality.DEFAULT_RULES_PATH))
    parser.add_argument("--config", default=None, help="settings YAML")
    parser.add_argument("--today", default=None, help="run date YYYY-MM-DD (default: UTC today)")
    parser.add_argument(
        "--inject-defect",
        type=int,
        default=0,
        metavar="N",
        help="demo only: add N duplicate curated records; the quality gate must fail",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    now = datetime.now(UTC)
    today = date.fromisoformat(args.today) if args.today else now.date()
    run_id = new_run_id(now)
    try:
        settings = load_settings(args.config) if args.config else load_settings()
        rules = quality.load_rules(args.rules)
        states = load_states()
        if args.sample:
            scope = T.Scope.from_settings(SAMPLE_SETTINGS)
            batches = sample_batches(SAMPLE_DIR, now.isoformat())
        else:
            scope = T.Scope.from_settings(settings) if not args.list_labels else None
            batches = _discover_batches(settings)
    except (ConfigError, quality.RulesError, ValueError, OSError) as exc:
        print(f"CONFIGURATION ERROR: {exc}", file=sys.stderr)
        return 2

    if args.sample:
        spark = _local_spark()
    else:
        from common.spark import get_spark

        spark = get_spark("cfpb-processing")

    try:
        if not batches:
            print(
                "NOTHING TO PROCESS: no complete raw batch (run ingestion first)", file=sys.stderr
            )
            return 2
        if args.list_labels:
            return _list_labels(spark, batches, states, today)

        output = None
        if not args.dry_run:
            output = _make_output(args.sample)
        from ingestion.version import code_version

        report = run(
            spark,
            batches,
            scope,
            rules,
            states,
            today=today,
            run_id=run_id,
            output=output,
            min_cell=int((settings.get("privacy") or {}).get("min_cell_size", 10)),
            code_version=code_version(),
            inject_defect=args.inject_defect,
        )
        _print_summary(report)
        if args.sample:
            local = SAMPLE_OUT / "quality" / f"run_id={run_id}" / "quality_report.json"
            print(f"Report: {local}" if output else "Dry run: nothing written")
        return 1 if report["gate"]["status"] == quality.FAIL else 0
    finally:
        spark.stop()


def _discover_batches(settings: dict[str, Any]) -> list[InputBatch]:
    from common.config import env
    from ingestion.discovery import completed
    from ingestion.layout import Layout
    from ingestion.storage import MinioStore
    from processing.readers import batches_from_discovery

    runs = completed(MinioStore.from_env(), Layout.from_settings(settings))
    return batches_from_discovery(runs, env("MINIO_BUCKET"))


def _make_output(sample: bool) -> Output:
    if sample:
        from ingestion.storage import LocalStore

        SAMPLE_OUT.mkdir(parents=True, exist_ok=True)
        return Output(SAMPLE_OUT.resolve().as_uri(), LocalStore(SAMPLE_OUT))
    from common.config import env
    from ingestion.storage import MinioStore

    return Output(f"s3a://{env('MINIO_BUCKET')}", MinioStore.from_env())


def _list_labels(
    spark: SparkSession, batches: list[InputBatch], states: list[str], today: date
) -> int:
    from pyspark.sql import functions as F

    df = T.parse(T.standardise(read_batches(spark, batches)), states, today)
    rows = (
        df.groupBy("product")
        .agg(
            F.count("*").alias("rows"),
            F.min("date_received").alias("first"),
            F.max("date_received").alias("last"),
        )
        .orderBy("product")
        .collect()
    )
    print(f"\n{'rows':>10}  {'first':<10}  {'last':<10}  product")
    for r in rows:
        print(f"{r['rows']:>10,}  {str(r['first']):<10}  {str(r['last']):<10}  {r['product']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
