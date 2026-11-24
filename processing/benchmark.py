"""CSV vs Parquet benchmark on the option-15 curated scope (Session 3.1 evidence).

The same curated records are written twice, once as CSV (header, uncompressed, the format CFPB
publishes) and once as Snappy Parquet (our curated format). Then three typical reads are timed on
both copies, each repeated ``--repeats`` times (median reported):

* ``full_scan_count``  read every row and count them;
* ``column_aggregate`` complaints per segment and year (2 columns: Parquet reads only those);
* ``filtered_count``   one segment and one year (Parquet can skip row groups using statistics).

The CSV read uses an explicit schema (no type inference) and ``multiLine`` (fields contain line
breaks), the same options the historical CSV needs in ``processing.readers``.
Usage (platform running, curated data present):

    docker compose run --rm spark python -m processing.benchmark
    docker compose run --rm spark python -m processing.benchmark --sample   # data/processing_out

Writes ``docs/evidence/csv_vs_parquet_benchmark.json`` and ``.md``; the two copies stay under
``benchmark/run_id=<id>/`` in the bucket (or ``data/processing_out/``).
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from common.config import REPO_ROOT

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

LOG = logging.getLogger("processing.benchmark")
EVIDENCE_DIR = REPO_ROOT / "docs" / "evidence"
SAMPLE_OUT = REPO_ROOT / "data" / "processing_out"
REPORT_NAME = "csv_vs_parquet_benchmark"


def flat_copy(curated: DataFrame) -> DataFrame:
    """Curated records with array columns joined into text, so both formats hold the same data."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType

    columns = []
    for field in curated.schema.fields:
        if isinstance(field.dataType, ArrayType):
            columns.append(F.array_join(F.col(field.name), "|").alias(field.name))
        else:
            columns.append(F.col(field.name))
    return curated.select(*columns)


def queries(segment: str, year: int) -> dict[str, Callable[[DataFrame], Any]]:
    from pyspark.sql import functions as F

    return {
        "full_scan_count": lambda df: df.count(),
        "column_aggregate": lambda df: df.groupBy("segment", "year").count().collect(),
        "filtered_count": lambda df: df.where(
            (F.col("segment") == segment) & (F.col("year") == year)
        ).count(),
    }


def timed(action: Callable[[], Any], repeats: int) -> tuple[float, list[float], Any]:
    """Median seconds over ``repeats`` runs, all runs, and the last result."""
    runs, result = [], None
    for _ in range(repeats):
        start = time.perf_counter()
        result = action()
        runs.append(round(time.perf_counter() - start, 3))
    return statistics.median(runs), runs, result


def run(
    spark: SparkSession,
    curated_uri: str,
    out_uri: str,
    size_of: Callable[[str], int],
    *,
    repeats: int = 3,
) -> dict[str, Any]:
    """Write both copies under ``out_uri`` and time the queries. ``size_of(sub)`` returns the
    total bytes of the data files under ``out_uri/sub``."""
    curated = flat_copy(spark.read.parquet(curated_uri))
    rows = curated.count()
    top = curated.groupBy("segment", "year").count().orderBy("count", ascending=False).first()
    segment, year = (top["segment"], int(top["year"])) if top else ("", 0)
    schema = curated.schema

    LOG.info("Writing %d rows as CSV and as Parquet under %s", rows, out_uri)
    # Quotes doubled ("") as in the CFPB file; fields may contain line breaks, so the CSV must be
    # read with multiLine, exactly like the historical file in processing.readers.
    curated.coalesce(1).write.mode("overwrite").option("header", True).option("escape", '"').csv(
        f"{out_uri}/csv"
    )
    curated.coalesce(1).write.mode("overwrite").parquet(f"{out_uri}/parquet")

    readers = {
        "csv": lambda: (
            spark.read.schema(schema)
            .option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(f"{out_uri}/csv")
        ),
        "parquet": lambda: spark.read.parquet(f"{out_uri}/parquet"),
    }
    results: dict[str, Any] = {}
    for fmt, reader in readers.items():
        results[fmt] = {"bytes": size_of(fmt), "queries": {}}
        for name, query in queries(segment, year).items():
            spark.catalog.clearCache()
            median, runs, value = timed(lambda q=query, r=reader: q(r()), repeats)
            results[fmt]["queries"][name] = {"median_seconds": median, "runs_seconds": runs}
            if name != "column_aggregate":
                results[fmt]["queries"][name]["result"] = value

    for name in queries(segment, year):
        csv_s = results["csv"]["queries"][name]["median_seconds"]
        pq_s = results["parquet"]["queries"][name]["median_seconds"]
        results["parquet"]["queries"][name]["speedup_vs_csv"] = (
            round(csv_s / pq_s, 2) if pq_s else None
        )
    csv_b, pq_b = results["csv"]["bytes"], results["parquet"]["bytes"]
    same = all(
        results["csv"]["queries"][q].get("result") == results["parquet"]["queries"][q].get("result")
        for q in ("full_scan_count", "filtered_count")
    )
    return {
        "benchmark": "csv_vs_parquet",
        "created_at": datetime.now(UTC).isoformat(),
        "input": curated_uri,
        "copies": out_uri,
        "rows": rows,
        "repeats": repeats,
        "filter_used": {"segment": segment, "year": year},
        "results": results,
        "size_ratio_csv_over_parquet": round(csv_b / pq_b, 2) if pq_b else None,
        "same_results_both_formats": same,
    }


def to_markdown(report: dict[str, Any]) -> str:
    res, mb = report["results"], 1024 * 1024
    flt = report["filter_used"]
    lines = [
        "# CSV vs Parquet benchmark (option 15 curated scope)",
        "",
        f"Created {report['created_at'][:19]}Z with `python -m processing.benchmark`. Input: "
        f"`{report['input']}` ({report['rows']:,} rows). Same records in both formats (array "
        "columns joined as text). Each query run "
        f"{report['repeats']} times, median shown. CSV read with an explicit schema and "
        "`multiLine` (as the real CFPB file requires).",
        "",
        "| | CSV (uncompressed) | Parquet (Snappy) | CSV / Parquet |",
        "|---|---|---|---|",
        f"| Size | {res['csv']['bytes'] / mb:,.1f} MB | "
        f"{res['parquet']['bytes'] / mb:,.1f} MB | "
        f"{report['size_ratio_csv_over_parquet']}x |",
    ]
    labels = {
        "full_scan_count": "Read all rows (count)",
        "column_aggregate": "Complaints per segment and year (2 columns)",
        "filtered_count": f"Filter `{flt['segment']}`, year {flt['year']} (count)",
    }
    for name, label in labels.items():
        c = res["csv"]["queries"][name]["median_seconds"]
        p = res["parquet"]["queries"][name]
        lines.append(
            f"| {label} | {c:.2f} s | {p['median_seconds']:.2f} s | {p['speedup_vs_csv']}x |"
        )
    lines += [
        "",
        f"Both formats returned the same counts: **{report['same_results_both_formats']}**.",
        "",
        "Why (Session 3.1): Parquet is columnar and compressed, so it stores repeated labels "
        "(product, issue, company) compactly, reads only the columns a query needs, and skips "
        "row groups whose min/max statistics exclude a filter. CSV is row-based text: every query "
        "reads and parses every byte. Timings come from one laptop and vary between runs; the "
        "size difference does not.",
        "",
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m processing.benchmark")
    parser.add_argument("--sample", action="store_true", help="use data/processing_out locally")
    parser.add_argument("--option", type=int, default=None, help="default: domain.option")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--out-dir", default=str(EVIDENCE_DIR), help="where to write the report")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_id = f"bench_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"

    if args.sample:
        from processing.job import _local_spark

        option = 0 if args.option is None else args.option
        spark = _local_spark()
        base = SAMPLE_OUT.resolve()
        # Plain file: URIs ("=" must not be percent-encoded as Path.as_uri() would do).
        curated_uri = f"file://{base / 'curated' / 'complaints' / f'option={option}'}"
        out_dir = base / "benchmark" / f"run_id={run_id}"
        out_uri = f"file://{out_dir}"

        def size_of(sub: str) -> int:
            return sum(
                p.stat().st_size
                for p in (out_dir / sub).rglob("*")
                if p.is_file() and not p.name.startswith((".", "_"))
            )
    else:
        from common.config import env, load_settings
        from common.spark import get_spark
        from ingestion.storage import MinioStore

        option = args.option or int(load_settings()["domain"]["option"])
        bucket = env("MINIO_BUCKET")
        store = MinioStore.from_env()
        spark = get_spark("cfpb-benchmark")
        curated_uri = f"s3a://{bucket}/curated/complaints/option={option}"
        prefix = f"benchmark/run_id={run_id}"
        out_uri = f"s3a://{bucket}/{prefix}"

        def size_of(sub: str) -> int:
            return sum(
                o.size
                for o in store.list_objects(f"{prefix}/{sub}/")
                if not o.key.rsplit("/", 1)[-1].startswith((".", "_"))
            )

    try:
        report = run(spark, curated_uri, out_uri, size_of, repeats=args.repeats)
    finally:
        spark.stop()
    report["run_id"] = run_id
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{REPORT_NAME}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (out / f"{REPORT_NAME}.md").write_text(to_markdown(report), encoding="utf-8")
    print(to_markdown(report))
    print(f"Written: {out / REPORT_NAME}.json and .md")
    return 0 if report["same_results_both_formats"] else 1


if __name__ == "__main__":
    sys.exit(main())
