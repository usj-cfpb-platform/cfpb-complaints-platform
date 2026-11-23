"""Read completed raw batches into one DataFrame with the curated field names and lineage.

Inputs come from ``ingestion.discovery`` (only complete batches, report section 11), or from the
committed sample in ``data/sample/`` for development and tests. Nothing here cleans values: every
field is still the raw string, so the transform step can report what was invalid.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from processing.schema import (
    SOURCE_FIELDS,
    api_struct,
    csv_struct,
    validate_csv_header,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

LOG = logging.getLogger("processing.readers")

SOURCE_FULL_LOAD = "full_load"
SOURCE_API = "api"
CORRUPT_COLUMN = "_corrupt_record"


@dataclass
class InputBatch:
    """One complete raw batch to process."""

    source: str  # "full_load" or "api"
    run_id: str
    ingested_at: str  # ISO 8601 UTC, from the manifest / run metadata
    paths: list[str]  # s3a:// or local paths
    header: list[str] | None = None  # CSV only (from the manifest)
    expected_records: int | None = None  # from the manifest / run metadata
    extra: dict[str, Any] = field(default_factory=dict)


def batches_from_discovery(runs: Iterable[Any], bucket: str) -> list[InputBatch]:
    """Turn ``ingestion.discovery.RawRun`` objects into input batches.

    Only the **newest** complete historical load is used: an older full load is fully contained
    in a newer one, so reading both would only add duplicates. Every complete API run is used.
    """
    runs = list(runs)
    full = [run for run in runs if run.source == "full"]
    api = [run for run in runs if run.source == "api"]
    batches: list[InputBatch] = []
    if full:
        newest = full[-1]  # discovery sorts by run timestamp
        doc = newest.document or {}
        batches.append(
            InputBatch(
                source=SOURCE_FULL_LOAD,
                run_id=newest.run_id,
                ingested_at=doc.get("download_finished_at", ""),
                paths=[f"s3a://{bucket}/{key}" for key in newest.data_keys],
                header=list(doc.get("header", [])),
                expected_records=newest.record_count,
                extra={"skipped_older_full_loads": [run.run_id for run in full[:-1]]},
            )
        )
    for run in api:
        doc = run.document or {}
        batches.append(
            InputBatch(
                source=SOURCE_API,
                run_id=run.run_id,
                ingested_at=doc.get("finished_at", ""),
                paths=[f"s3a://{bucket}/{key}" for key in run.data_keys],
                expected_records=run.record_count,
            )
        )
    return batches


def sample_batches(sample_dir: Path, ingested_at: str) -> list[InputBatch]:
    """Input batches for the committed development sample (no MinIO needed)."""
    csv_path = sample_dir / "complaints_sample.csv"
    with csv_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = sum(1 for _ in reader)
    pages = sorted(str(p) for p in sample_dir.glob("api_response_*.json"))
    batches = [
        InputBatch(
            source=SOURCE_FULL_LOAD,
            run_id="full_sample",
            ingested_at=ingested_at,
            paths=[str(csv_path)],
            header=header,
            expected_records=rows,
        )
    ]
    if pages:
        batches.append(
            InputBatch(source=SOURCE_API, run_id="api_sample", ingested_at=ingested_at, paths=pages)
        )
    return batches


def _with_lineage(df: DataFrame, batch: InputBatch) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        df.withColumn("source", F.lit(batch.source))
        .withColumn("source_run_id", F.lit(batch.run_id))
        .withColumn("ingested_at", F.lit(batch.ingested_at or None).cast("timestamp"))
    )


def read_csv_batch(spark: SparkSession, batch: InputBatch) -> DataFrame:
    """Read the historical CSV with an explicit all-string schema built from its header.

    ``multiLine`` is required because quoted fields contain newlines; ``escape='"'`` handles
    doubled quotes inside fields. A row with the wrong number of fields lands in
    ``_corrupt_record`` instead of being silently shifted.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField

    if not batch.header:
        raise ValueError(f"CSV batch {batch.run_id} has no header in its manifest")
    rename = validate_csv_header(batch.header)
    schema = csv_struct(batch.header)
    schema.add(StructField(CORRUPT_COLUMN, StringType(), True))
    df = (
        spark.read.schema(schema)
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .option("encoding", "UTF-8")
        .csv(batch.paths)
    )
    columns = [F.col(f"`{csv_name}`").alias(name) for csv_name, name in rename.items()]
    df = df.select(*columns, F.col(CORRUPT_COLUMN))
    return _with_lineage(_conform(df), batch)


def read_api_batch(spark: SparkSession, batch: InputBatch) -> DataFrame:
    """Read raw API pages: one JSON document per page, records in ``hits.hits[]._source``."""
    from pyspark.sql import functions as F

    pages = spark.read.schema(api_struct()).option("multiLine", True).json(batch.paths)
    df = pages.select(F.explode_outer("hits.hits").alias("hit")).where(F.col("hit").isNotNull())
    df = df.select("hit._source.*").withColumn(CORRUPT_COLUMN, F.lit(None).cast("string"))
    return _with_lineage(_conform(df), batch)


def _conform(df: DataFrame) -> DataFrame:
    """Same columns, same order, for every source; absent source fields become null."""
    from pyspark.sql import functions as F

    present = set(df.columns)
    columns = [
        F.col(name) if name in present else F.lit(None).cast("string").alias(name)
        for name in SOURCE_FIELDS
    ]
    return df.select(*columns, F.col(CORRUPT_COLUMN))


def read_batches(spark: SparkSession, batches: list[InputBatch]) -> DataFrame:
    """Union of every batch, in the conformed layout."""
    frames = []
    for batch in batches:
        reader = read_csv_batch if batch.source == SOURCE_FULL_LOAD else read_api_batch
        LOG.info("Reading %s batch %s (%d object(s))", batch.source, batch.run_id, len(batch.paths))
        frames.append(reader(spark, batch))
    if not frames:
        raise ValueError("No complete raw batch to process (run ingestion first)")
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result
