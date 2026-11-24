"""Which fields differ between the CSV version and the newest API version of the same complaint?

Read-only diagnostic (writes nothing). It explains the ``updated`` count of a MongoDB load: a
document is updated when ``record_hash`` changes, and the hash is computed on source values.

    docker compose run --rm spark python -m scripts.diagnose_versions

For every Complaint ID present in the newest full CSV load and in at least one API batch, the
newest API version is compared with the CSV version, field by field:

* ``raw``    - the value as received (what ``record_hash`` uses today);
* ``parsed`` - after typing (dates as dates, Yes/No as booleans, tags as a sorted list).

A field that differs in ``raw`` but not in ``parsed`` is a format difference, not a content change.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from common.config import load_settings
from processing import transform as T
from processing.job import _discover_batches, load_states
from processing.readers import read_batches
from processing.schema import SOURCE_FIELDS

RAW_COLUMN = {
    "date_received": "date_received_raw",
    "date_sent_to_company": "date_sent_to_company_raw",
    "timely": "timely_raw",
}


def main() -> int:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from common.spark import get_spark

    settings = load_settings()
    scope = T.Scope.from_settings(settings)
    batches = _discover_batches(settings)
    spark = get_spark("cfpb-diagnose-versions")
    try:
        df = T.parse(
            T.standardise(read_batches(spark, batches)), load_states(), datetime.now(UTC).date()
        )
        df = df.where(
            T.reject_reason_identity(df).isNull() & F.coalesce(T.in_scope(scope), F.lit(False))
        )
        df = df.withColumn("tags_parsed", F.array_join(F.array_sort("tags"), "|"))
        fields = [f for f in SOURCE_FIELDS if f != "complaint_id"]
        keep = ["complaint_id", "tags_parsed"] + fields + list(RAW_COLUMN.values())

        csv = df.where(F.col("source") == "full_load").select(*keep)
        newest = Window.partitionBy("complaint_id").orderBy(F.col("ingested_at").desc())
        api = (
            df.where(F.col("source") == "api")
            .withColumn("_r", F.row_number().over(newest))
            .where("_r = 1")
            .select(*keep)
        )
        pairs = csv.alias("c").join(api.alias("a"), "complaint_id").cache()

        def raw(side: str, f: str):
            if f == "tags":
                return F.coalesce(F.col(f"{side}.tags_parsed"), F.lit(""))
            return F.coalesce(F.col(f"{side}.{RAW_COLUMN.get(f, f)}").cast("string"), F.lit(""))

        def parsed(side: str, f: str):
            if f == "tags":
                return F.coalesce(F.col(f"{side}.tags_parsed"), F.lit(""))
            return F.coalesce(F.col(f"{side}.{f}").cast("string"), F.lit(""))

        exprs = []
        for f in fields:
            exprs.append(F.sum((raw("c", f) != raw("a", f)).cast("int")).alias(f"raw__{f}"))
            exprs.append(
                F.sum((parsed("c", f) != parsed("a", f)).cast("int")).alias(f"parsed__{f}")
            )
        any_raw = F.greatest(*[(raw("c", f) != raw("a", f)).cast("int") for f in fields])
        any_parsed = F.greatest(*[(parsed("c", f) != parsed("a", f)).cast("int") for f in fields])
        exprs += [
            F.count("*").alias("pairs"),
            F.sum(any_raw).alias("any_raw"),
            F.sum(any_parsed).alias("any_parsed"),
        ]
        row = pairs.agg(*exprs).collect()[0]

        print(f"\nComplaints in the CSV and in an API batch : {row['pairs']:,}")
        print(f"  differ in at least one RAW field      : {row['any_raw']:,}")
        print(f"  differ in at least one PARSED field   : {row['any_parsed']:,}  (real changes)")
        print(f"\n{'field':<26}{'raw diff':>10}{'parsed diff':>13}")
        for f in fields:
            print(f"{f:<26}{row['raw__' + f]:>10,}{row['parsed__' + f]:>13,}")

        print("\nExamples (CSV -> API) for fields that differ:")
        for f in fields:
            if row["raw__" + f]:
                ex = (
                    pairs.where(raw("c", f) != raw("a", f))
                    .select("complaint_id", raw("c", f).alias("csv"), raw("a", f).alias("api"))
                    .limit(2)
                    .collect()
                )
                for e in ex:
                    print(
                        f"  {f:<24} id={e['complaint_id']}: {e['csv'][:40]!r} -> {e['api'][:40]!r}"
                    )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
