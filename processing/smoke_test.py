"""Infrastructure smoke test: Spark writes Snappy Parquet to MinIO and reads it back."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

from common.config import env
from common.spark import get_spark

LOG = logging.getLogger("spark_smoke_test")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = get_spark("cfpb-smoke-test")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = f"s3a://{env('MINIO_BUCKET')}/_smoke/run={stamp}"

    rows = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    spark.createDataFrame(rows, ["id", "label"]).write.mode("overwrite").parquet(path)
    count = spark.read.parquet(path).count()
    spark.stop()

    if count != len(rows):
        LOG.error("Smoke test FAILED: wrote %d rows, read %d from %s", len(rows), count, path)
        return 1
    LOG.info("Smoke test PASSED: %d rows round-tripped through %s", count, path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
