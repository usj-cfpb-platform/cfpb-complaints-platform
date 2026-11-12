"""Shared SparkSession factory: local mode, tuned for a student laptop, wired to MinIO via S3A."""

from __future__ import annotations

import glob
import os
from typing import TYPE_CHECKING

from common.config import env

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def spark_conf(
    endpoint: str,
    access_key: str,
    secret_key: str,
    driver_memory: str = "4g",
    shuffle_partitions: int = 8,
    jars: list[str] | None = None,
) -> dict[str, str]:
    """Build the Spark configuration as a plain dict (unit-testable without Java)."""
    conf = {
        "spark.driver.memory": driver_memory,
        # Default is 200 partitions: far too many for one laptop and small data.
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
    }
    if jars:
        conf["spark.jars"] = ",".join(jars)
    return conf


def get_spark(app_name: str) -> SparkSession:
    """Create (or reuse) the SparkSession for a job."""
    from pyspark.sql import SparkSession

    jars_dir = os.environ.get("SPARK_EXTRA_JARS_DIR", "/opt/spark-jars")
    conf = spark_conf(
        endpoint=env("MINIO_ENDPOINT"),
        access_key=env("MINIO_ROOT_USER"),
        secret_key=env("MINIO_ROOT_PASSWORD"),
        driver_memory=env("SPARK_DRIVER_MEMORY", "4g"),
        jars=sorted(glob.glob(os.path.join(jars_dir, "*.jar"))),
    )
    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
