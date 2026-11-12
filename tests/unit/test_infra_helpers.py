import pytest

from common.spark import spark_conf
from scripts.init_storage import parse_endpoint


def test_parse_endpoint_http() -> None:
    assert parse_endpoint("http://minio:9000") == ("minio:9000", False)


def test_parse_endpoint_https() -> None:
    assert parse_endpoint("https://s3.example.org") == ("s3.example.org", True)


@pytest.mark.parametrize("bad", ["minio:9000", "ftp://minio:9000", ""])
def test_parse_endpoint_rejects_bad_values(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_endpoint(bad)


def test_spark_conf_is_laptop_sized_and_points_to_minio() -> None:
    conf = spark_conf("http://minio:9000", "user", "secret", jars=["a.jar", "b.jar"])
    assert conf["spark.sql.shuffle.partitions"] == "8"
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.sql.parquet.compression.codec"] == "snappy"
    assert conf["spark.jars"] == "a.jar,b.jar"


def test_spark_conf_without_jars_has_no_jars_key() -> None:
    assert "spark.jars" not in spark_conf("http://minio:9000", "u", "s")
