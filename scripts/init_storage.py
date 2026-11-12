"""Create the MinIO bucket used by every layer. Safe to run repeatedly (idempotent)."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from common.config import env

if TYPE_CHECKING:
    from minio import Minio

LOG = logging.getLogger("init_storage")


def parse_endpoint(url: str) -> tuple[str, bool]:
    """Turn 'http://minio:9000' into ('minio:9000', secure=False)."""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"MINIO_ENDPOINT must look like http://host:port, got {url!r}")
    return parsed.netloc, parsed.scheme == "https"


def wait_and_create_bucket(client: Minio, bucket: str, timeout_s: int = 90) -> bool:
    """Wait until MinIO answers, then create the bucket if missing.

    Returns True if the bucket was created, False if it already existed.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            if client.bucket_exists(bucket):
                LOG.info("Bucket %s already exists", bucket)
                return False
            client.make_bucket(bucket)
            LOG.info("Created bucket %s", bucket)
            return True
        except Exception as exc:  # MinIO not ready yet (connection refused, 503, ...)
            if time.monotonic() > deadline:
                raise TimeoutError(f"MinIO not ready after {timeout_s}s") from exc
            LOG.info("Waiting for MinIO... (%s)", type(exc).__name__)
            time.sleep(3)


def main() -> None:
    from minio import Minio  # imported here so unit tests need no MinIO client

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    host, secure = parse_endpoint(env("MINIO_ENDPOINT"))
    client = Minio(
        host,
        access_key=env("MINIO_ROOT_USER"),
        secret_key=env("MINIO_ROOT_PASSWORD"),
        secure=secure,
    )
    wait_and_create_bucket(client, env("MINIO_BUCKET"))


if __name__ == "__main__":
    main()
