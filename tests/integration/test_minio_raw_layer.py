"""Raw layer against a REAL MinIO (the Docker Compose service).

Skipped automatically when the ``minio`` package is missing or MinIO is not reachable.
Run it from the host with MinIO up (``docker compose up -d``):

    PowerShell:  $env:MINIO_ENDPOINT="http://localhost:9000"; $env:MINIO_ROOT_USER="..."
                 $env:MINIO_ROOT_PASSWORD="..."; $env:MINIO_BUCKET="datalake"
                 python -m pytest tests/integration -v

Non-destructive by design: every object is written under ``_integration_tests/<uuid>/`` and only
those objects are removed afterwards. Real project data is never read, changed or deleted.
"""

import os
import uuid
from datetime import UTC, datetime

import pytest
from ingest_helpers import (
    ApiSim,
    FileServe,
    MockServer,
    ScriptedSession,
    csv_text,
    make_records,
    make_zip_bytes,
    no_sleep,
)

from common.config import load_settings
from ingestion.discovery import discover
from ingestion.historical import HistoricalLoader, HistoricalSettings
from ingestion.incremental import HighWaterMarkStore, IncrementalCollector, IncrementalSettings
from ingestion.layout import Layout
from ingestion.net import HttpClient, RetryPolicy
from ingestion.storage import MinioStore, ObjectExistsError, ObjectInfo

pytest.importorskip("minio")

REQUIRED = ("MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BUCKET")
if any(not os.environ.get(name) for name in REQUIRED):
    pytest.skip("MinIO environment variables not set", allow_module_level=True)


@pytest.fixture(scope="module")
def store():
    try:
        minio_store = MinioStore.from_env()
        if not minio_store.client.bucket_exists(minio_store.bucket):
            pytest.skip(f"bucket {minio_store.bucket!r} does not exist (run storage-init)")
    except Exception as exc:  # connection refused, bad credentials, ...
        pytest.skip(f"MinIO not reachable: {exc}")
    return minio_store


class PrefixedStore:
    """Keeps every object under ``root/`` while the collectors still see canonical keys.

    The contracts require paths that start with ``raw/cfpb/...``, so the layout must stay
    canonical. This wrapper adds the ``_integration_tests/<uuid>/`` prefix on the way in and strips
    it on the way out, which keeps the test objects separate from the real project data.
    """

    def __init__(self, inner: MinioStore, root: str) -> None:
        self.inner = inner
        self.root = root

    def _k(self, key: str) -> str:
        return f"{self.root}/{key}"

    def exists(self, key: str) -> bool:
        return self.inner.exists(self._k(key))

    def stat(self, key: str):
        info = self.inner.stat(self._k(key))
        return None if info is None else ObjectInfo(key, info.size)

    def put_bytes(self, key: str, data: bytes, **kwargs):
        info = self.inner.put_bytes(self._k(key), data, **kwargs)
        return ObjectInfo(key, info.size)

    def put_file(self, key: str, path, **kwargs):
        info = self.inner.put_file(self._k(key), path, **kwargs)
        return ObjectInfo(key, info.size)

    def get_bytes(self, key: str) -> bytes:
        return self.inner.get_bytes(self._k(key))

    def list_objects(self, prefix: str):
        for obj in self.inner.list_objects(self._k(prefix)):
            yield ObjectInfo(obj.key[len(self.root) + 1 :], obj.size)


@pytest.fixture
def zone(store):
    root = f"_integration_tests/{uuid.uuid4().hex[:12]}"
    yield PrefixedStore(store, root), Layout.from_settings(load_settings()), root
    for obj in list(store.list_objects(root)):  # remove only what this test created
        store.client.remove_object(store.bucket, obj.key)


def test_write_once_is_enforced_by_real_minio(store, zone) -> None:
    _, _, root = zone
    key = f"{root}/probe.json"
    store.put_bytes(key, b"original")
    with pytest.raises(ObjectExistsError):
        store.put_bytes(key, b"changed")
    assert store.get_bytes(key) == b"original"


def test_historical_load_and_discovery_on_minio(zone, tmp_path) -> None:
    scoped, layout, _ = zone
    body = make_zip_bytes({"complaints.csv": csv_text(30)})
    with MockServer(FileServe(body)) as server:
        settings = HistoricalSettings(
            url=server.url + "/f.zip", backoff_base_seconds=0, tmp_dir=tmp_path
        )
        loader = HistoricalLoader(
            scoped, layout, settings, HttpClient(settings.policy, sleep=no_sleep), version="it"
        )
        result = loader.run()
        again = loader.run()
    assert result.status == "loaded" and again.status == "skipped_up_to_date"
    assert scoped.get_bytes(result.manifest["archive_path"]) == body
    [run] = [r for r in discover(scoped, layout, "full") if r.complete]
    assert run.record_count == 30


def test_incremental_run_and_checkpoint_on_minio(zone) -> None:
    scoped, layout, _ = zone
    settings = IncrementalSettings(
        endpoint="https://api.example.org/",
        page_size=10,
        backoff_base_seconds=0,
        request_delay_seconds=0,
    )
    client = HttpClient(
        RetryPolicy(5, 2, 0), session=ScriptedSession(sim=ApiSim(make_records(25))), sleep=no_sleep
    )
    collector = IncrementalCollector(
        scoped,
        layout,
        settings,
        ("Product A", "Product B"),
        client,
        HighWaterMarkStore(scoped, layout.state_key),
        clock=lambda: datetime(2026, 11, 13, 9, tzinfo=UTC),
        sleep=no_sleep,
        version="it",
    )
    first = collector.run()
    second = collector.run()
    assert first.outcome == second.outcome == "success" and first.hwm_advanced
    assert not second.hwm_advanced
    assert len([r for r in discover(scoped, layout, "api") if r.complete]) == 2
