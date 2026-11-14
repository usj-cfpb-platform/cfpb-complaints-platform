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

from ingestion.discovery import discover
from ingestion.historical import HistoricalLoader, HistoricalSettings
from ingestion.incremental import HighWaterMarkStore, IncrementalCollector, IncrementalSettings
from ingestion.layout import Layout
from ingestion.net import HttpClient, RetryPolicy
from ingestion.storage import MinioStore, ObjectExistsError

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


@pytest.fixture
def zone(store):
    root = f"_integration_tests/{uuid.uuid4().hex[:12]}"
    layout = Layout(
        full_prefix=f"{root}/raw/cfpb/full_load",
        api_prefix=f"{root}/raw/cfpb/api",
        state_key=f"{root}/state/high_water_mark.json",
    )
    yield layout, root
    for obj in list(store.list_objects(root)):  # remove only what this test created
        store.client.remove_object(store.bucket, obj.key)


def test_write_once_is_enforced_by_real_minio(store, zone) -> None:
    _, root = zone
    key = f"{root}/probe.json"
    store.put_bytes(key, b"original")
    with pytest.raises(ObjectExistsError):
        store.put_bytes(key, b"changed")
    assert store.get_bytes(key) == b"original"


def test_historical_load_and_discovery_on_minio(store, zone, tmp_path) -> None:
    layout, _ = zone
    body = make_zip_bytes({"complaints.csv": csv_text(30)})
    with MockServer(FileServe(body)) as server:
        settings = HistoricalSettings(
            url=server.url + "/f.zip", backoff_base_seconds=0, tmp_dir=tmp_path
        )
        loader = HistoricalLoader(
            store, layout, settings, HttpClient(settings.policy, sleep=no_sleep), version="it"
        )
        result = loader.run()
        again = loader.run()
    assert result.status == "loaded" and again.status == "skipped_up_to_date"
    assert store.get_bytes(result.manifest["archive_path"]) == body
    [run] = [r for r in discover(store, layout, "full") if r.complete]
    assert run.record_count == 30


def test_incremental_run_and_checkpoint_on_minio(store, zone) -> None:
    layout, _ = zone
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
        store,
        layout,
        settings,
        ("Product A", "Product B"),
        client,
        HighWaterMarkStore(store, layout.state_key),
        clock=lambda: datetime(2026, 11, 13, 9, tzinfo=UTC),
        sleep=no_sleep,
        version="it",
    )
    first = collector.run()
    second = collector.run()
    assert first.outcome == second.outcome == "success" and first.hwm_advanced
    assert not second.hwm_advanced
    assert len([r for r in discover(store, layout, "api") if r.complete]) == 2
