"""Raw-zone layout and storage backends: object keys, write-once rule, partial uploads."""

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
from ingest_helpers import FakeMinio

from common.config import REPO_ROOT
from ingestion.layout import Layout, ingest_date_of, new_run_id, parse_run_prefix
from ingestion.storage import (
    LocalStore,
    MinioStore,
    ObjectExistsError,
    StorageError,
    validate_key,
)

NOW = datetime(2026, 11, 13, 9, 0, 0, tzinfo=UTC)


def _schema(name: str) -> dict:
    return json.loads((REPO_ROOT / "config" / "schemas" / name).read_text(encoding="utf-8"))


# ---------------------------------------------------------------- layout ------------------
def test_run_id_format_and_schema_patterns() -> None:
    full = new_run_id("full", NOW, rand="9f8e7d")
    api = new_run_id("api", NOW, rand="a1b2c3")
    assert full == "full_20261113T090000Z_9f8e7d"
    assert api == "api_20261113T090000Z_a1b2c3"
    assert re.match(_schema("manifest.schema.json")["properties"]["run_id"]["pattern"], full)
    assert re.match(_schema("run_metadata.schema.json")["properties"]["run_id"]["pattern"], api)


def test_random_suffix_differs_between_runs() -> None:
    assert new_run_id("api", NOW) != new_run_id("api", NOW)


def test_run_id_rejects_unknown_source() -> None:
    with pytest.raises(ValueError):
        new_run_id("csv", NOW)


def test_keys_match_the_contract_patterns() -> None:
    layout = Layout()
    date = ingest_date_of(NOW)
    manifest_pattern = _schema("manifest.schema.json")["properties"]["archive_path"]["pattern"]
    page_pattern = _schema("run_metadata.schema.json")["properties"]["raw_objects"]["items"][
        "properties"
    ]["path"]["pattern"]

    archive = layout.archive_key("full_20261113T090000Z_9f8e7d", date)
    page = layout.page_key("api_20261113T090000Z_a1b2c3", date, 7)
    assert archive == (
        "raw/cfpb/full_load/ingest_date=2026-11-13/run_id=full_20261113T090000Z_9f8e7d/"
        "complaints.csv.zip"
    )
    assert page.endswith("/response_0007.json")
    assert re.match(manifest_pattern, archive)
    assert re.match(page_pattern, page)
    assert layout.extracted_key("full_20261113T090000Z_9f8e7d", date).endswith(
        "/extracted/complaints.csv"
    )
    assert layout.manifest_key("full_20261113T090000Z_9f8e7d", date).endswith("/manifest.json")
    assert layout.run_metadata_key("api_20261113T090000Z_a1b2c3", date).endswith(
        "/run_metadata.json"
    )


def test_page_numbers_start_at_one() -> None:
    with pytest.raises(ValueError):
        Layout().page_key("api_20261113T090000Z_a1b2c3", "2026-11-13", 0)


def test_layout_reads_prefixes_from_settings() -> None:
    layout = Layout.from_settings(
        {"storage": {"raw_full_load_prefix": "/x/full/", "raw_api_prefix": "x/api"}}
    )
    assert layout.full_prefix == "x/full"
    assert layout.api_prefix == "x/api"
    assert layout.state_key == "state/high_water_mark.json"


def test_committed_settings_match_default_layout() -> None:
    from common.config import load_settings

    assert Layout.from_settings(load_settings()) == Layout()


def test_ingest_date_is_utc() -> None:
    late = datetime.fromisoformat("2026-11-13T23:30:00-05:00")  # already 04:30 UTC on the 14th
    assert ingest_date_of(late) == "2026-11-14"


def test_parse_run_prefix() -> None:
    key = (
        "raw/cfpb/api/ingest_date=2026-11-13/run_id=api_20261113T031700Z_a1b2c3/response_0001.json"
    )
    assert parse_run_prefix(key) == ("2026-11-13", "api_20261113T031700Z_a1b2c3")
    assert parse_run_prefix("state/high_water_mark.json") is None


# ---------------------------------------------------------------- key safety --------------
@pytest.mark.parametrize("bad", ["", "/abs", "a//b", "a/../b", "a\\b", "./a", "a/./b"])
def test_validate_key_rejects_unsafe_keys(bad: str) -> None:
    with pytest.raises(StorageError):
        validate_key(bad)


# ---------------------------------------------------------------- LocalStore --------------
def test_local_store_roundtrip_and_list(tmp_path: Path) -> None:
    store = LocalStore(tmp_path)
    store.put_bytes("raw/a/one.json", b"1")
    store.put_bytes("raw/a/two.json", b"22")
    store.put_bytes("other/x.json", b"3")
    assert store.get_bytes("raw/a/one.json") == b"1"
    assert store.stat("raw/a/two.json").size == 2
    assert [o.key for o in store.list_objects("raw/")] == ["raw/a/one.json", "raw/a/two.json"]
    assert store.stat("missing") is None
    with pytest.raises(StorageError):
        store.get_bytes("missing")


def test_local_store_never_overwrites_raw_objects(tmp_path: Path) -> None:
    store = LocalStore(tmp_path)
    store.put_bytes("raw/a.json", b"original")
    with pytest.raises(ObjectExistsError):
        store.put_bytes("raw/a.json", b"different")
    assert store.get_bytes("raw/a.json") == b"original"


def test_local_store_explicit_overwrite_is_allowed_for_state(tmp_path: Path) -> None:
    store = LocalStore(tmp_path)
    store.put_bytes("state/hwm.json", b"v1")
    store.put_bytes("state/hwm.json", b"v2", overwrite=True)
    assert store.get_bytes("state/hwm.json") == b"v2"


def test_local_store_put_file_and_no_leftover_temp_files(tmp_path: Path) -> None:
    source = tmp_path / "src.bin"
    source.write_bytes(b"x" * 1000)
    store = LocalStore(tmp_path / "zone")
    info = store.put_file("raw/big.bin", source)
    assert info.size == 1000
    assert not list((tmp_path / "zone").rglob(".tmp-*"))


def test_local_store_failed_write_leaves_no_partial_object(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    with pytest.raises(FileNotFoundError):
        store.put_file("raw/never.bin", tmp_path / "does-not-exist.bin")
    assert store.stat("raw/never.bin") is None
    assert not list((tmp_path / "zone").rglob(".tmp-*"))


# ---------------------------------------------------------------- MinioStore --------------
def test_minio_store_put_get_list_and_metadata() -> None:
    client = FakeMinio()
    store = MinioStore(client, "datalake")
    info = store.put_bytes("raw/a.json", b"abc", metadata={"sha256": "x"})
    assert info.size == 3
    assert client.user_metadata["raw/a.json"] == {"sha256": "x"}
    assert store.get_bytes("raw/a.json") == b"abc"
    assert [o.key for o in store.list_objects("raw/")] == ["raw/a.json"]
    assert store.stat("nope") is None and not store.exists("nope")


def test_minio_store_put_file_uploads_by_path(tmp_path: Path) -> None:
    source = tmp_path / "a.zip"
    source.write_bytes(b"zipdata")
    store = MinioStore(FakeMinio(), "datalake")
    assert store.put_file("raw/a.zip", source).size == 7


def test_minio_store_refuses_to_overwrite_existing_raw_object() -> None:
    client = FakeMinio()
    store = MinioStore(client, "datalake")
    store.put_bytes("raw/a.json", b"original")
    with pytest.raises(ObjectExistsError):
        store.put_bytes("raw/a.json", b"changed")
    assert client.objects["raw/a.json"] == b"original"


def test_minio_store_overwrite_only_when_explicit() -> None:
    client = FakeMinio()
    store = MinioStore(client, "datalake")
    store.put_bytes("state/x.json", b"1")
    store.put_bytes("state/x.json", b"22", overwrite=True)
    assert client.objects["state/x.json"] == b"22"


def test_minio_store_wraps_upload_failure() -> None:
    client = FakeMinio()
    client.fail_on = lambda key: True
    store = MinioStore(client, "datalake")
    with pytest.raises(StorageError, match="upload failed"):
        store.put_bytes("raw/a.json", b"x")
    assert not client.objects  # nothing half-written


def test_minio_store_detects_size_mismatch_after_upload() -> None:
    client = FakeMinio()
    client.report_wrong_size = True
    store = MinioStore(client, "datalake")
    with pytest.raises(StorageError, match="verification failed"):
        store.put_bytes("raw/a.json", b"abc")


def test_minio_store_rejects_unsafe_keys() -> None:
    with pytest.raises(StorageError):
        MinioStore(FakeMinio(), "datalake").put_bytes("../escape", b"x")


def test_minio_credentials_come_only_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("minio")
    for name in ("MINIO_ENDPOINT", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "MINIO_BUCKET"):
        monkeypatch.delenv(name, raising=False)
    from common.config import ConfigError

    with pytest.raises(ConfigError):
        MinioStore.from_env()
