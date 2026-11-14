"""Historical loader: download -> verify -> extract -> manifest -> upload, with failure handling."""

import hashlib
import json
import shutil
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from ingest_helpers import (
    CSV_HEADER,
    FileServe,
    MockServer,
    Resp,
    csv_text,
    make_zip_bytes,
    no_sleep,
)

from ingestion.contracts import validate_manifest
from ingestion.discovery import discover, latest_complete_full_load
from ingestion.historical import (
    HistoricalError,
    HistoricalLoader,
    HistoricalSettings,
    extract_member,
    inspect_source,
    main,
    parse_zip_prefix,
    read_csv_structure,
    same_source_version,
    validate_archive,
)
from ingestion.layout import Layout
from ingestion.net import HttpClient, RetryPolicy
from ingestion.storage import LocalStore, StorageError


# ---------------------------------------------------------------- helpers -----------------
def stepping_clock(start: datetime = datetime(2026, 11, 13, 9, 0, 0, tzinfo=UTC)):
    """Each call returns a time one second later: run ids sort in the order the runs happened."""
    state = {"now": start}

    def clock() -> datetime:
        state["now"] += timedelta(seconds=1)
        return state["now"]

    return clock


def make_loader(url: str, store, tmp_path: Path, clock=None, **overrides) -> HistoricalLoader:
    settings = HistoricalSettings(
        url=url,
        timeout_seconds=5,
        max_retries=2,
        backoff_base_seconds=0,
        chunk_bytes=4096,
        tmp_dir=tmp_path / "tmp",
        **overrides,
    )
    client = HttpClient(settings.policy, sleep=no_sleep)
    kwargs = {"clock": clock} if clock else {}
    return HistoricalLoader(store, Layout(), settings, client, version="abc1234", **kwargs)


def write_zip(
    path: Path, members: dict[str, str | bytes], compression=zipfile.ZIP_DEFLATED
) -> Path:
    path.write_bytes(make_zip_bytes(members, compression))
    return path


# ---------------------------------------------------------------- archive checks ----------
def test_validate_archive_accepts_the_expected_single_member(tmp_path: Path) -> None:
    info = validate_archive(
        write_zip(tmp_path / "a.zip", {"complaints.csv": csv_text()}), "complaints.csv"
    )
    assert info.filename == "complaints.csv" and info.file_size > 0


def test_validate_archive_rejects_non_zip(tmp_path: Path) -> None:
    bad = tmp_path / "a.zip"
    bad.write_bytes(b"<html>Access denied</html>")
    with pytest.raises(HistoricalError, match="not a valid ZIP"):
        validate_archive(bad, "complaints.csv")


def test_validate_archive_rejects_truncated_zip(tmp_path: Path) -> None:
    data = make_zip_bytes()
    bad = tmp_path / "a.zip"
    bad.write_bytes(data[: len(data) // 2])
    with pytest.raises(HistoricalError):
        validate_archive(bad, "complaints.csv")


@pytest.mark.parametrize(
    "members",
    [
        {"other.csv": csv_text()},
        {"complaints.csv": csv_text(), "extra.txt": "x"},
        {"complaints.csv": ""},
    ],
)
def test_validate_archive_rejects_unexpected_contents(tmp_path: Path, members) -> None:
    with pytest.raises(HistoricalError):
        validate_archive(write_zip(tmp_path / "a.zip", members), "complaints.csv")


def test_extract_member_streams_and_hashes(tmp_path: Path) -> None:
    text = csv_text(1000)
    archive = write_zip(tmp_path / "a.zip", {"complaints.csv": text})
    out = tmp_path / "out.csv"
    size, digest = extract_member(archive, "complaints.csv", out, chunk_bytes=512)
    assert size == len(text.encode()) == out.stat().st_size
    assert digest == hashlib.sha256(text.encode()).hexdigest()


def test_extract_member_detects_corrupted_content_via_crc(tmp_path: Path) -> None:
    archive = write_zip(
        tmp_path / "a.zip", {"complaints.csv": csv_text(200)}, compression=zipfile.ZIP_STORED
    )
    data = bytearray(archive.read_bytes())
    data[data.index(b"Product A") + 2] ^= 0xFF  # flip a byte inside the stored CSV
    archive.write_bytes(bytes(data))
    with pytest.raises(HistoricalError, match="corrupted"):
        extract_member(archive, "complaints.csv", tmp_path / "out.csv")
    assert not (tmp_path / "out.csv").exists()


# ---------------------------------------------------------------- CSV structure -----------
def test_csv_structure_header_and_record_count(tmp_path: Path) -> None:
    path = tmp_path / "c.csv"
    path.write_text(csv_text(7), encoding="utf-8")
    header, count = read_csv_structure(path, ("Complaint ID",))
    assert header == CSV_HEADER and count == 7


def test_csv_record_count_respects_quoted_newlines(tmp_path: Path) -> None:
    path = tmp_path / "c.csv"
    path.write_text(
        "Date received,Product,State,Complaint ID\n"
        '2026-10-01,"Line one\nline two",CA,1\n'
        "2026-10-02,Product B,NY,2\n",
        encoding="utf-8",
    )
    assert read_csv_structure(path, ("Complaint ID",))[1] == 2  # not 3 physical lines


def test_csv_with_bom_and_case_insensitive_required_columns(tmp_path: Path) -> None:
    path = tmp_path / "c.csv"
    path.write_bytes("﻿Date received,complaint id\n2026-10-01,1\n".encode())
    header, count = read_csv_structure(path, ("Complaint ID", "Date received"))
    assert header[0] == "Date received" and count == 1


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("Date received,Product\n2026-10-01,A\n", "missing required column"),
        ("Date received,Complaint ID\n", "no data rows"),
        ("", "no header"),
    ],
)
def test_csv_structure_rejects_bad_files(tmp_path: Path, content: str, message: str) -> None:
    path = tmp_path / "c.csv"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(HistoricalError, match=message):
        read_csv_structure(path, ("Complaint ID", "Date received"))


# ---------------------------------------------------------------- full loader -------------
def test_successful_load_stores_zip_csv_and_a_valid_manifest(tmp_path: Path) -> None:
    zip_bytes = make_zip_bytes({"complaints.csv": csv_text(25)})
    store = LocalStore(tmp_path / "zone")
    with MockServer(FileServe(zip_bytes)) as server:
        result = make_loader(server.url + "/complaints.csv.zip", store, tmp_path).run()

    assert result.status == "loaded"
    manifest = result.manifest
    validate_manifest(manifest)  # satisfies config/schemas/manifest.schema.json
    assert manifest["record_count"] == 25
    assert manifest["header"] == CSV_HEADER
    assert manifest["code_version"] == "abc1234"
    assert manifest["archive_bytes"] == len(zip_bytes)
    assert manifest["archive_sha256"] == hashlib.sha256(zip_bytes).hexdigest()
    assert manifest["extracted_sha256"] == hashlib.sha256(csv_text(25).encode()).hexdigest()
    assert manifest["source_etag"] == '"v1"'

    # original archive stored byte-for-byte, CSV extracted, manifest present
    assert store.get_bytes(manifest["archive_path"]) == zip_bytes
    assert store.get_bytes(manifest["extracted_path"]).decode() == csv_text(25)
    assert json.loads(store.get_bytes(result.manifest_key)) == manifest
    assert manifest["archive_path"].startswith("raw/cfpb/full_load/ingest_date=")
    assert f"run_id={manifest['run_id']}/complaints.csv.zip" in manifest["archive_path"]


def test_temp_files_are_removed_after_success(tmp_path: Path) -> None:
    with MockServer(FileServe(make_zip_bytes())) as server:
        make_loader(server.url + "/f.zip", LocalStore(tmp_path / "zone"), tmp_path).run()
    leftovers = [p for p in (tmp_path / "tmp").rglob("*") if p.is_file()]
    assert leftovers == []


def test_rerun_of_unchanged_source_downloads_and_uploads_nothing(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    with MockServer(FileServe(make_zip_bytes())) as server:
        loader = make_loader(server.url + "/f.zip", store, tmp_path)
        first = loader.run()
        objects_after_first = sorted(o.key for o in store.list_objects("raw/"))
        second = loader.run()
        gets = server.count("GET")
    assert first.status == "loaded" and second.status == "skipped_up_to_date"
    assert gets == 1  # the second run only sent a HEAD
    assert sorted(o.key for o in store.list_objects("raw/")) == objects_after_first


def test_force_creates_a_new_run_and_never_touches_the_old_one(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    with MockServer(FileServe(make_zip_bytes())) as server:
        loader = make_loader(server.url + "/f.zip", store, tmp_path)
        first = loader.run()
        before = store.get_bytes(first.manifest["archive_path"])
        second = loader.run(force=True)
    assert second.status == "loaded" and second.run_id != first.run_id
    assert store.get_bytes(first.manifest["archive_path"]) == before
    assert len([r for r in discover(store, Layout()) if r.complete]) == 2


def test_changed_source_is_stored_as_a_new_run_without_replacing_the_old(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    v1 = make_zip_bytes({"complaints.csv": csv_text(5)})
    v2 = make_zip_bytes({"complaints.csv": csv_text(9)})
    serve = FileServe(v1, etag='"v1"')
    with MockServer(serve) as server:
        loader = make_loader(server.url + "/f.zip", store, tmp_path, clock=stepping_clock())
        first = loader.run()
        serve.body, serve.etag = v2, '"v2"'
        second = loader.run()
    assert second.status == "loaded"
    assert store.get_bytes(first.manifest["archive_path"]) == v1
    assert store.get_bytes(second.manifest["archive_path"]) == v2
    assert latest_complete_full_load(store, Layout()).run_id == second.run_id


def test_identical_content_without_validators_is_not_stored_twice(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    serve = FileServe(make_zip_bytes(), etag=None, last_modified=None)
    with MockServer(serve) as server:
        loader = make_loader(server.url + "/f.zip", store, tmp_path)
        first = loader.run()
        second = loader.run()
    assert first.status == "loaded" and second.status == "skipped_identical"
    assert len([r for r in discover(store, Layout()) if r.complete]) == 1


def test_dry_run_downloads_and_writes_nothing(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    body = make_zip_bytes()
    with MockServer(FileServe(body)) as server:
        result = make_loader(server.url + "/f.zip", store, tmp_path).run(dry_run=True)
        assert server.count("GET") == 0
    assert result.status == "dry_run"
    assert result.plan["source_content_length"] == len(body)
    assert list(store.list_objects("")) == []


# ---------------------------------------------------------------- failure handling --------
def test_corrupted_download_is_detected_and_nothing_is_stored(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    with MockServer(FileServe(b"this is not a zip file at all")) as server:
        with pytest.raises(HistoricalError, match="not a valid ZIP"):
            make_loader(server.url + "/f.zip", store, tmp_path).run()
    assert list(store.list_objects("")) == []


def test_crc_corruption_inside_zip_is_detected(tmp_path: Path) -> None:
    data = bytearray(make_zip_bytes({"complaints.csv": csv_text(300)}, zipfile.ZIP_STORED))
    data[data.index(b"Product A") + 3] ^= 0xFF
    store = LocalStore(tmp_path / "zone")
    with MockServer(FileServe(bytes(data))) as server:
        with pytest.raises(HistoricalError, match="corrupted"):
            make_loader(server.url + "/f.zip", store, tmp_path).run()
    assert list(store.list_objects("")) == []


def test_http_failure_reports_clearly_and_stores_nothing(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    with MockServer(lambda req: Resp(500)) as server:
        with pytest.raises(HistoricalError, match="Download failed"):
            make_loader(server.url + "/f.zip", store, tmp_path).run()
    assert list(store.list_objects("")) == []


def test_not_found_fails_fast(tmp_path: Path) -> None:
    with MockServer(lambda req: Resp(404)) as server:
        with pytest.raises(HistoricalError, match="404"):
            make_loader(server.url + "/f.zip", LocalStore(tmp_path / "z"), tmp_path).run()
        assert server.count("GET") == 1


def test_timeout_then_retry_still_completes(tmp_path: Path) -> None:
    serve = FileServe(make_zip_bytes(), delays=[0.6])
    store = LocalStore(tmp_path / "zone")
    with MockServer(serve) as server:
        loader = make_loader(server.url + "/f.zip", store, tmp_path)
        loader.client.policy = RetryPolicy(0.2, 2, 0)
        result = loader.run()
    assert result.status == "loaded"
    assert result.manifest["http_retry_count"] >= 1


class FailingStore(LocalStore):
    """Simulates MinIO going away while a particular object is being uploaded."""

    def __init__(self, root: Path, fail_suffix: str) -> None:
        super().__init__(root)
        self.fail_suffix = fail_suffix
        self.enabled = True

    def _maybe_fail(self, key: str) -> None:
        if self.enabled and key.endswith(self.fail_suffix):
            raise StorageError(f"simulated upload failure for {key}")

    def put_file(self, key, path, **kwargs):
        self._maybe_fail(key)
        return super().put_file(key, path, **kwargs)

    def put_bytes(self, key, data, **kwargs):
        self._maybe_fail(key)
        return super().put_bytes(key, data, **kwargs)


@pytest.mark.parametrize("fail_suffix", ["extracted/complaints.csv", "manifest.json"])
def test_upload_failure_leaves_an_incomplete_batch_that_is_never_discoverable(
    tmp_path: Path, fail_suffix: str
) -> None:
    store = FailingStore(tmp_path / "zone", fail_suffix)
    with MockServer(FileServe(make_zip_bytes())) as server:
        loader = make_loader(server.url + "/f.zip", store, tmp_path)
        with pytest.raises(HistoricalError, match="incomplete"):
            loader.run()

        runs = discover(store, Layout())
        assert runs and not any(r.complete for r in runs)  # orphan objects are reported incomplete
        assert latest_complete_full_load(store, Layout()) is None
        assert not any(o.key.endswith("manifest.json") for o in store.list_objects("raw/"))

        store.enabled = False  # storage recovers; a rerun creates a fresh, complete run
        result = loader.run()
    assert result.status == "loaded"
    assert latest_complete_full_load(store, Layout()).run_id == result.run_id


def test_manifest_violating_the_contract_is_never_uploaded(tmp_path: Path, monkeypatch) -> None:
    import ingestion.historical as historical

    store = LocalStore(tmp_path / "zone")
    monkeypatch.setattr(historical, "code_version", lambda: "unknown")
    bad_settings = HistoricalSettings(url="http://x", tmp_dir=tmp_path / "tmp")
    assert bad_settings.expected_member == "complaints.csv"
    with MockServer(FileServe(make_zip_bytes())) as server:
        loader = make_loader(server.url + "/f.zip", store, tmp_path)
        loader.layout = Layout(full_prefix="wrong/prefix")  # breaks the archive_path pattern
        with pytest.raises(HistoricalError, match="violates its contract"):
            loader.run()
    assert list(store.list_objects("")) == []


def test_insufficient_disk_space_fails_before_downloading(tmp_path: Path, monkeypatch) -> None:
    usage = shutil.disk_usage(tmp_path)
    monkeypatch.setattr(shutil, "disk_usage", lambda p: usage._replace(free=10))
    with MockServer(FileServe(make_zip_bytes())) as server:
        with pytest.raises(HistoricalError, match="free disk space"):
            make_loader(server.url + "/f.zip", LocalStore(tmp_path / "z"), tmp_path).run()
        assert server.count("GET") == 0


def test_same_source_version_logic() -> None:
    from ingestion.download import SourceInfo

    src = SourceInfo("u", 10, '"e"', "lm", True)
    base = {"source_url": "u"}
    assert same_source_version({**base, "source_etag": '"e"'}, src)
    assert not same_source_version({**base, "source_etag": '"other"'}, src)
    assert not same_source_version({"source_url": "other", "source_etag": '"e"'}, src)
    no_etag = SourceInfo("u", 10, None, "lm", True)
    assert same_source_version(
        {**base, "source_last_modified": "lm", "source_content_length": 10}, no_etag
    )
    assert not same_source_version(base, no_etag)
    assert not same_source_version(base, None)


# ---------------------------------------------------------------- inspect -----------------
def test_parse_zip_prefix_handles_deflated_and_stored_members() -> None:
    text = csv_text(50)
    for compression in (zipfile.ZIP_DEFLATED, zipfile.ZIP_STORED):
        data = make_zip_bytes({"complaints.csv": text}, compression)
        name, prefix = parse_zip_prefix(data)
        assert name == "complaints.csv" and prefix.decode().startswith("Date received,")


def test_parse_zip_prefix_rejects_non_zip() -> None:
    with pytest.raises(HistoricalError):
        parse_zip_prefix(b"x" * 100)


def test_inspect_reads_header_and_date_formats_from_the_first_bytes_only() -> None:
    big = make_zip_bytes({"complaints.csv": csv_text(60_000)})  # much larger than the probe
    assert len(big) > 262_144
    with MockServer(FileServe(big)) as server:
        info = inspect_source(HttpClient(RetryPolicy(5, 1, 0), sleep=no_sleep), server.url + "/f")
        ranged = [r.headers.get("Range") for r in server.requests if r.method == "GET"]
    assert info["csv_header"] == CSV_HEADER
    assert info["content_length_bytes"] == len(big)
    assert info["date_column_samples"]["Date received"][0].startswith("2026-10-")
    assert info["zip_member"] == "complaints.csv"
    assert ranged == ["bytes=0-262143"]


# ---------------------------------------------------------------- command line ------------
def write_config(tmp_path: Path, url: str) -> Path:
    config = {
        "source": {"full_csv_url": url, "api_base_url": "http://unused/"},
        "historical": {
            "timeout_seconds": 5,
            "max_retries": 1,
            "backoff_base_seconds": 0,
            "tmp_dir": str(tmp_path / "tmp"),
        },
    }
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return path


def test_cli_loads_into_a_local_sink_and_reruns_safely(tmp_path: Path, capsys) -> None:
    with MockServer(FileServe(make_zip_bytes({"complaints.csv": csv_text(12)}))) as server:
        config = write_config(tmp_path, server.url + "/f.zip")
        argv = ["--sink", "local", "--out", str(tmp_path / "zone"), "--config", str(config)]
        assert main(argv) == 0
        first = json.loads(capsys.readouterr().out)
        assert first["status"] == "loaded" and first["record_count"] == 12
        assert main(argv) == 0
        assert json.loads(capsys.readouterr().out)["status"] == "skipped_up_to_date"


def test_cli_failure_returns_exit_code_1(tmp_path: Path, capsys) -> None:
    with MockServer(lambda req: Resp(404)) as server:
        config = write_config(tmp_path, server.url + "/f.zip")
        code = main(["--sink", "local", "--out", str(tmp_path / "z"), "--config", str(config)])
    assert code == 1
    assert "HISTORICAL LOAD FAILED" in capsys.readouterr().err


def test_cli_missing_output_folder_is_a_usage_error(tmp_path: Path, capsys) -> None:
    config = write_config(tmp_path, "http://127.0.0.1:9/f.zip")
    assert main(["--sink", "local", "--config", str(config)]) == 2


def test_cli_inspect_mode(tmp_path: Path, capsys) -> None:
    with MockServer(FileServe(make_zip_bytes())) as server:
        config = write_config(tmp_path, server.url + "/f.zip")
        assert main(["--inspect", "--config", str(config)]) == 0
    assert json.loads(capsys.readouterr().out)["csv_header"] == CSV_HEADER
