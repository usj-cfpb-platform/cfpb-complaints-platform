"""Completed-batch discovery, GitHub-artifact sync into the raw zone, and the dev sample."""

import csv
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from ingest_helpers import (
    ApiSim,
    FileServe,
    MockServer,
    Resp,
    ScriptedSession,
    csv_text,
    make_records,
    make_zip_bytes,
    no_sleep,
)

from common.config import REPO_ROOT
from ingestion.discovery import completed, discover, latest_complete_full_load
from ingestion.discovery import main as discovery_main
from ingestion.historical import HistoricalLoader, HistoricalSettings
from ingestion.incremental import (
    HighWaterMarkStore,
    IncrementalCollector,
    IncrementalSettings,
    parse_page,
)
from ingestion.layout import Layout
from ingestion.net import HttpClient, RetryPolicy
from ingestion.storage import LocalStore
from ingestion.sync_artifacts import (
    GitHubArtifacts,
    SyncError,
    safe_extract,
    sync_directory,
)
from ingestion.sync_artifacts import main as sync_main
from scripts import make_sample

LAYOUT = Layout()
NOW = datetime(2026, 11, 13, 9, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------- builders ----------------
def collect_run(store, count: int = 25, *, max_pages=None, with_state: bool = False):
    settings = IncrementalSettings(
        endpoint="https://api.example.org/",
        page_size=10,
        backoff_base_seconds=0,
        request_delay_seconds=0,
    )
    client = HttpClient(
        RetryPolicy(5, 2, 0),
        session=ScriptedSession(sim=ApiSim(make_records(count))),
        sleep=no_sleep,
    )
    hwm = HighWaterMarkStore(store, LAYOUT.state_key) if with_state else None
    collector = IncrementalCollector(
        store,
        LAYOUT,
        settings,
        ("Product A", "Product B"),
        client,
        hwm,
        clock=lambda: NOW,
        sleep=no_sleep,
        version="abc1234",
    )
    return collector.run(max_pages=max_pages)


def load_full(store, tmp_path: Path, rows: int = 5):
    with MockServer(FileServe(make_zip_bytes({"complaints.csv": csv_text(rows)}))) as server:
        settings = HistoricalSettings(
            url=server.url + "/f.zip", backoff_base_seconds=0, tmp_dir=tmp_path / "tmp"
        )
        loader = HistoricalLoader(
            store, LAYOUT, settings, HttpClient(settings.policy, sleep=no_sleep), version="abc1234"
        )
        return loader.run(force=True)


# ---------------------------------------------------------------- discovery ---------------
def test_only_finished_batches_are_listed_as_complete(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    full = load_full(store, tmp_path, rows=8)
    good = collect_run(store)
    partial = collect_run(store, max_pages=1)

    by_id = {r.run_id: r for r in discover(store, LAYOUT)}
    assert by_id[full.run_id].complete and by_id[good.run_id].complete
    assert not by_id[partial.run_id].complete and "partial" in by_id[partial.run_id].reason

    done = completed(store, LAYOUT)
    assert {r.run_id for r in done} == {full.run_id, good.run_id}
    api_run = by_id[good.run_id]
    assert api_run.record_count == 25
    assert api_run.data_keys == [o["path"] for o in good.metadata["raw_objects"]]
    full_run = by_id[full.run_id]
    assert full_run.data_keys == [full.manifest["extracted_path"]] and full_run.record_count == 8


def test_discovery_filters_by_kind_and_finds_latest_full_load(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    first = load_full(store, tmp_path)
    second = load_full(store, tmp_path, rows=9)
    collect_run(store)
    assert {r.source for r in discover(store, LAYOUT, "full")} == {"full"}
    assert {r.source for r in discover(store, LAYOUT, "api")} == {"api"}
    assert latest_complete_full_load(store, LAYOUT).run_id in {first.run_id, second.run_id}
    assert len(completed(store, LAYOUT, "full")) == 2


def test_missing_or_resized_objects_make_a_batch_incomplete(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    run = collect_run(store)
    page = tmp_path / "zone" / run.metadata["raw_objects"][0]["path"]
    page.write_bytes(page.read_bytes()[:-5])  # truncated page
    [found] = discover(store, LAYOUT, "api")
    assert not found.complete and "size differs" in found.reason
    page.unlink()
    assert "missing" in discover(store, LAYOUT, "api")[0].reason


def test_invalid_markers_are_reported_incomplete(tmp_path: Path) -> None:
    store = LocalStore(tmp_path / "zone")
    full = load_full(store, tmp_path)
    api = collect_run(store)
    (tmp_path / "zone" / full.manifest_key).write_text("{ not json")
    meta_key = LAYOUT.run_metadata_key(api.run_id, "2026-11-13")
    broken = dict(api.metadata, outcome="maybe")
    (tmp_path / "zone" / meta_key).write_text(json.dumps(broken))
    statuses = {r.source: r for r in discover(store, LAYOUT)}
    assert not statuses["full"].complete and "invalid manifest" in statuses["full"].reason
    assert not statuses["api"].complete and "invalid run metadata" in statuses["api"].reason


def test_discovery_cli_lists_json(tmp_path: Path, capsys) -> None:
    store = LocalStore(tmp_path / "zone")
    run = collect_run(store)
    collect_run(store, max_pages=1)
    assert discovery_main(["--sink", "local", "--out", str(tmp_path / "zone"), "--json"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert [r["run_id"] for r in listed] == [run.run_id] and listed[0]["complete"] is True
    assert (
        discovery_main(
            ["--sink", "local", "--out", str(tmp_path / "zone"), "--incomplete", "--json"]
        )
        == 0
    )
    assert len(json.loads(capsys.readouterr().out)) == 2


# ---------------------------------------------------------------- sync --------------------
@pytest.fixture
def collected(tmp_path: Path):
    """A 'GitHub artifact': runs written by the stateless collector into a local folder."""
    root = tmp_path / "artifact"
    src = LocalStore(root)
    good = collect_run(src)
    return root, src, good


def make_dest(tmp_path: Path):
    dest = LocalStore(tmp_path / "minio")
    return dest, HighWaterMarkStore(dest, LAYOUT.state_key)


def test_sync_uploads_runs_unchanged_and_advances_the_checkpoint(tmp_path: Path, collected) -> None:
    root, _, run = collected
    dest, hwm = make_dest(tmp_path)
    report = sync_directory(root, dest, LAYOUT, hwm)
    assert report.ok and len(report.uploaded) == 1 and not report.skipped
    for item in run.metadata["raw_objects"]:
        assert dest.get_bytes(item["path"]) == (root / item["path"]).read_bytes()
    marker = LAYOUT.run_metadata_key(run.run_id, "2026-11-13")
    assert dest.get_bytes(marker) == (root / marker).read_bytes()  # original run_id and bytes
    assert hwm.read().complaint_id == "1024"
    assert [r.complete for r in discover(dest, LAYOUT)] == [True]


def test_already_synced_runs_are_skipped(tmp_path: Path, collected) -> None:
    root, _, _ = collected
    dest, hwm = make_dest(tmp_path)
    sync_directory(root, dest, LAYOUT, hwm)
    objects = sorted(o.key for o in dest.list_objects(""))
    again = sync_directory(root, dest, LAYOUT, hwm)
    assert len(again.skipped) == 1 and not again.uploaded
    assert sorted(o.key for o in dest.list_objects("")) == objects


def test_tampered_page_is_rejected_and_nothing_is_uploaded(tmp_path: Path, collected) -> None:
    root, _, run = collected
    page = root / run.metadata["raw_objects"][1]["path"]
    data = bytearray(page.read_bytes())
    data[100] ^= 0x01  # same size, different content
    page.write_bytes(bytes(data))
    dest, hwm = make_dest(tmp_path)
    report = sync_directory(root, dest, LAYOUT, hwm)
    assert not report.ok and "SHA-256 mismatch" in report.rejected[0][1]
    assert list(dest.list_objects("")) == []
    assert hwm.read() is None


def test_missing_page_is_rejected(tmp_path: Path, collected) -> None:
    root, _, run = collected
    (root / run.metadata["raw_objects"][0]["path"]).unlink()
    dest, hwm = make_dest(tmp_path)
    report = sync_directory(root, dest, LAYOUT, hwm)
    assert not report.ok and "missing" in report.rejected[0][1]
    assert list(dest.list_objects("")) == []


def test_partial_runs_are_synced_for_provenance_but_never_move_the_checkpoint(
    tmp_path: Path,
) -> None:
    root = tmp_path / "artifact"
    run = collect_run(LocalStore(root), max_pages=1)
    dest, hwm = make_dest(tmp_path)
    report = sync_directory(root, dest, LAYOUT, hwm)
    assert report.ok and len(report.uploaded) == 1
    assert hwm.read() is None
    assert not any(r.complete for r in discover(dest, LAYOUT))
    assert run.outcome == "partial"


def test_existing_object_with_different_content_is_never_overwritten(
    tmp_path: Path, collected
) -> None:
    root, _, run = collected
    dest, hwm = make_dest(tmp_path)
    first_page = run.metadata["raw_objects"][0]["path"]
    dest.put_bytes(first_page, b"something else entirely")
    report = sync_directory(root, dest, LAYOUT, hwm)
    assert not report.ok and "different content" in report.rejected[0][1]
    assert dest.get_bytes(first_page) == b"something else entirely"
    assert not dest.exists(LAYOUT.run_metadata_key(run.run_id, "2026-11-13"))


def test_interrupted_earlier_sync_is_completed_on_retry(tmp_path: Path, collected) -> None:
    root, _, run = collected
    dest, hwm = make_dest(tmp_path)
    first = run.metadata["raw_objects"][0]["path"]
    dest.put_bytes(first, (root / first).read_bytes())  # pages arrived, marker did not
    report = sync_directory(root, dest, LAYOUT, hwm)
    assert report.ok and len(report.uploaded) == 1
    assert [r.complete for r in discover(dest, LAYOUT)] == [True]


def test_invalid_metadata_and_mismatched_path_are_rejected(tmp_path: Path, collected) -> None:
    root, _, run = collected
    marker = root / LAYOUT.run_metadata_key(run.run_id, "2026-11-13")
    dest, hwm = make_dest(tmp_path)

    original = marker.read_text()
    marker.write_text(json.dumps({**run.metadata, "outcome": "unknown"}))
    assert "invalid run_metadata.json" in sync_directory(root, dest, LAYOUT, hwm).rejected[0][1]

    marker.write_text(json.dumps({**run.metadata, "run_id": "api_20250101T000000Z_abcdef"}))
    assert "does not match" in sync_directory(root, dest, LAYOUT, hwm).rejected[0][1]
    marker.write_text(original)
    assert sync_directory(root, dest, LAYOUT, hwm).ok


def test_runs_sync_oldest_first_so_the_checkpoint_ends_on_the_newest(tmp_path: Path) -> None:
    root = tmp_path / "artifact"
    src = LocalStore(root)
    older = datetime(2026, 11, 12, 9, 0, 0, tzinfo=UTC)
    settings = IncrementalSettings(
        endpoint="https://api.example.org/",
        page_size=10,
        backoff_base_seconds=0,
        request_delay_seconds=0,
    )
    for moment, count in ((NOW, 25), (older, 12)):  # newest written first on purpose
        client = HttpClient(
            RetryPolicy(5, 2, 0),
            session=ScriptedSession(sim=ApiSim(make_records(count))),
            sleep=no_sleep,
        )
        IncrementalCollector(
            src,
            LAYOUT,
            settings,
            ("Product A", "Product B"),
            client,
            clock=lambda m=moment: m,
            sleep=no_sleep,
            version="abc1234",
        ).run()
    dest, hwm = make_dest(tmp_path)
    assert len(sync_directory(root, dest, LAYOUT, hwm).uploaded) == 2
    assert hwm.read().complaint_id == "1024"


def test_sync_dry_run_verifies_but_uploads_nothing(tmp_path: Path, collected) -> None:
    root, _, _ = collected
    dest, hwm = make_dest(tmp_path)
    report = sync_directory(root, dest, LAYOUT, hwm, dry_run=True)
    assert report.ok and len(report.uploaded) == 1
    assert list(dest.list_objects("")) == [] and hwm.read() is None


def test_sync_cli_from_directory(tmp_path: Path, collected, capsys) -> None:
    root, _, _ = collected
    argv = ["--from-dir", str(root), "--sink", "local", "--out", str(tmp_path / "minio")]
    assert sync_main(argv) == 0
    assert len(json.loads(capsys.readouterr().out)["uploaded"]) == 1
    assert sync_main(argv) == 0
    assert len(json.loads(capsys.readouterr().out)["skipped_already_synced"]) == 1


def test_sync_cli_without_token_or_directory_is_a_usage_error(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    assert sync_main(["--sink", "local", "--out", str(tmp_path / "m")]) == 2
    assert "GITHUB_TOKEN" in capsys.readouterr().err


def test_safe_extract_blocks_path_traversal(tmp_path: Path) -> None:
    evil = tmp_path / "evil.zip"
    with zipfile.ZipFile(evil, "w") as archive:
        archive.writestr("../escape.txt", "x")
    with pytest.raises(SyncError, match="unsafe path"):
        safe_extract(evil, tmp_path / "out")
    assert not (tmp_path / "escape.txt").exists()

    fine = tmp_path / "fine.zip"
    with zipfile.ZipFile(fine, "w") as archive:
        archive.writestr("raw/a.json", "{}")
    safe_extract(fine, tmp_path / "out")
    assert (tmp_path / "out" / "raw" / "a.json").read_text() == "{}"


def test_github_artifact_client_lists_filters_and_downloads(tmp_path: Path) -> None:
    payload = make_zip_bytes({"raw/a.json": "{}"})
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    def respond(req) -> Resp:
        if req.path.endswith("/actions/artifacts"):
            page = req.query.get("page", ["1"])[0]
            artifacts = (
                []
                if page != "1"
                else [
                    {
                        "id": 1,
                        "name": "api-run-1-1",
                        "created_at": now,
                        "expired": False,
                        "archive_download_url": f"{server.url}/dl/1",
                    },
                    {
                        "id": 2,
                        "name": "api-run-2-1",
                        "created_at": now,
                        "expired": True,
                        "archive_download_url": f"{server.url}/dl/2",
                    },
                    {
                        "id": 3,
                        "name": "something-else",
                        "created_at": now,
                        "expired": False,
                        "archive_download_url": f"{server.url}/dl/3",
                    },
                    {
                        "id": 4,
                        "name": "api-run-0-1",
                        "created_at": "2020-01-01T00:00:00Z",
                        "expired": False,
                        "archive_download_url": f"{server.url}/dl/4",
                    },
                ]
            )
            return Resp(
                200,
                {"Content-Type": "application/json"},
                json.dumps({"artifacts": artifacts}).encode(),
            )
        return Resp(200, {}, payload)

    with MockServer(respond) as server:
        client = GitHubArtifacts(
            "o/r",
            "secret-token",
            HttpClient(RetryPolicy(5, 1, 0), sleep=no_sleep),
            api_base=server.url,
        )
        since = datetime(2026, 1, 1, tzinfo=UTC)
        found = client.list_recent("api-run-", since)
        assert [a["id"] for a in found] == [1]  # expired, other names and old ones are filtered
        client.download(found[0], tmp_path / "a.zip")
        assert (tmp_path / "a.zip").read_bytes() == payload
        assert server.requests[0].headers["Authorization"] == "Bearer secret-token"
        assert server.requests[0].headers["Accept"] == "application/vnd.github+json"


# ---------------------------------------------------------------- dev sample --------------
def test_sample_contains_the_required_edge_cases(tmp_path: Path) -> None:
    make_sample.main(["--out", str(tmp_path)])
    with (tmp_path / "complaints_sample.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    ids = [r["Complaint ID"] for r in rows]
    assert len(ids) != len(set(ids))  # duplicate complaint_id
    assert any(r["State"] == "" for r in rows)  # missing state
    assert any(r["Date received"] == "2026-13-45" for r in rows)  # invalid date
    assert any("\n" in r["Issue"] for r in rows)  # newline inside a quoted field


def test_sample_api_pages_are_valid_for_the_collector(tmp_path: Path) -> None:
    make_sample.main(["--out", str(tmp_path)])
    pages = [parse_page((tmp_path / f"api_response_{n:04d}.json").read_bytes()) for n in (1, 2)]
    assert sum(len(p.ids) for p in pages) == 41 and pages[0].total == 41
    assert len(set(pages[0].ids) | set(pages[1].ids)) == 40  # one duplicate across the pages


def test_committed_sample_is_up_to_date_and_small() -> None:
    sample_dir = REPO_ROOT / "data" / "sample"
    expected = {
        "complaints_sample.csv",
        "api_response_0001.json",
        "api_response_0002.json",
    }
    assert expected <= {p.name for p in sample_dir.iterdir()}
    assert sum((sample_dir / n).stat().st_size for n in expected) < 100_000
    records = make_sample.synthetic_records()
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(make_sample.csv_header())
    writer.writerows(make_sample.csv_rows(records))
    committed = (sample_dir / "complaints_sample.csv").read_bytes().decode("utf-8")
    assert committed.replace("\r\n", "\n") == buffer.getvalue().replace("\r\n", "\n")


def test_sample_from_zip_streams_only_the_first_rows(tmp_path: Path) -> None:
    archive = tmp_path / "c.zip"
    archive.write_bytes(make_zip_bytes({"complaints.csv": csv_text(100)}))
    header, rows = make_sample.sample_from_zip(archive, 3)
    assert len(rows) == 3 and header[0] == "Date received"
