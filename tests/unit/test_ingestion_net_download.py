"""HTTP client (retries/backoff/timeouts) and the resumable streaming download."""

import hashlib
import json
from pathlib import Path

import pytest
import requests
from ingest_helpers import FileServe, MockServer, Resp, ScriptedSession, no_sleep

from ingestion.download import (
    DownloadError,
    download_resumable,
    head_source,
    sha256_file,
)
from ingestion.net import HttpClient, HttpStatusError, RetriesExhausted, RetryPolicy


def make_client(**policy: float) -> tuple[HttpClient, list[float]]:
    sleeps: list[float] = []
    defaults = {"timeout_seconds": 5, "max_retries": 3, "backoff_base_seconds": 2}
    client = HttpClient(RetryPolicy(**{**defaults, **policy}), sleep=sleeps.append)
    return client, sleeps


# ---------------------------------------------------------------- HttpClient ---------------
def test_500_then_success_is_retried_and_counted() -> None:
    with MockServer() as server:
        replies = [Resp(500), Resp(200, body=b"ok")]
        server.respond = lambda req: replies.pop(0)
        client, sleeps = make_client()
        response = client.request("GET", server.url + "/x")
        assert response.content == b"ok"
        assert client.retry_count == 1
        assert client.last_status == 200
        assert sleeps == [2]


def test_404_fails_fast_without_retry() -> None:
    with MockServer(lambda req: Resp(404)) as server:
        client, sleeps = make_client()
        with pytest.raises(HttpStatusError) as excinfo:
            client.request("GET", server.url + "/missing")
        assert excinfo.value.status == 404
        assert server.count() == 1
        assert client.retry_count == 0 and sleeps == []


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410])
def test_client_errors_are_never_retried(status: int) -> None:
    with MockServer(lambda req: Resp(status)) as server:
        client, _ = make_client()
        with pytest.raises(HttpStatusError):
            client.request("GET", server.url)
        assert server.count() == 1


def test_retries_are_bounded_never_infinite() -> None:
    with MockServer(lambda req: Resp(503)) as server:
        client, sleeps = make_client(max_retries=3)
        with pytest.raises(RetriesExhausted):
            client.request("GET", server.url)
        assert server.count() == 4  # 1 attempt + 3 retries
        assert client.retry_count == 3
        assert len(sleeps) == 3


def test_backoff_is_exponential() -> None:
    policy = RetryPolicy(timeout_seconds=1, max_retries=5, backoff_base_seconds=2)
    assert [policy.delay(n) for n in (1, 2, 3, 4)] == [2, 4, 8, 16]
    assert policy.delay(20) == 60  # capped


def test_429_is_retried_and_retry_after_is_honoured() -> None:
    with MockServer() as server:
        replies = [Resp(429, {"Retry-After": "7"}), Resp(200, body=b"ok")]
        server.respond = lambda req: replies.pop(0)
        client, sleeps = make_client()
        client.request("GET", server.url)
        assert sleeps == [7]


def test_timeout_is_retried_then_fails_with_exhausted() -> None:
    with MockServer(lambda req: Resp(200, body=b"slow", delay=0.6)) as server:
        client, sleeps = make_client(timeout_seconds=0.15, max_retries=2)
        with pytest.raises(RetriesExhausted, match="Timeout|timed out|Read"):
            client.request("GET", server.url)
        assert client.retry_count == 2 and len(sleeps) == 2


def test_timeout_then_success() -> None:
    with MockServer() as server:
        replies = [Resp(200, body=b"slow", delay=0.6), Resp(200, body=b"fast")]
        server.respond = lambda req: replies.pop(0)
        client, _ = make_client(timeout_seconds=0.15)
        assert client.request("GET", server.url).content == b"fast"
        assert client.retry_count == 1


def test_connection_error_is_retried() -> None:
    session = ScriptedSession([requests.ConnectionError("reset"), 200])
    client = HttpClient(RetryPolicy(5, 3, 1), session=session, sleep=no_sleep)
    assert client.request("GET", "http://x").status_code == 200
    assert client.retry_count == 1


def test_policy_validation() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(timeout_seconds=0)
    with pytest.raises(ValueError):
        RetryPolicy(max_retries=-1)


# ---------------------------------------------------------------- head_source -------------
def test_head_source_reads_size_and_validators() -> None:
    body = b"x" * 123
    with MockServer(FileServe(body)) as server:
        client, _ = make_client()
        info = head_source(client, server.url + "/f.zip")
        assert info.content_length == 123
        assert info.etag == '"v1"'
        assert info.accepts_ranges is True
        assert server.requests[0].method == "HEAD"


# ---------------------------------------------------------------- download ----------------
PAYLOAD = bytes(range(256)) * 400  # 102,400 bytes


def test_download_success_checksum_and_streaming(tmp_path: Path) -> None:
    with MockServer(FileServe(PAYLOAD)) as server:
        client, _ = make_client()
        result = download_resumable(client, server.url + "/f", tmp_path / "f.zip", chunk_bytes=4096)
    assert result.size_bytes == len(PAYLOAD)
    assert result.sha256 == hashlib.sha256(PAYLOAD).hexdigest() == sha256_file(result.path)
    assert result.etag == '"v1"'
    assert not result.resumed and result.interruptions == 0
    assert not (tmp_path / "f.zip.part").exists()


def test_interrupted_download_resumes_with_range_and_is_byte_identical(tmp_path: Path) -> None:
    serve = FileServe(PAYLOAD, truncate_after=[30_000])
    with MockServer(serve) as server:
        client, sleeps = make_client()
        result = download_resumable(client, server.url + "/f", tmp_path / "f.zip", chunk_bytes=4096)
        gets = [r for r in server.requests if r.method == "GET"]
    assert result.sha256 == hashlib.sha256(PAYLOAD).hexdigest()
    assert result.resumed and result.interruptions == 1
    assert gets[0].headers.get("Range") is None
    # Resumes from the bytes durably on disk (whole chunks), never beyond what was received.
    resume_at = int(gets[1].headers["Range"].removeprefix("bytes=").rstrip("-"))
    assert 0 < resume_at <= 30_000
    assert gets[1].headers["If-Range"] == '"v1"'
    assert sleeps  # backed off before resuming


def test_download_survives_several_interruptions(tmp_path: Path) -> None:
    serve = FileServe(PAYLOAD, truncate_after=[10_000, 5_000, 20_000])
    with MockServer(serve) as server:
        client, _ = make_client(max_retries=5)
        result = download_resumable(client, server.url + "/f", tmp_path / "f.zip", chunk_bytes=2048)
    assert result.interruptions == 3
    assert result.sha256 == hashlib.sha256(PAYLOAD).hexdigest()


def test_server_without_range_support_restarts_from_zero(tmp_path: Path) -> None:
    serve = FileServe(PAYLOAD, accept_ranges=False, truncate_after=[20_000])
    with MockServer(serve) as server:
        client, _ = make_client()
        result = download_resumable(client, server.url + "/f", tmp_path / "f.zip", chunk_bytes=4096)
    assert result.sha256 == hashlib.sha256(PAYLOAD).hexdigest()
    assert not result.resumed


def test_changed_file_on_server_discards_partial_bytes(tmp_path: Path) -> None:
    """If-Range with a stale ETag makes the server send the NEW file in full: no mixed content."""
    old, new = b"A" * 50_000, b"B" * 60_000
    destination = tmp_path / "f.zip"
    part = tmp_path / "f.zip.part"
    part.write_bytes(old[:20_000])
    (tmp_path / "f.zip.part.json").write_text('{"url": "URL", "etag": "\\"old\\""}')
    with MockServer(FileServe(new, etag='"new"')) as server:
        sidecar = {"url": f"{server.url}/f", "etag": '"old"', "last_modified": None}
        (tmp_path / "f.zip.part.json").write_text(json.dumps(sidecar))
        client, _ = make_client()
        result = download_resumable(client, server.url + "/f", destination)
    assert destination.read_bytes() == new
    assert result.sha256 == hashlib.sha256(new).hexdigest()


def test_partial_file_from_a_previous_run_is_resumed(tmp_path: Path) -> None:
    destination = tmp_path / "f.zip"
    (tmp_path / "f.zip.part").write_bytes(PAYLOAD[:40_000])
    with MockServer(FileServe(PAYLOAD)) as server:
        sidecar = {"url": f"{server.url}/f", "etag": '"v1"', "last_modified": None}
        (tmp_path / "f.zip.part.json").write_text(json.dumps(sidecar))
        client, _ = make_client()
        result = download_resumable(client, server.url + "/f", destination)
        range_header = [r.headers.get("Range") for r in server.requests if r.method == "GET"]
    assert range_header == ["bytes=40000-"]
    assert result.sha256 == hashlib.sha256(PAYLOAD).hexdigest()


def test_partial_file_for_another_url_is_not_reused(tmp_path: Path) -> None:
    (tmp_path / "f.zip.part").write_bytes(b"garbage")
    (tmp_path / "f.zip.part.json").write_text('{"url": "http://elsewhere/other"}')
    with MockServer(FileServe(PAYLOAD)) as server:
        client, _ = make_client()
        result = download_resumable(client, server.url + "/f", tmp_path / "f.zip")
    assert result.sha256 == hashlib.sha256(PAYLOAD).hexdigest()


def test_http_failure_raises_download_error(tmp_path: Path) -> None:
    with MockServer(lambda req: Resp(404)) as server:
        client, _ = make_client()
        with pytest.raises(DownloadError, match="404"):
            download_resumable(client, server.url + "/gone", tmp_path / "f.zip")
    assert not (tmp_path / "f.zip").exists()


def test_persistent_server_errors_exhaust_retries(tmp_path: Path) -> None:
    with MockServer(FileServe(PAYLOAD, fail_statuses=[500] * 20)) as server:
        client, _ = make_client(max_retries=2)
        with pytest.raises(DownloadError, match="failed after"):
            download_resumable(client, server.url + "/f", tmp_path / "f.zip")


def test_too_many_interruptions_keep_partial_file_for_next_run(tmp_path: Path) -> None:
    serve = FileServe(PAYLOAD, truncate_after=[10_000] * 10)
    with MockServer(serve) as server:
        client, _ = make_client(max_retries=2)
        with pytest.raises(DownloadError, match="partial file was kept"):
            download_resumable(client, server.url + "/f", tmp_path / "f.zip", chunk_bytes=2048)
    assert (tmp_path / "f.zip.part").exists()
    assert not (tmp_path / "f.zip").exists()  # never exposed as complete


def test_slow_server_timeout_during_download_is_retried(tmp_path: Path) -> None:
    serve = FileServe(PAYLOAD, delays=[0.6])
    with MockServer(serve) as server:
        client, _ = make_client(timeout_seconds=0.2)
        result = download_resumable(client, server.url + "/f", tmp_path / "f.zip")
    assert result.sha256 == hashlib.sha256(PAYLOAD).hexdigest()
    assert client.retry_count >= 1


def test_empty_download_is_rejected(tmp_path: Path) -> None:
    with MockServer(FileServe(b"")) as server:
        client, _ = make_client()
        with pytest.raises(DownloadError, match="empty"):
            download_resumable(client, server.url + "/f", tmp_path / "f.zip")
