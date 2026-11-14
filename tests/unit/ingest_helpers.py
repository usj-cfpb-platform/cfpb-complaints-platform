"""Test support for the Turn 2 collectors (no network, no MinIO, no Docker needed).

* ``MockServer``   - a real HTTP server on localhost (threads) with a scriptable response, so
  downloads are exercised over real sockets: Range requests, truncated bodies, timeouts, 5xx.
* ``FileServe``    - a ready-made handler that serves one file with HEAD / Range / If-Range.
* ``ApiSim``       - a tiny simulation of the CFPB search API (date window, product filter,
  ``size``, ``sort`` and ``search_after``) so pagination is tested end to end.
* ``ScriptedSession`` - a ``requests.Session`` stand-in driven by ``ApiSim`` or a script.
* ``FakeMinio``    - an in-memory object with the subset of the ``minio`` client API we use.
"""

from __future__ import annotations

import io
import json
import threading
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

CSV_HEADER = ["Date received", "Product", "State", "Complaint ID"]


# --------------------------------------------------------------------------------------
# ZIP / CSV fixtures
# --------------------------------------------------------------------------------------
def csv_text(rows: int = 5, header: list[str] | None = None) -> str:
    head = header or CSV_HEADER
    lines = [",".join(head)]
    for i in range(rows):
        lines.append(f"2026-10-{i + 1:02d},Product A,CA,{1000 + i}")
    return "\n".join(lines) + "\n"


def make_zip_bytes(
    members: dict[str, str | bytes] | None = None, compression: int = zipfile.ZIP_DEFLATED
) -> bytes:
    members = members if members is not None else {"complaints.csv": csv_text()}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression) as archive:
        for name, content in members.items():
            archive.writestr(name, content)
    return buffer.getvalue()


# --------------------------------------------------------------------------------------
# Real HTTP server
# --------------------------------------------------------------------------------------
@dataclass
class Req:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]


@dataclass
class Resp:
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    truncate_to: int | None = None  # send this many body bytes, then drop the connection
    delay: float = 0.0


class MockServer:
    def __init__(self, respond: Callable[[Req], Resp] | None = None) -> None:
        self.respond = respond or (lambda req: Resp(404))
        self.requests: list[Req] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:  # silence test noise
                return

            def _serve(self) -> None:
                parsed = urlparse(self.path)
                req = Req(
                    self.command,
                    parsed.path,
                    parse_qs(parsed.query),
                    {k: v for k, v in self.headers.items()},
                )
                outer.requests.append(req)
                resp = outer.respond(req)
                if resp.delay:
                    time.sleep(resp.delay)
                self.send_response(resp.status)
                for key, value in resp.headers.items():
                    self.send_header(key, value)
                if "Content-Length" not in resp.headers:
                    self.send_header("Content-Length", str(len(resp.body)))
                self.end_headers()
                if self.command != "HEAD":
                    chunk = resp.body if resp.truncate_to is None else resp.body[: resp.truncate_to]
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                if resp.truncate_to is not None:
                    self.close_connection = True

            do_GET = _serve
            do_HEAD = _serve

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> MockServer:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def count(self, method: str | None = None) -> int:
        return sum(1 for r in self.requests if method is None or r.method == method)


class FileServe:
    """Serve one file like a real file host: HEAD, Range, If-Range, optional failures."""

    def __init__(
        self,
        body: bytes,
        *,
        etag: str | None = '"v1"',
        last_modified: str | None = "Fri, 13 Nov 2026 10:00:00 GMT",
        accept_ranges: bool = True,
        fail_statuses: list[int] | None = None,
        truncate_after: list[int] | None = None,
        delays: list[float] | None = None,
    ) -> None:
        self.body = body
        self.etag = etag
        self.last_modified = last_modified
        self.accept_ranges = accept_ranges
        self.fail_statuses = list(fail_statuses or [])
        self.truncate_after = list(truncate_after or [])
        self.delays = list(delays or [])

    def _validators(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.etag:
            headers["ETag"] = self.etag
        if self.last_modified:
            headers["Last-Modified"] = self.last_modified
        if self.accept_ranges:
            headers["Accept-Ranges"] = "bytes"
        return headers

    def __call__(self, req: Req) -> Resp:
        if req.method == "HEAD":
            return Resp(200, {**self._validators(), "Content-Length": str(len(self.body))})
        if self.fail_statuses:
            return Resp(self.fail_statuses.pop(0))
        delay = self.delays.pop(0) if self.delays else 0.0
        headers = self._validators()
        body, status = self.body, 200
        range_header = req.headers.get("Range")
        if range_header and self.accept_ranges:
            if_range = req.headers.get("If-Range")
            if if_range is None or if_range in {self.etag, self.last_modified}:
                start = int(range_header.split("=")[1].split("-")[0])
                if start >= len(self.body):
                    return Resp(416, {"Content-Range": f"bytes */{len(self.body)}"})
                body, status = self.body[start:], 206
                headers["Content-Range"] = f"bytes {start}-{len(self.body) - 1}/{len(self.body)}"
        truncate = self.truncate_after.pop(0) if self.truncate_after else None
        return Resp(status, headers, body, truncate_to=truncate, delay=delay)


# --------------------------------------------------------------------------------------
# CFPB API simulation
# --------------------------------------------------------------------------------------
def make_records(count: int, first_day: date = date(2026, 10, 1), per_day: int = 1) -> list[dict]:
    """Records in publication order; ``complaint_id`` and ``date_received`` increase together."""
    records = []
    for i in range(count):
        received = first_day + timedelta(days=i // per_day)
        records.append(
            {
                "complaint_id": str(1000 + i),
                "date_received": received.isoformat(),
                "product": "Product A" if i % 2 == 0 else "Product B",
                "state": "CA",
                "company": f"Company {i % 3}",
                "created_ms": 1_790_000_000_000 + i * 1000,
            }
        )
    return records


@dataclass
class ApiSim:
    """Behaves like the search API for the parameters the collector uses."""

    records: list[dict]
    emit_break_points: bool = False
    emit_sort: bool = True
    ignore_search_after: bool = False  # buggy server: always answers page 1
    skip_one_per_page: bool = False  # buggy server: break_points skip a record per page
    exact_total: bool = True
    is_data_stale: bool = False
    calls: list[list[tuple[str, str]]] = field(default_factory=list)

    @staticmethod
    def _cursor(record: dict) -> str:
        return f"{record['created_ms']}_{record['complaint_id']}"

    def answer(self, params: list[tuple[str, str]]) -> dict[str, Any]:
        self.calls.append(list(params))
        query: dict[str, list[str]] = {}
        for key, value in params:
            query.setdefault(key, []).append(value)
        lo = query.get("date_received_min", ["0000-01-01"])[0]
        hi = query.get("date_received_max", ["9999-12-31"])[0]
        products = query.get("product")
        size = int(query.get("size", ["10"])[0])
        matching = [
            r
            for r in self.records
            if lo <= r["date_received"] < hi and (not products or r["product"] in products)
        ]
        matching.sort(key=lambda r: (r["created_ms"], r["complaint_id"]), reverse=True)
        total = len(matching)
        cursor = None if self.ignore_search_after else query.get("search_after", [None])[0]
        if cursor:
            matching = [r for r in matching if self._cursor(r) < cursor]
        page = matching[:size]
        hits = []
        for r in page:
            hit: dict[str, Any] = {
                "_id": r["complaint_id"],
                "_source": {k: v for k, v in r.items() if k != "created_ms"},
            }
            if self.emit_sort:
                hit["sort"] = [r["created_ms"], r["complaint_id"]]
            hits.append(hit)
        meta: dict[str, Any] = {
            "license": "CC0",
            "last_updated": "2026-11-13T12:00:00-05:00",
            "last_indexed": "2026-11-13T12:00:00-05:00",
            "total_record_count": len(self.records),
            "is_data_stale": self.is_data_stale,
            "has_data_issue": False,
        }
        if self.emit_break_points and page:
            # Cursor for the NEXT page, keyed by its page number (this call is page len(calls)).
            # A buggy server can point one record too far ahead, silently skipping a record.
            skip = 1 if self.skip_one_per_page else 0
            index = min(size - 1 + skip, len(matching) - 1)
            meta["break_points"] = {str(len(self.calls) + 1): self._cursor(matching[index])}
        return {
            "_meta": meta,
            "hits": {
                "total": {"value": total, "relation": "eq" if self.exact_total else "gte"},
                "hits": hits,
            },
        }


class ScriptedSession:
    """Stands in for ``requests.Session``: answers from a script of items, or from ``ApiSim``.

    Script items: ``int`` (HTTP status with empty body), ``bytes`` (200 body), ``dict`` (200 JSON),
    an ``Exception`` instance (raised), or a callable ``(params) -> item``.
    """

    def __init__(self, script: list[Any] | None = None, sim: ApiSim | None = None) -> None:
        self.script = list(script or [])
        self.sim = sim
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, Any]] = []

    @staticmethod
    def _response(status: int, body: bytes = b"", headers: dict[str, str] | None = None):
        response = requests.Response()
        response.status_code = status
        response._content = body
        response._content_consumed = True
        response.headers.update(headers or {})
        return response

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        self.calls.append({"method": method, "url": url, **kwargs})
        item: Any
        if self.script:
            item = self.script.pop(0)
            if callable(item):
                item = item(kwargs.get("params"))
        elif self.sim is not None:
            item = self.sim.answer(list(kwargs.get("params") or []))
        else:
            raise AssertionError("ScriptedSession ran out of scripted responses")
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, int):
            return self._response(item)
        if isinstance(item, bytes):
            return self._response(200, item)
        return self._response(200, json.dumps(item).encode("utf-8"))


# --------------------------------------------------------------------------------------
# In-memory stand-in for the minio client
# --------------------------------------------------------------------------------------
class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass
class _Stat:
    size: int


@dataclass
class _Listed:
    object_name: str
    size: int


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data

    def close(self) -> None:
        pass

    def release_conn(self) -> None:
        pass


class FakeMinio:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.user_metadata: dict[str, dict[str, str]] = {}
        self.fail_on: Callable[[str], bool] | None = None
        self.report_wrong_size = False

    def _maybe_fail(self, key: str) -> None:
        if self.fail_on and self.fail_on(key):
            raise OSError(f"simulated upload failure for {key}")

    def put_object(self, bucket, key, data, length, content_type=None, metadata=None):
        self._maybe_fail(key)
        self.objects[key] = data.read()
        self.user_metadata[key] = dict(metadata or {})

    def fput_object(self, bucket, key, file_path, content_type=None, metadata=None, part_size=0):
        self._maybe_fail(key)
        self.objects[key] = Path(file_path).read_bytes()
        self.user_metadata[key] = dict(metadata or {})

    def stat_object(self, bucket, key):
        if key not in self.objects:
            raise FakeS3Error("NoSuchKey")
        size = len(self.objects[key])
        return _Stat(size + 1 if self.report_wrong_size else size)

    def get_object(self, bucket, key):
        if key not in self.objects:
            raise FakeS3Error("NoSuchKey")
        return _Body(self.objects[key])

    def list_objects(self, bucket, prefix="", recursive=True):
        for key in sorted(self.objects):
            if key.startswith(prefix):
                yield _Listed(key, len(self.objects[key]))


def no_sleep(_: float) -> None:
    return None


class FlakyStore:
    """A LocalStore whose writes can be made to fail for chosen keys (outage simulation)."""

    def __new__(cls, root: Path, rule: Callable[[str], Exception | None]):
        from ingestion.storage import LocalStore

        class _Flaky(LocalStore):
            def put_bytes(self, key, data, **kwargs):
                if (exc := rule(key)) is not None:
                    raise exc
                return super().put_bytes(key, data, **kwargs)

            def put_file(self, key, path, **kwargs):
                if (exc := rule(key)) is not None:
                    raise exc
                return super().put_file(key, path, **kwargs)

        return _Flaky(root)
