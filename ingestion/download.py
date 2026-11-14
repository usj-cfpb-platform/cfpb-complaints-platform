"""Resumable, streaming file download with integrity checks.

Designed for the multi-GB CFPB ZIP on a slow, unreliable connection:

* streamed to disk in fixed-size chunks (constant memory, never the whole file in RAM);
* an interrupted transfer is **resumed** with an HTTP ``Range`` request (``If-Range`` guards
  against the file changing on the server between attempts);
* completed only when the byte count equals the size announced by the server;
* the SHA-256 checksum is computed from the finished file.

The partial file lives next to the destination as ``<name>.part`` with a ``<name>.part.json``
sidecar that remembers which source (URL + validators) the bytes belong to.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

import requests

from ingestion.net import HttpClient, HttpStatusError, RetriesExhausted

LOG = logging.getLogger("ingestion.download")

DEFAULT_CHUNK_BYTES = 1024 * 1024
_CONTENT_RANGE_RE = re.compile(r"^bytes (?P<start>\d+)-(?P<end>\d+)/(?P<total>\d+|\*)$")
_UNSATISFIED_RANGE_RE = re.compile(r"^bytes \*/(?P<total>\d+)$")


class DownloadError(RuntimeError):
    """The download could not be completed or verified."""


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    size_bytes: int
    sha256: str
    etag: str | None
    last_modified: str | None
    resumed: bool
    interruptions: int


@dataclass(frozen=True)
class SourceInfo:
    """What the server says about a file (HTTP HEAD)."""

    url: str
    content_length: int | None
    etag: str | None
    last_modified: str | None
    accepts_ranges: bool


def sha256_file(path: Path, chunk_bytes: int = DEFAULT_CHUNK_BYTES) -> str:
    """Streaming SHA-256 (constant memory)."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_bytes):
            digest.update(chunk)
    return digest.hexdigest()


def head_source(client: HttpClient, url: str) -> SourceInfo:
    """HTTP HEAD: size and validators, without downloading anything."""
    response = client.request("HEAD", url)
    try:
        headers = response.headers
        length = headers.get("Content-Length")
        return SourceInfo(
            url=url,
            content_length=int(length) if length and length.isdigit() else None,
            etag=headers.get("ETag"),
            last_modified=headers.get("Last-Modified"),
            accepts_ranges=headers.get("Accept-Ranges", "").lower() == "bytes",
        )
    finally:
        response.close()


def _read_sidecar(path: Path) -> dict[str, str | None]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _discard(*paths: Path) -> None:
    for path in paths:
        path.unlink(missing_ok=True)


def download_resumable(
    client: HttpClient,
    url: str,
    destination: Path,
    *,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
) -> DownloadResult:
    """Download ``url`` to ``destination`` (resuming an earlier partial download if valid).

    Raises ``DownloadError`` if the server cannot deliver a complete, size-consistent file within
    the retry budget of ``client.policy``. A failed download keeps its ``.part`` file so the next
    run can resume instead of starting over.
    """
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    sidecar = destination.with_name(destination.name + ".part.json")

    interruptions = 0
    resumed = False
    etag: str | None = None
    last_modified: str | None = None
    budget = client.policy.max_retries

    while True:
        offset = part.stat().st_size if part.exists() else 0
        saved = _read_sidecar(sidecar)
        if offset and saved.get("url") != url:
            LOG.warning("Partial file belongs to another source; restarting from zero")
            _discard(part, sidecar)
            offset = 0
        validator = saved.get("etag") or saved.get("last_modified") if offset else None

        headers: dict[str, str] = {"Accept-Encoding": "identity"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
            if validator:
                headers["If-Range"] = validator
            LOG.info("Resuming download at byte %d", offset)

        try:
            response = client.request("GET", url, headers=headers, stream=True)
        except HttpStatusError as exc:
            if exc.status == 416 and offset:
                # Range not satisfiable: our partial file is at (or past) the end, or stale.
                # Re-validate via a size check on a fresh full request.
                LOG.warning("HTTP 416 at offset %d; restarting from zero", offset)
                _discard(part, sidecar)
                interruptions += 1
                if interruptions > budget:
                    raise DownloadError("Too many restarts after HTTP 416") from exc
                continue
            raise DownloadError(str(exc)) from exc
        except RetriesExhausted as exc:
            raise DownloadError(str(exc)) from exc

        try:
            status = response.status_code
            etag = response.headers.get("ETag") or etag
            last_modified = response.headers.get("Last-Modified") or last_modified

            if status == 206:
                match = _CONTENT_RANGE_RE.match(response.headers.get("Content-Range", ""))
                if not match or int(match.group("start")) != offset:
                    raise DownloadError(
                        f"Unexpected Content-Range {response.headers.get('Content-Range')!r} "
                        f"for a request starting at byte {offset}"
                    )
                total = int(match.group("total")) if match.group("total") != "*" else None
                resumed = True
                mode = "ab"
            else:  # 200: full body (either first attempt, or server ignored/invalidated Range)
                if offset:
                    LOG.warning(
                        "Server restarted the transfer; discarding %d partial bytes", offset
                    )
                declared = response.headers.get("Content-Length")
                total = int(declared) if declared and declared.isdigit() else None
                offset = 0
                mode = "wb"

            sidecar.write_text(
                json.dumps({"url": url, "etag": etag, "last_modified": last_modified}),
                encoding="utf-8",
            )

            written = offset
            interrupted_by: Exception | None = None
            try:
                with part.open(mode) as out:
                    for chunk in response.iter_content(chunk_size=chunk_bytes):
                        if chunk:
                            out.write(chunk)
                            written += len(chunk)
                    out.flush()
                    os.fsync(out.fileno())
            except (requests.RequestException, OSError) as exc:
                interrupted_by = exc
        finally:
            response.close()

        if interrupted_by is None and (total is None or written == total):
            break  # complete

        interruptions += 1
        reason = repr(interrupted_by) if interrupted_by else f"got {written} of {total} bytes"
        if interruptions > budget:
            raise DownloadError(
                f"Download incomplete after {interruptions} interruptions ({reason}). "
                f"The partial file was kept at {part} and the next run will resume it."
            )
        LOG.warning("Transfer interrupted (%s); resuming", reason)
        client.retry_count += 1
        client.wait_before_retry(interruptions)

    os.replace(part, destination)
    _discard(sidecar)
    size = destination.stat().st_size
    if size == 0:
        raise DownloadError("Downloaded file is empty")
    return DownloadResult(
        path=destination,
        size_bytes=size,
        sha256=sha256_file(destination, chunk_bytes),
        etag=etag,
        last_modified=last_modified,
        resumed=resumed,
        interruptions=interruptions,
    )
