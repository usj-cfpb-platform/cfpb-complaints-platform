"""Raw-zone storage: one small interface, two backends.

* ``MinioStore``  - the real raw zone (bucket from ``MINIO_BUCKET``), S3 API.
* ``LocalStore``  - a plain folder with the same key layout. Used on the GitHub Actions runner
  (which has no MinIO; the folder is uploaded as a workflow artifact), for dry runs and tests.

Contract rule enforced here: raw objects are **write-once**. ``put_*`` refuses to replace an
existing object (``ObjectExistsError``) unless ``overwrite=True``, which only the high-water-mark
state file is allowed to use.
"""

from __future__ import annotations

import io
import os
import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from common.config import env

if TYPE_CHECKING:
    from minio import Minio

MIB = 1024 * 1024
MULTIPART_PART_SIZE = 64 * MIB  # large archives are uploaded in 64 MiB parts, never held in RAM
_MISSING_CODES = {"NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound"}


class StorageError(RuntimeError):
    """The storage backend failed or returned something inconsistent."""


class ObjectExistsError(StorageError):
    """A write-once object already exists; it was NOT replaced."""


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int


class RawStore(Protocol):
    """What the collectors need from a storage backend."""

    def exists(self, key: str) -> bool: ...

    def stat(self, key: str) -> ObjectInfo | None: ...

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> ObjectInfo: ...

    def put_file(
        self,
        key: str,
        path: Path,
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> ObjectInfo: ...

    def get_bytes(self, key: str) -> bytes: ...

    def list_objects(self, prefix: str) -> Iterator[ObjectInfo]: ...


def validate_key(key: str) -> str:
    """Reject keys that could escape the zone or are not canonical."""
    if not key or key.startswith("/") or "\\" in key or "//" in key:
        raise StorageError(f"Invalid object key: {key!r}")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise StorageError(f"Invalid object key: {key!r}")
    return key


# ---------------------------------------------------------------------------------------
class LocalStore:
    """Folder-backed store with the same keys as MinIO."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / validate_key(key)

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def stat(self, key: str) -> ObjectInfo | None:
        path = self._path(key)
        return ObjectInfo(key, path.stat().st_size) if path.is_file() else None

    def _guard(self, key: str, overwrite: bool) -> Path:
        path = self._path(key)
        if path.exists() and not overwrite:
            raise ObjectExistsError(f"Refusing to overwrite write-once object: {key}")
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> ObjectInfo:
        path = self._guard(key, overwrite)
        self._atomic_write(path, io.BytesIO(data))
        return ObjectInfo(key, len(data))

    def put_file(
        self,
        key: str,
        path: Path,
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> ObjectInfo:
        target = self._guard(key, overwrite)
        with Path(path).open("rb") as source:
            self._atomic_write(target, source)
        return ObjectInfo(key, target.stat().st_size)

    @staticmethod
    def _atomic_write(target: Path, source: Any) -> None:
        """Write to a temp file in the same folder, then rename: no half-written objects."""
        fd, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=".tmp-", suffix=".part")
        try:
            with os.fdopen(fd, "wb") as out:
                shutil.copyfileobj(source, out, length=MIB)
            os.chmod(tmp_name, 0o644)  # mkstemp creates 0600; objects should be readable
            os.replace(tmp_name, target)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise

    def get_bytes(self, key: str) -> bytes:
        path = self._path(key)
        if not path.is_file():
            raise StorageError(f"Object not found: {key}")
        return path.read_bytes()

    def list_objects(self, prefix: str) -> Iterator[ObjectInfo]:
        base = self.root
        wanted = prefix.strip("/")
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.name.startswith(".tmp-"):
                continue
            key = path.relative_to(base).as_posix()
            if key.startswith(wanted):
                yield ObjectInfo(key, path.stat().st_size)


# ---------------------------------------------------------------------------------------
class MinioStore:
    """MinIO / S3 backend built on the official ``minio`` client (already in requirements.txt)."""

    def __init__(self, client: Minio | Any, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    @classmethod
    def from_env(cls) -> MinioStore:
        """Build from ``MINIO_ENDPOINT``, ``MINIO_BUCKET``, ``MINIO_ROOT_USER/PASSWORD``.

        Credentials come only from the environment (injected from ``.env``); they are never
        written to logs, metadata or code.
        """
        from minio import Minio  # imported here so unit tests need no MinIO client

        from scripts.init_storage import parse_endpoint

        host, secure = parse_endpoint(env("MINIO_ENDPOINT"))
        client = Minio(
            host,
            access_key=env("MINIO_ROOT_USER"),
            secret_key=env("MINIO_ROOT_PASSWORD"),
            secure=secure,
        )
        return cls(client, env("MINIO_BUCKET"))

    # -- helpers ------------------------------------------------------------------------
    @staticmethod
    def _is_missing(exc: Exception) -> bool:
        return getattr(exc, "code", None) in _MISSING_CODES

    def stat(self, key: str) -> ObjectInfo | None:
        validate_key(key)
        try:
            result = self.client.stat_object(self.bucket, key)
        except Exception as exc:
            if self._is_missing(exc):
                return None
            raise StorageError(f"stat failed for {key}: {exc}") from exc
        return ObjectInfo(key, int(result.size))

    def exists(self, key: str) -> bool:
        return self.stat(key) is not None

    def _check_writable(self, key: str, overwrite: bool) -> None:
        validate_key(key)
        if not overwrite and self.exists(key):
            raise ObjectExistsError(f"Refusing to overwrite write-once object: {key}")

    def _verify(self, key: str, expected_size: int) -> ObjectInfo:
        info = self.stat(key)
        if info is None or info.size != expected_size:
            got = None if info is None else info.size
            raise StorageError(
                f"Upload verification failed for {key}: expected {expected_size} bytes, found {got}"
            )
        return info

    # -- writes -------------------------------------------------------------------------
    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> ObjectInfo:
        self._check_writable(key, overwrite)
        try:
            self.client.put_object(
                self.bucket,
                key,
                io.BytesIO(data),
                len(data),
                content_type=content_type,
                metadata=metadata,
            )
        except Exception as exc:
            raise StorageError(f"upload failed for {key}: {exc}") from exc
        return self._verify(key, len(data))

    def put_file(
        self,
        key: str,
        path: Path,
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> ObjectInfo:
        self._check_writable(key, overwrite)
        size = Path(path).stat().st_size
        try:
            self.client.fput_object(
                self.bucket,
                key,
                str(path),
                content_type=content_type,
                metadata=metadata,
                part_size=MULTIPART_PART_SIZE,
            )
        except Exception as exc:
            raise StorageError(f"upload failed for {key}: {exc}") from exc
        return self._verify(key, size)

    # -- reads --------------------------------------------------------------------------
    def get_bytes(self, key: str) -> bytes:
        validate_key(key)
        try:
            response = self.client.get_object(self.bucket, key)
        except Exception as exc:
            raise StorageError(f"download failed for {key}: {exc}") from exc
        try:
            return response.read()
        finally:
            response.close()
            release = getattr(response, "release_conn", None)
            if release:
                release()

    def list_objects(self, prefix: str) -> Iterator[ObjectInfo]:
        try:
            for item in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
                yield ObjectInfo(item.object_name, int(item.size or 0))
        except Exception as exc:
            raise StorageError(f"list failed for prefix {prefix}: {exc}") from exc


def make_store(sink: str, out_dir: Path | str | None = None) -> RawStore:
    """Factory used by every CLI: ``minio`` (default) or ``local`` (needs ``out_dir``)."""
    if sink == "minio":
        return MinioStore.from_env()
    if sink == "local":
        if out_dir is None:
            raise StorageError("--out is required with --sink local")
        return LocalStore(out_dir)
    raise StorageError(f"Unknown sink: {sink!r}")
