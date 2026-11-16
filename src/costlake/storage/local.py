"""Filesystem implementation of :class:`ObjectStore` (one directory per bucket)."""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from costlake.storage.base import ObjectInfo, ObjectNotFoundError


class LocalObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def _path(self, bucket: str, key: str) -> Path:
        path = (self.root / bucket / key).resolve()
        if not path.is_relative_to(self.root / bucket):
            raise ValueError(f"key escapes bucket: {key!r}")
        return path

    def list_objects(self, bucket: str, prefix: str = "") -> Iterator[ObjectInfo]:
        base = self.root / bucket
        if not base.exists():
            return
        for path in sorted(p for p in base.rglob("*") if p.is_file()):
            key = path.relative_to(base).as_posix()
            if not key.startswith(prefix) or key.endswith(".tmp"):
                continue
            stat = path.stat()
            yield ObjectInfo(
                bucket=bucket,
                key=key,
                size=stat.st_size,
                etag=_md5(path),
                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
            )

    def get_bytes(self, bucket: str, key: str) -> bytes:
        path = self._path(bucket, key)
        if not path.is_file():
            raise ObjectNotFoundError(f"{bucket}/{key}")
        return path.read_bytes()

    def put_bytes(self, bucket: str, key: str, data: bytes) -> None:
        path = self._path(bucket, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(path)  # atomic on POSIX: readers never see a partial object

    def copy(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        src = self._path(src_bucket, src_key)
        if not src.is_file():
            raise ObjectNotFoundError(f"{src_bucket}/{src_key}")
        dst = self._path(dst_bucket, dst_key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        keys = [o.key for o in self.list_objects(bucket, prefix)]
        for key in keys:
            self._path(bucket, key).unlink()
        return len(keys)

    def exists(self, bucket: str, key: str) -> bool:
        return self._path(bucket, key).is_file()


def _md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()
