"""Object storage abstraction.

The pipeline only needs a handful of operations on buckets. Keeping them behind a
small protocol lets the same code run against MinIO / AWS S3 in containers and
against the local filesystem in unit tests and the no-Docker quickstart.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


class ObjectNotFoundError(KeyError):
    """Raised when a key does not exist in a bucket."""


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    bucket: str
    key: str
    size: int
    etag: str
    last_modified: datetime

    @property
    def parent(self) -> str:
        """Key prefix of the object, with a trailing slash ("" at bucket root)."""
        head, sep, _ = self.key.rpartition("/")
        return head + sep


class ObjectStore(Protocol):
    def list_objects(self, bucket: str, prefix: str = "") -> Iterator[ObjectInfo]: ...

    def get_bytes(self, bucket: str, key: str) -> bytes: ...

    def put_bytes(self, bucket: str, key: str, data: bytes) -> None: ...

    def copy(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None: ...

    def delete_prefix(self, bucket: str, prefix: str) -> int: ...

    def exists(self, bucket: str, key: str) -> bool: ...
