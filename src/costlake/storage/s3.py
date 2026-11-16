"""S3 / MinIO implementation of :class:`ObjectStore`.

Transient errors (throttling, 5xx, connection resets) are retried with exponential
backoff on top of botocore's own retry mode; a missing key is never retried.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.exceptions import ConnectionError as BotoConnectionError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from costlake.storage.base import ObjectInfo, ObjectNotFoundError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_RETRYABLE_CODES = {"SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable", "503"}


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, (EndpointConnectionError, BotoConnectionError)):
        return True
    if isinstance(exc, ClientError):
        return exc.response.get("Error", {}).get("Code") in _RETRYABLE_CODES
    return False


_retry = retry(
    retry=retry_if_exception(_is_transient),
    wait=wait_exponential(multiplier=0.5, max=8),
    stop=stop_after_attempt(5),
    reraise=True,
)


class S3ObjectStore:
    def __init__(
        self,
        endpoint_url: str | None,
        region: str,
        access_key_id: str | None,
        secret_access_key: str | None,
        client: S3Client | None = None,
    ) -> None:
        self._client: S3Client = client or boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=Config(
                retries={"max_attempts": 3, "mode": "standard"},
                s3={"addressing_style": "path"} if endpoint_url else {},
                connect_timeout=5,
                read_timeout=60,
            ),
        )

    @_retry
    def list_objects(self, bucket: str, prefix: str = "") -> Iterator[ObjectInfo]:
        paginator = self._client.get_paginator("list_objects_v2")
        objects: list[ObjectInfo] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                objects.append(
                    ObjectInfo(
                        bucket=bucket,
                        key=item["Key"],
                        size=item["Size"],
                        etag=item["ETag"].strip('"'),
                        last_modified=item["LastModified"],
                    )
                )
        # Materialised so the retry decorator covers the whole listing.
        return iter(sorted(objects, key=lambda o: o.key))

    @_retry
    def get_bytes(self, bucket: str, key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in {"NoSuchKey", "404"}:
                raise ObjectNotFoundError(f"{bucket}/{key}") from exc
            raise
        return response["Body"].read()

    @_retry
    def put_bytes(self, bucket: str, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=bucket, Key=key, Body=data)

    @_retry
    def copy(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        try:
            self._client.copy_object(
                Bucket=dst_bucket, Key=dst_key, CopySource={"Bucket": src_bucket, "Key": src_key}
            )
        except ClientError as exc:
            if _error_code(exc) in {"NoSuchKey", "404"}:
                raise ObjectNotFoundError(f"{src_bucket}/{src_key}") from exc
            raise

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        keys = [o.key for o in self.list_objects(bucket, prefix)]
        for start in range(0, len(keys), 1000):
            chunk = keys[start : start + 1000]
            self._delete_batch(bucket, chunk)
        return len(keys)

    @_retry
    def _delete_batch(self, bucket: str, keys: list[str]) -> None:
        response = self._client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys], "Quiet": True}
        )
        if errors := response.get("Errors"):
            raise RuntimeError(f"failed to delete {len(errors)} objects: {errors[:3]}")

    @_retry
    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in {"NoSuchKey", "404", "NotFound"}:
                return False
            raise
        return True


def _error_code(exc: ClientError) -> Any:
    return exc.response.get("Error", {}).get("Code")
