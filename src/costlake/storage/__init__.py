from costlake.config import Settings, StorageBackend
from costlake.storage.base import ObjectInfo, ObjectNotFoundError, ObjectStore
from costlake.storage.local import LocalObjectStore
from costlake.storage.s3 import S3ObjectStore

__all__ = [
    "LocalObjectStore",
    "ObjectInfo",
    "ObjectNotFoundError",
    "ObjectStore",
    "S3ObjectStore",
    "build_object_store",
]


def build_object_store(settings: Settings) -> ObjectStore:
    if settings.storage_backend is StorageBackend.S3:
        return S3ObjectStore(
            endpoint_url=settings.s3_endpoint_url,
            region=settings.s3_region,
            access_key_id=(
                settings.s3_access_key_id.get_secret_value() if settings.s3_access_key_id else None
            ),
            secret_access_key=(
                settings.s3_secret_access_key.get_secret_value()
                if settings.s3_secret_access_key
                else None
            ),
        )
    return LocalObjectStore(settings.local_root)
