from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from costlake.storage import LocalObjectStore, ObjectNotFoundError, S3ObjectStore


def test_local_roundtrip_listing_and_delete(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    store.put_bytes("b", "a/1.csv", b"x")
    store.put_bytes("b", "a/sub/2.csv", b"yy")
    store.put_bytes("b", "c/3.csv", b"z")
    objects = list(store.list_objects("b", "a/"))
    assert [o.key for o in objects] == ["a/1.csv", "a/sub/2.csv"]
    assert objects[1].parent == "a/sub/"
    assert store.get_bytes("b", "a/sub/2.csv") == b"yy"
    store.copy("b", "c/3.csv", "other", "copied.csv")
    assert store.exists("other", "copied.csv")
    assert store.delete_prefix("b", "a/") == 2
    assert not store.exists("b", "a/1.csv")
    assert list(store.list_objects("missing-bucket")) == []


def test_local_rejects_path_traversal(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    with pytest.raises(ValueError, match="escapes"):
        store.put_bytes("b", "../outside", b"x")


def test_local_missing_key(tmp_path: Path) -> None:
    with pytest.raises(ObjectNotFoundError):
        LocalObjectStore(tmp_path).get_bytes("b", "nope")


@mock_aws
def test_s3_store_against_moto() -> None:
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="landing")
    client.create_bucket(Bucket="lake")
    store = S3ObjectStore(None, "us-east-1", "test", "test", client=client)
    for i in range(1205):  # more than one listing page and one delete batch
        store.put_bytes("landing", f"p/{i:05d}.csv", b"data")
    objects = list(store.list_objects("landing", "p/"))
    assert len(objects) == 1205
    assert objects[0].etag and objects[0].size == 4
    store.copy("landing", "p/00001.csv", "lake", "bronze/x.csv")
    assert store.get_bytes("lake", "bronze/x.csv") == b"data"
    assert store.exists("lake", "bronze/x.csv")
    assert not store.exists("lake", "nope")
    with pytest.raises(ObjectNotFoundError):
        store.get_bytes("lake", "nope")
    assert store.delete_prefix("landing", "p/") == 1205
    assert list(store.list_objects("landing")) == []
