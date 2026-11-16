"""Detect new billing deliveries in the landing bucket and land them in bronze.

A *delivery* is the set of data files present under one folder of a source prefix.
This matches how providers write exports:

* AWS Data Exports (overwrite mode) rewrite every part of
  ``.../BILLING_PERIOD=2026-09/`` on each refresh;
* Azure Cost Management writes each run to a new ``<date-range>/<run-id>/`` folder;
* the costlake generator writes ``<source>/<period>/<export-timestamp>/``.

The delivery id hashes the folder and the ETags of its files, so re-scanning an
unchanged landing zone is a no-op and a rewritten export is a new delivery.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from costlake.config import SourceConfig
from costlake.ledger import Delivery, DeliveryFile, DeliveryStatus, Ledger
from costlake.logs import get_logger
from costlake.normalization.readers import is_data_file
from costlake.storage import ObjectInfo, ObjectStore

log = get_logger(__name__)


@dataclass(frozen=True)
class DeliveryCandidate:
    source_id: str
    prefix: str
    objects: tuple[ObjectInfo, ...]

    @property
    def delivery_id(self) -> str:
        digest = hashlib.sha256(f"{self.source_id}\n{self.prefix}\n".encode())
        for obj in self.objects:
            digest.update(f"{obj.key}\t{obj.etag}\n".encode())
        return digest.hexdigest()[:24]

    @property
    def exported_at(self) -> datetime:
        return max(o.last_modified for o in self.objects)


def discover(
    store: ObjectStore,
    landing_bucket: str,
    source: SourceConfig,
    settle_seconds: int,
    now: datetime | None = None,
) -> list[DeliveryCandidate]:
    now = now or datetime.now(UTC)
    groups: dict[str, list[ObjectInfo]] = defaultdict(list)
    for obj in store.list_objects(landing_bucket, source.prefix):
        if is_data_file(obj.key) and obj.size > 0:
            groups[obj.parent].append(obj)

    candidates = []
    for prefix, objects in sorted(groups.items()):
        candidate = DeliveryCandidate(
            source.id, prefix, tuple(sorted(objects, key=lambda o: o.key))
        )
        if now - candidate.exported_at < timedelta(seconds=settle_seconds):
            log.info("delivery.not_settled", source=source.id, prefix=prefix)
            continue
        candidates.append(candidate)
    return candidates


def land_in_bronze(
    store: ObjectStore,
    ledger: Ledger,
    landing_bucket: str,
    lake_bucket: str,
    candidate: DeliveryCandidate,
) -> Delivery | None:
    """Copy a delivery to ``bronze/`` and register it. Returns None if already known."""
    delivery_id = candidate.delivery_id
    if ledger.delivery_exists(delivery_id):
        return None

    files: list[DeliveryFile] = []
    base = f"bronze/source={candidate.source_id}/delivery={delivery_id}/"
    for obj in candidate.objects:
        data = store.get_bytes(landing_bucket, obj.key)
        sha256 = hashlib.sha256(data).hexdigest()
        relative = obj.key[len(candidate.prefix) :]
        bronze_key = base + relative
        # Bronze keeps the exact bytes received (audit, replay); never rewritten.
        store.put_bytes(lake_bucket, bronze_key, data)
        files.append(
            DeliveryFile(
                landing_key=obj.key,
                etag=obj.etag,
                size=obj.size,
                sha256=sha256,
                bronze_key=bronze_key,
            )
        )

    delivery = Delivery(
        delivery_id=delivery_id,
        source_id=candidate.source_id,
        prefix=candidate.prefix,
        status=DeliveryStatus.BRONZE,
        file_count=len(files),
        total_bytes=sum(f.size for f in files),
        exported_at=candidate.exported_at,
    )
    ledger.register_delivery(delivery, files)
    log.info(
        "delivery.landed",
        delivery_id=delivery_id,
        source=candidate.source_id,
        prefix=candidate.prefix,
        files=len(files),
        bytes=delivery.total_bytes,
    )
    return delivery
