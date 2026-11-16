from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from costlake.config import SourceConfig
from costlake.ingestion.discovery import discover, land_in_bronze
from costlake.ledger import DeliveryStatus, Ledger, SilverPartition
from costlake.storage import LocalObjectStore

SOURCE = SourceConfig(id="aws-focus", format="focus", prefix="aws/focus")


@pytest.fixture
def store(tmp_path):  # type: ignore[no-untyped-def]
    s = LocalObjectStore(tmp_path / "store")
    s.put_bytes("landing", "aws/focus/2026-09/t1/part-1.csv", b"a")
    s.put_bytes("landing", "aws/focus/2026-09/t1/part-2.csv.gz", b"b")
    s.put_bytes("landing", "aws/focus/2026-09/t1/manifest.json", b"{}")
    s.put_bytes("landing", "aws/focus/2026-10/t1/part-1.parquet", b"c")
    s.put_bytes("landing", "aws/focus/2026-10/t1/empty.csv", b"")
    s.put_bytes("landing", "azure/focus/2026-10/t1/part-1.csv", b"d")
    return s


@pytest.fixture
def ledger(tmp_path) -> Ledger:  # type: ignore[no-untyped-def]
    led = Ledger(f"sqlite:///{tmp_path / 'l.sqlite'}")
    led.init_schema()
    return led


def test_discovery_groups_data_files_by_folder(store: LocalObjectStore) -> None:
    candidates = discover(store, "landing", SOURCE, settle_seconds=0)
    assert [(c.prefix, [o.key.rsplit("/", 1)[1] for o in c.objects]) for c in candidates] == [
        ("aws/focus/2026-09/t1/", ["part-1.csv", "part-2.csv.gz"]),
        ("aws/focus/2026-10/t1/", ["part-1.parquet"]),
    ]


def test_unsettled_folders_are_ignored(store: LocalObjectStore) -> None:
    now = datetime.now(UTC)
    assert discover(store, "landing", SOURCE, settle_seconds=3600, now=now) == []
    assert len(discover(store, "landing", SOURCE, 3600, now=now + timedelta(hours=2))) == 2


def test_delivery_id_is_stable_and_content_addressed(store: LocalObjectStore) -> None:
    first = [c.delivery_id for c in discover(store, "landing", SOURCE, 0)]
    assert first == [c.delivery_id for c in discover(store, "landing", SOURCE, 0)]
    store.put_bytes("landing", "aws/focus/2026-09/t1/part-1.csv", b"rewritten")
    second = [c.delivery_id for c in discover(store, "landing", SOURCE, 0)]
    assert second[0] != first[0] and second[1] == first[1]


def test_landing_is_idempotent_and_keeps_raw_bytes(store: LocalObjectStore, ledger: Ledger) -> None:
    candidate = discover(store, "landing", SOURCE, 0)[0]
    delivery = land_in_bronze(store, ledger, "landing", "lake", candidate)
    assert delivery is not None and delivery.status is DeliveryStatus.BRONZE
    assert land_in_bronze(store, ledger, "landing", "lake", candidate) is None
    files = ledger.delivery_files_of(delivery.delivery_id)
    assert [f.bronze_key for f in files] == [
        f"bronze/source=aws-focus/delivery={delivery.delivery_id}/part-1.csv",
        f"bronze/source=aws-focus/delivery={delivery.delivery_id}/part-2.csv.gz",
    ]
    assert store.get_bytes("lake", files[0].bronze_key) == b"a"
    assert [d.delivery_id for d in ledger.pending_deliveries()] == [delivery.delivery_id]


def test_publish_supersedes_previous_owner_and_requeue(
    store: LocalObjectStore, ledger: Ledger
) -> None:
    candidates = discover(store, "landing", SOURCE, 0)
    first = land_in_bronze(store, ledger, "landing", "lake", candidates[0])
    second = land_in_bronze(store, ledger, "landing", "lake", candidates[1])
    assert first and second

    def part(owner: str) -> SilverPartition:
        return SilverPartition("aws-focus", "2026-09", owner, "k", 10, 5.0, first.exported_at)

    ledger.publish(first.delivery_id, [part(first.delivery_id)], 10, 5.0, ["2026-09"])
    assert ledger.get_delivery(first.delivery_id).status is DeliveryStatus.PUBLISHED
    ledger.publish(second.delivery_id, [part(second.delivery_id)], 10, 5.0, ["2026-09"])
    assert ledger.get_delivery(first.delivery_id).status is DeliveryStatus.SUPERSEDED
    current = ledger.current_partition("aws-focus", "2026-09")
    assert current is not None and current.delivery_id == second.delivery_id
    assert ledger.silver_totals() == (10, 5.0)

    with pytest.raises(LookupError):
        ledger.requeue(second.delivery_id)  # not quarantined
    ledger.quarantine(second.delivery_id, "boom")
    ledger.requeue(second.delivery_id)
    assert ledger.get_delivery(second.delivery_id).status is DeliveryStatus.BRONZE


def test_step_tracking(ledger: Ledger) -> None:
    step = ledger.start_step("run-1", "ingest")
    ledger.finish_step(step, "failed", 3, "x" * 10_000)
    with ledger.engine.connect() as conn:
        row = conn.exec_driver_sql("select status, rows, length(error) from step_runs").one()
    assert tuple(row) == ("failed", 3, 4000)
