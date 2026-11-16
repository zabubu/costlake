from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Engine, create_engine, delete, event, func, insert, select, update
from sqlalchemy.engine import Connection, RowMapping

from costlake.ledger.models import (
    DeliveryStatus,
    deliveries,
    delivery_files,
    dq_results,
    metadata,
    silver_partitions,
    step_runs,
)
from costlake.quality.checks import CheckResult


def utcnow() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime) -> datetime:
    # SQLite returns naive datetimes; everything stored is UTC.
    return value if value.tzinfo else value.replace(tzinfo=UTC)


@dataclass(frozen=True)
class DeliveryFile:
    landing_key: str
    etag: str
    size: int
    sha256: str
    bronze_key: str


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    source_id: str
    prefix: str
    status: DeliveryStatus
    file_count: int
    total_bytes: int
    exported_at: datetime
    error: str | None = None


@dataclass(frozen=True)
class SilverPartition:
    source_id: str
    billing_period: str
    delivery_id: str
    object_key: str
    row_count: int
    billed_cost_total: float
    exported_at: datetime


class Ledger:
    def __init__(self, url: str) -> None:
        self.engine: Engine = create_engine(url, future=True, pool_pre_ping=True)
        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine, "connect", _sqlite_pragmas)

    @contextmanager
    def _tx(self) -> Iterator[Connection]:
        with self.engine.begin() as conn:
            yield conn

    def init_schema(self) -> None:
        metadata.create_all(self.engine)

    # -- deliveries -----------------------------------------------------------------

    def delivery_exists(self, delivery_id: str) -> bool:
        with self._tx() as conn:
            row = conn.execute(
                select(deliveries.c.delivery_id).where(deliveries.c.delivery_id == delivery_id)
            ).first()
        return row is not None

    def register_delivery(self, delivery: Delivery, files: Sequence[DeliveryFile]) -> None:
        with self._tx() as conn:
            conn.execute(
                insert(deliveries).values(
                    delivery_id=delivery.delivery_id,
                    source_id=delivery.source_id,
                    prefix=delivery.prefix,
                    status=delivery.status.value,
                    file_count=delivery.file_count,
                    total_bytes=delivery.total_bytes,
                    exported_at=delivery.exported_at,
                    discovered_at=utcnow(),
                    error=delivery.error,
                )
            )
            if files:
                conn.execute(
                    insert(delivery_files),
                    [
                        {
                            "delivery_id": delivery.delivery_id,
                            "landing_key": f.landing_key,
                            "etag": f.etag,
                            "size": f.size,
                            "sha256": f.sha256,
                            "bronze_key": f.bronze_key,
                        }
                        for f in files
                    ],
                )

    def pending_deliveries(self) -> list[Delivery]:
        """Deliveries in bronze waiting for normalisation, oldest export first."""
        with self._tx() as conn:
            rows = conn.execute(
                select(deliveries)
                .where(deliveries.c.status == DeliveryStatus.BRONZE.value)
                .order_by(deliveries.c.exported_at, deliveries.c.delivery_id)
            ).mappings()
            return [_delivery(r) for r in rows]

    def delivery_files_of(self, delivery_id: str) -> list[DeliveryFile]:
        with self._tx() as conn:
            rows = conn.execute(
                select(delivery_files)
                .where(delivery_files.c.delivery_id == delivery_id)
                .order_by(delivery_files.c.landing_key)
            ).mappings()
            return [
                DeliveryFile(
                    landing_key=r["landing_key"],
                    etag=r["etag"],
                    size=r["size"],
                    sha256=r["sha256"],
                    bronze_key=r["bronze_key"],
                )
                for r in rows
            ]

    def get_delivery(self, delivery_id: str) -> Delivery:
        with self._tx() as conn:
            row = (
                conn.execute(select(deliveries).where(deliveries.c.delivery_id == delivery_id))
                .mappings()
                .one()
            )
        return _delivery(row)

    def quarantine(self, delivery_id: str, error: str) -> None:
        with self._tx() as conn:
            conn.execute(
                update(deliveries)
                .where(deliveries.c.delivery_id == delivery_id)
                .values(
                    status=DeliveryStatus.QUARANTINED.value,
                    error=error[:4000],
                    processed_at=utcnow(),
                )
            )

    def requeue(self, delivery_id: str) -> None:
        """Send a quarantined delivery back to normalisation (e.g. after an adapter fix)."""
        with self._tx() as conn:
            result = conn.execute(
                update(deliveries)
                .where(
                    deliveries.c.delivery_id == delivery_id,
                    deliveries.c.status == DeliveryStatus.QUARANTINED.value,
                )
                .values(status=DeliveryStatus.BRONZE.value, error=None, processed_at=None)
            )
            if result.rowcount != 1:
                raise LookupError(f"no quarantined delivery with id {delivery_id!r}")

    def list_deliveries(self, limit: int = 50) -> list[Delivery]:
        with self._tx() as conn:
            rows = conn.execute(
                select(deliveries).order_by(deliveries.c.discovered_at.desc()).limit(limit)
            ).mappings()
            return [_delivery(r) for r in rows]

    def save_quality_results(self, delivery_id: str, results: Iterable[CheckResult]) -> None:
        now = utcnow()
        rows = [
            {
                "delivery_id": delivery_id,
                "check_name": r.name,
                "severity": r.severity.value,
                "passed": r.passed,
                "observed": r.observed,
                "message": r.message,
                "checked_at": now,
            }
            for r in results
        ]
        if rows:
            with self._tx() as conn:
                conn.execute(insert(dq_results), rows)

    # -- silver partitions --------------------------------------------------------------

    def current_partition(self, source_id: str, billing_period: str) -> SilverPartition | None:
        with self._tx() as conn:
            row = (
                conn.execute(
                    select(silver_partitions, deliveries.c.exported_at)
                    .join(
                        deliveries,
                        deliveries.c.delivery_id == silver_partitions.c.delivery_id,
                    )
                    .where(
                        silver_partitions.c.source_id == source_id,
                        silver_partitions.c.billing_period == billing_period,
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            return None
        return SilverPartition(
            source_id=row["source_id"],
            billing_period=row["billing_period"],
            delivery_id=row["delivery_id"],
            object_key=row["object_key"],
            row_count=row["row_count"],
            billed_cost_total=row["billed_cost_total"],
            exported_at=_aware(row["exported_at"]),
        )

    def publish(
        self,
        delivery_id: str,
        partitions: Sequence[SilverPartition],
        row_count: int,
        billed_cost_total: float,
        billing_periods: Sequence[str],
    ) -> None:
        """Atomically point partitions at ``delivery_id`` and mark it published.

        Deliveries that no longer back any partition become ``superseded``.
        """
        now = utcnow()
        with self._tx() as conn:
            for p in partitions:
                conn.execute(
                    delete(silver_partitions).where(
                        silver_partitions.c.source_id == p.source_id,
                        silver_partitions.c.billing_period == p.billing_period,
                    )
                )
                conn.execute(
                    insert(silver_partitions).values(
                        source_id=p.source_id,
                        billing_period=p.billing_period,
                        delivery_id=delivery_id,
                        object_key=p.object_key,
                        row_count=p.row_count,
                        billed_cost_total=p.billed_cost_total,
                        written_at=now,
                    )
                )
            conn.execute(
                update(deliveries)
                .where(deliveries.c.delivery_id == delivery_id)
                .values(
                    status=(
                        DeliveryStatus.PUBLISHED.value
                        if partitions
                        else DeliveryStatus.SUPERSEDED.value
                    ),
                    processed_at=now,
                    row_count=row_count,
                    billed_cost_total=billed_cost_total,
                    billing_periods=",".join(billing_periods),
                    error=None,
                )
            )
            owners = select(silver_partitions.c.delivery_id).distinct().scalar_subquery()
            conn.execute(
                update(deliveries)
                .where(
                    deliveries.c.status == DeliveryStatus.PUBLISHED.value,
                    deliveries.c.delivery_id.not_in(owners),
                )
                .values(status=DeliveryStatus.SUPERSEDED.value)
            )

    def silver_object_keys(self) -> list[str]:
        with self._tx() as conn:
            return list(
                conn.execute(
                    select(silver_partitions.c.object_key).order_by(silver_partitions.c.object_key)
                ).scalars()
            )

    def silver_totals(self) -> tuple[int, float]:
        with self._tx() as conn:
            rows, total = conn.execute(
                select(
                    func.coalesce(func.sum(silver_partitions.c.row_count), 0),
                    func.coalesce(func.sum(silver_partitions.c.billed_cost_total), 0.0),
                )
            ).one()
        return int(rows), float(total)

    def delivery_status_counts(self) -> dict[str, int]:
        with self._tx() as conn:
            rows = conn.execute(
                select(deliveries.c.status, func.count()).group_by(deliveries.c.status)
            ).all()
        return {str(status): int(count) for status, count in rows}

    # -- step runs ------------------------------------------------------------------------

    def start_step(self, run_id: str, step: str) -> int:
        with self._tx() as conn:
            result = conn.execute(
                insert(step_runs).values(
                    run_id=run_id, step=step, status="running", started_at=utcnow()
                )
            )
            return int(result.inserted_primary_key[0])  # type: ignore[index]

    def finish_step(
        self, step_id: int, status: str, rows: int | None = None, error: str | None = None
    ) -> None:
        with self._tx() as conn:
            conn.execute(
                update(step_runs)
                .where(step_runs.c.id == step_id)
                .values(
                    status=status,
                    finished_at=utcnow(),
                    rows=rows,
                    error=error[:4000] if error else None,
                )
            )


def _delivery(r: RowMapping) -> Delivery:
    return Delivery(
        delivery_id=r["delivery_id"],
        source_id=r["source_id"],
        prefix=r["prefix"],
        status=DeliveryStatus(r["status"]),
        file_count=r["file_count"],
        total_bytes=r["total_bytes"],
        exported_at=_aware(r["exported_at"]),
        error=r["error"],
    )


def _sqlite_pragmas(dbapi_connection: object, _: object) -> None:
    cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.close()
