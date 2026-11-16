"""Bronze -> silver: normalise a delivery, gate it on quality, publish its partitions.

Silver is partitioned by ``(source, billing period)`` and each partition is a single
Parquet object that is *replaced* by the newest delivery covering that period.
Providers restate a month many times until it closes (late usage, credits,
refunds), so appending would double count; replacing makes reprocessing idempotent.
An older delivery processed late (out-of-order) never overwrites a newer one.
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from datetime import UTC, datetime

import polars as pl

from costlake.config import PipelineConfig
from costlake.ledger import Delivery, Ledger, SilverPartition
from costlake.logs import get_logger
from costlake.normalization.adapters import ContractViolationError, adapter_for
from costlake.normalization.readers import (
    UnreadableFileError,
    UnsupportedFileError,
    read_export_file,
)
from costlake.normalization.schema import SILVER_SCHEMA
from costlake.quality.checks import CheckResult, run_checks
from costlake.storage import ObjectStore

log = get_logger(__name__)

SILVER_PREFIX = "silver/focus/"


def silver_key(source_id: str, billing_period: str) -> str:
    return f"{SILVER_PREFIX}source={source_id}/period={billing_period}/data.parquet"


@dataclass
class DeliveryOutcome:
    delivery_id: str
    status: str
    rows: int = 0
    billed_cost: float = 0.0
    periods_written: list[str] = field(default_factory=list)
    periods_skipped: list[str] = field(default_factory=list)
    checks: list[CheckResult] = field(default_factory=list)
    error: str | None = None


def process_delivery(
    store: ObjectStore,
    ledger: Ledger,
    lake_bucket: str,
    config: PipelineConfig,
    delivery: Delivery,
    now: datetime | None = None,
) -> DeliveryOutcome:
    now = now or datetime.now(UTC)
    bound = log.bind(delivery_id=delivery.delivery_id, source=delivery.source_id)
    source = config.source(delivery.source_id)

    try:
        frames = [
            read_export_file(f.bronze_key, store.get_bytes(lake_bucket, f.bronze_key))
            for f in ledger.delivery_files_of(delivery.delivery_id)
        ]
        raw = pl.concat([f for f in frames if f.width], how="diagonal_relaxed")
        result = adapter_for(source.format).normalize(raw)
    except (ContractViolationError, UnreadableFileError, UnsupportedFileError, ValueError) as exc:
        message = f"{type(exc).__name__}: {exc}"
        ledger.quarantine(delivery.delivery_id, message)
        bound.warning("delivery.quarantined", reason=message)
        return DeliveryOutcome(delivery.delivery_id, "quarantined", error=message)

    checks = run_checks(result, config.quality)
    ledger.save_quality_results(delivery.delivery_id, checks)
    failed = [c for c in checks if c.blocking]
    for check in checks:
        if not check.passed:
            bound.warning(
                "quality.check_failed",
                check=check.name,
                severity=check.severity.value,
                observed=check.observed,
                detail=check.message,
            )
    if failed:
        message = "; ".join(f"{c.name}: {c.message}" for c in failed)
        ledger.quarantine(delivery.delivery_id, message)
        bound.warning("delivery.quarantined", reason=message)
        return DeliveryOutcome(delivery.delivery_id, "quarantined", checks=checks, error=message)

    silver = result.frame.with_columns(
        pl.lit(delivery.source_id).alias("x_source_id"),
        pl.lit(delivery.delivery_id).alias("x_delivery_id"),
        pl.col("BillingPeriodStart").dt.strftime("%Y-%m").alias("x_billing_period"),
        pl.lit(now).cast(SILVER_SCHEMA["x_ingested_at"]).alias("x_ingested_at"),
    ).select(list(SILVER_SCHEMA))

    outcome = DeliveryOutcome(
        delivery.delivery_id,
        "published",
        rows=silver.height,
        billed_cost=float(silver.get_column("BilledCost").sum()),
        checks=checks,
    )
    partitions: list[SilverPartition] = []
    for (period_key,), part in sorted(
        silver.group_by(["x_billing_period"]), key=lambda kv: str(kv[0])
    ):
        period = str(period_key)
        current = ledger.current_partition(delivery.source_id, period)
        if current and current.exported_at > delivery.exported_at:
            outcome.periods_skipped.append(period)
            bound.info("silver.partition_kept_newer", period=period, owner=current.delivery_id)
            continue
        key = silver_key(delivery.source_id, period)
        store.put_bytes(lake_bucket, key, _to_parquet(part))
        partitions.append(
            SilverPartition(
                source_id=delivery.source_id,
                billing_period=period,
                delivery_id=delivery.delivery_id,
                object_key=key,
                row_count=part.height,
                billed_cost_total=float(part.get_column("BilledCost").sum()),
                exported_at=delivery.exported_at,
            )
        )
        outcome.periods_written.append(period)

    ledger.publish(
        delivery.delivery_id,
        partitions,
        row_count=silver.height,
        billed_cost_total=outcome.billed_cost,
        billing_periods=sorted(outcome.periods_written + outcome.periods_skipped),
    )
    if not partitions:
        outcome.status = "superseded"
    bound.info(
        "delivery.published",
        rows=outcome.rows,
        billed_cost=round(outcome.billed_cost, 2),
        periods=outcome.periods_written,
        skipped=outcome.periods_skipped,
    )
    return outcome


def _to_parquet(frame: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.sort(["ChargePeriodStart", "ServiceName"]).write_parquet(
        buffer, compression="zstd", statistics=True
    )
    return buffer.getvalue()
