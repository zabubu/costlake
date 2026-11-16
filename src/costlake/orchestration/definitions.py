"""Dagster code location: assets, checks, sensor, schedule.

No ``from __future__ import annotations`` here: Dagster inspects the real type hints.

Every asset delegates to :mod:`costlake.pipeline`; this module only adds what an
orchestrator is for: lineage, retries, scheduling, event-driven triggering, run
history and a UI.
"""

import hashlib
import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    Backoff,
    ConfigurableResource,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    RetryPolicy,
    RunRequest,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    SkipReason,
    asset,
    define_asset_job,
    schedule,
    sensor,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from costlake import pipeline
from costlake.config import get_settings
from costlake.ingestion.discovery import discover
from costlake.logs import configure_logging
from costlake.transform.warehouse import dbt_environment

settings = get_settings()
configure_logging(settings.log_level, settings.log_format)

IO_RETRY = RetryPolicy(max_retries=3, delay=15, backoff=Backoff.EXPONENTIAL)

dbt_project = DbtProject(
    project_dir=Path(settings.dbt_project_dir),
    target_path=Path(
        os.environ.get("COSTLAKE_DBT_MANIFEST_DIR", settings.dbt_project_dir / "target")
    ),
)
dbt_project.prepare_if_dev()


class CostlakeResource(ConfigurableResource):  # type: ignore[type-arg]
    """Builds the pipeline runtime from the environment (COSTLAKE_* variables)."""

    def runtime(self) -> pipeline.Runtime:
        return pipeline.Runtime.from_settings(get_settings())


def _dbt_executable() -> str:
    """dbt installed in the same environment as this code, whatever PATH says."""
    candidate = Path(sys.executable).parent / "dbt"
    return str(candidate) if candidate.exists() else "dbt"


def _run_id(context: AssetExecutionContext) -> str:
    return context.run.run_id[:12]


@asset(group_name="bronze", retry_policy=IO_RETRY, compute_kind="python")
def bronze_deliveries(
    context: AssetExecutionContext, costlake: CostlakeResource
) -> MaterializeResult:  # type: ignore[type-arg]
    """New export folders copied from the landing bucket to bronze and registered."""
    landed = pipeline.ingest(costlake.runtime(), _run_id(context))
    return MaterializeResult(
        metadata={
            "new_deliveries": len(landed),
            "bytes": sum(d.total_bytes for d in landed),
            "sources": sorted({d.source_id for d in landed}),
        }
    )


@asset(
    group_name="silver",
    deps=[bronze_deliveries],
    retry_policy=IO_RETRY,
    compute_kind="polars",
    check_specs=[
        AssetCheckSpec("no_quarantined_delivery", asset="silver_focus"),
        AssetCheckSpec("quality_warnings", asset="silver_focus"),
    ],
)
def silver_focus(context: AssetExecutionContext, costlake: CostlakeResource) -> MaterializeResult:  # type: ignore[type-arg]
    """FOCUS 1.0 charges, one Parquet object per (source, billing period)."""
    rt = costlake.runtime()
    outcomes = pipeline.normalize(rt, _run_id(context))
    quarantined = [o for o in outcomes if o.status == "quarantined"]
    warnings = sorted(
        {c.name for o in outcomes for c in o.checks if not c.passed and not c.blocking}
    )
    rows, billed = rt.ledger.silver_totals()
    return MaterializeResult(
        metadata={
            "deliveries_processed": len(outcomes),
            "partitions_written": sum(len(o.periods_written) for o in outcomes),
            "silver_rows": rows,
            "silver_billed_cost": round(billed, 2),
        },
        check_results=[
            AssetCheckResult(
                check_name="no_quarantined_delivery",
                passed=not quarantined,
                severity=AssetCheckSeverity.ERROR,
                metadata={"quarantined": [f"{o.delivery_id}: {o.error}" for o in quarantined][:20]},
            ),
            AssetCheckResult(
                check_name="quality_warnings",
                passed=not warnings,
                severity=AssetCheckSeverity.WARN,
                metadata={"failed_warning_checks": warnings},
            ),
        ],
    )


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def gold_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator[Any]:
    """Gold layer: dbt models and tests over silver (tests become asset checks)."""
    current = get_settings()
    current.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ.update(dbt_environment(current))
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    group_name="gold",
    deps=[AssetKey(["gold", "agg_daily_service_cost"])],
    compute_kind="numpy",
)
def cost_anomalies(context: AssetExecutionContext, costlake: CostlakeResource) -> MaterializeResult:  # type: ignore[type-arg]
    """Robust z-score anomalies on daily cost per provider and service."""
    anomalies = pipeline.detect(costlake.runtime(), _run_id(context))
    recent = anomalies.sort("charge_date", descending=True).head(10)
    return MaterializeResult(
        metadata={
            "anomalies": anomalies.height,
            "critical": anomalies.filter(anomalies["severity"] == "critical").height,
            "latest": [
                f"{r['charge_date']} {r['provider']} / {r['service_name']} "
                f"{r['direction']} z={r['z_score']}"
                for r in recent.iter_rows(named=True)
            ],
        }
    )


@asset(
    group_name="serving",
    deps=[
        cost_anomalies,
        AssetKey(["gold", "fct_daily_cost"]),
        AssetKey(["gold", "mart_owner_monthly_cost"]),
        AssetKey(["gold", "mart_tag_coverage_daily"]),
        AssetKey(["gold", "mart_commitment_savings_monthly"]),
        AssetKey(["gold", "mart_month_to_date"]),
    ],
    retry_policy=IO_RETRY,
    compute_kind="postgres",
)
def serving_tables(context: AssetExecutionContext, costlake: CostlakeResource) -> MaterializeResult:  # type: ignore[type-arg]
    """Gold tables copied to PostgreSQL for Grafana and the metrics exporter."""
    published = pipeline.publish(costlake.runtime(), _run_id(context))
    return MaterializeResult(metadata={t.name: t.rows for t in published})


costlake_pipeline = define_asset_job(
    "costlake_pipeline",
    selection=AssetSelection.all(),
    description="Landing -> bronze -> silver -> gold -> anomalies -> serving.",
)


@sensor(
    job=costlake_pipeline,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
)
def landing_sensor(context: SensorEvaluationContext) -> Iterator[RunRequest | SkipReason]:
    """Start the pipeline when a new, settled export folder appears in the landing bucket."""
    rt = pipeline.Runtime.from_settings(get_settings())
    new_ids = sorted(
        candidate.delivery_id
        for source in rt.config.sources
        for candidate in discover(
            rt.store, rt.settings.landing_bucket, source, rt.config.ingestion.settle_seconds
        )
        if not rt.ledger.delivery_exists(candidate.delivery_id)
    )
    if not new_ids:
        yield SkipReason("no new delivery in the landing bucket")
        return
    run_key = hashlib.sha256("\n".join(new_ids).encode()).hexdigest()[:16]
    context.log.info(f"{len(new_ids)} new deliveries, run_key={run_key}")
    yield RunRequest(run_key=run_key, tags={"costlake/new_deliveries": str(len(new_ids))})


@schedule(
    job=costlake_pipeline,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_refresh(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    """Daily rebuild: picks up requeued deliveries and refreshes anomaly detection."""
    rt = pipeline.Runtime.from_settings(get_settings())
    if not pipeline.has_silver_data(rt):
        return SkipReason("silver layer is empty")
    stamp = (context.scheduled_execution_time or datetime.now(UTC)).strftime("%Y-%m-%d")
    return RunRequest(run_key=f"daily-{stamp}")


defs = Definitions(
    assets=[bronze_deliveries, silver_focus, gold_dbt_models, cost_anomalies, serving_tables],
    jobs=[costlake_pipeline],
    sensors=[landing_sensor],
    schedules=[daily_refresh],
    resources={
        "costlake": CostlakeResource(),
        "dbt": DbtCliResource(
            project_dir=dbt_project,
            profiles_dir=str(settings.dbt_project_dir),
            dbt_executable=_dbt_executable(),
        ),
    },
)
