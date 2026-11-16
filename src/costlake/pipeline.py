"""The pipeline steps, independent of any orchestrator.

Dagster assets and the ``costlake pipeline run`` command both call these functions;
keeping the logic here makes every step testable without a scheduler and keeps the
orchestration layer thin.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

import polars as pl
import structlog

from costlake.anomaly.detector import detect_anomalies
from costlake.config import PipelineConfig, Settings, load_pipeline_config
from costlake.ingestion.discovery import discover, land_in_bronze
from costlake.ledger import Delivery, Ledger
from costlake.logs import get_logger
from costlake.normalization.silver import DeliveryOutcome, process_delivery
from costlake.observability.metrics import timed_step
from costlake.serving.publisher import PublishedTable, publish_tables
from costlake.storage import ObjectStore, build_object_store
from costlake.transform.dbt import run_dbt
from costlake.transform.warehouse import connect_warehouse

log = get_logger(__name__)


@dataclass
class Runtime:
    settings: Settings
    config: PipelineConfig
    store: ObjectStore
    ledger: Ledger

    @classmethod
    def from_settings(cls, settings: Settings) -> Runtime:
        ledger = Ledger(settings.database_url)
        ledger.init_schema()
        return cls(settings, load_pipeline_config(settings), build_object_store(settings), ledger)


@dataclass
class RunSummary:
    run_id: str
    landed: list[Delivery] = field(default_factory=list)
    outcomes: list[DeliveryOutcome] = field(default_factory=list)
    dbt_nodes: int = 0
    anomalies: int = 0
    published: list[PublishedTable] = field(default_factory=list)


@contextmanager
def _step(rt: Runtime, run_id: str, name: str) -> Iterator[Callable[[int], None]]:
    """Record a step in the ledger, push its telemetry and bind log context."""
    rows = 0

    def set_rows(n: int) -> None:
        nonlocal rows
        rows = n

    step_id = rt.ledger.start_step(run_id, name)
    with (
        structlog.contextvars.bound_contextvars(run_id=run_id, step=name),
        timed_step(rt.settings.pushgateway_url, name) as telemetry,
    ):
        log.info("step.started")
        try:
            yield set_rows
        except Exception as exc:
            telemetry.rows = rows
            rt.ledger.finish_step(step_id, "failed", rows, f"{type(exc).__name__}: {exc}")
            log.exception("step.failed")
            raise
        telemetry.rows = rows
        rt.ledger.finish_step(step_id, "succeeded", rows)
        log.info("step.succeeded", rows=rows)


def ingest(rt: Runtime, run_id: str) -> list[Delivery]:
    with _step(rt, run_id, "ingest") as set_rows:
        landed: list[Delivery] = []
        for source in rt.config.sources:
            for candidate in discover(
                rt.store,
                rt.settings.landing_bucket,
                source,
                rt.config.ingestion.settle_seconds,
            ):
                delivery = land_in_bronze(
                    rt.store,
                    rt.ledger,
                    rt.settings.landing_bucket,
                    rt.settings.lake_bucket,
                    candidate,
                )
                if delivery is not None:
                    landed.append(delivery)
        set_rows(len(landed))
        return landed


def normalize(rt: Runtime, run_id: str) -> list[DeliveryOutcome]:
    with _step(rt, run_id, "normalize") as set_rows:
        outcomes = [
            process_delivery(rt.store, rt.ledger, rt.settings.lake_bucket, rt.config, delivery)
            for delivery in rt.ledger.pending_deliveries()
        ]
        set_rows(sum(o.rows for o in outcomes if o.status != "quarantined"))
        return outcomes


def has_silver_data(rt: Runtime) -> bool:
    return bool(rt.ledger.silver_object_keys())


def transform(rt: Runtime, run_id: str) -> int:
    with _step(rt, run_id, "transform") as set_rows:
        nodes = run_dbt(rt.settings, ["build"])
        set_rows(len(nodes))
        return len(nodes)


def detect(rt: Runtime, run_id: str) -> pl.DataFrame:
    with _step(rt, run_id, "detect") as set_rows, connect_warehouse(rt.settings) as conn:
        daily = pl.from_arrow(
            conn.sql(
                "select charge_date, provider, service_name, currency, effective_cost "
                "from gold.agg_daily_service_cost"
            ).to_arrow_table()
        )
        if not isinstance(daily, pl.DataFrame):  # pragma: no cover - from_arrow on a table
            raise TypeError("expected a DataFrame")
        anomalies = detect_anomalies(daily, rt.config.anomaly)
        conn.register("detected_anomalies", anomalies.to_arrow())
        conn.execute(
            "create or replace table gold.cost_anomalies as select * from detected_anomalies"
        )
        conn.unregister("detected_anomalies")
        set_rows(anomalies.height)
        log.info("anomalies.detected", count=anomalies.height)
        return anomalies


def publish(rt: Runtime, run_id: str) -> list[PublishedTable]:
    if not rt.settings.serving_database_url:
        log.info("serving.skipped", reason="COSTLAKE_SERVING_DATABASE_URL is not set")
        return []
    with (
        _step(rt, run_id, "publish") as set_rows,
        connect_warehouse(rt.settings) as conn,
    ):
        published = publish_tables(conn, rt.settings.serving_database_url)
        set_rows(sum(t.rows for t in published))
        return published


def run_all(rt: Runtime, run_id: str | None = None) -> RunSummary:
    """Run every step in order. Downstream steps run only if silver holds data."""
    summary = RunSummary(run_id=run_id or uuid.uuid4().hex[:12])
    summary.landed = ingest(rt, summary.run_id)
    summary.outcomes = normalize(rt, summary.run_id)
    if not has_silver_data(rt):
        log.warning("pipeline.no_silver_data", run_id=summary.run_id)
        return summary
    summary.dbt_nodes = transform(rt, summary.run_id)
    summary.anomalies = detect(rt, summary.run_id).height
    summary.published = publish(rt, summary.run_id)
    return summary
