"""Prometheus exporter for the *state* of the lake and of the business metrics.

Collected on every scrape from the ledger and the serving tables in PostgreSQL:
freshness per source, delivery statuses, quality failures, recent anomalies,
month-to-date spend and tag coverage. Alerting rules in
``monitoring/prometheus/rules`` are written against these series.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from costlake.logs import get_logger

log = get_logger(__name__)

_LEDGER_QUERIES = {
    "deliveries": "select status, count(*) from deliveries group by status",
    "freshness": (
        "select source_id, max(exported_at) from deliveries "
        "where status in ('published', 'superseded') group by source_id"
    ),
    "dq": (
        "select check_name, severity, count(*) from dq_results "
        "where not passed and checked_at >= :since group by check_name, severity"
    ),
}

_SERVING_QUERIES = {
    "anomalies": (
        "select provider, service_name, severity, direction, count(*) "
        "from serving.cost_anomalies a "
        "where charge_date >= (select max(charge_date) from serving.agg_daily_service_cost) - 7 "
        "group by provider, service_name, severity, direction"
    ),
    "mtd": (
        "select provider, currency, mtd_cost, forecast_cost, coalesce(previous_period_cost, 0) "
        "from serving.mart_month_to_date"
    ),
    "coverage": (
        "select provider, coverage_ratio from serving.mart_tag_coverage_daily t "
        "where charge_date = (select max(charge_date) from serving.mart_tag_coverage_daily)"
    ),
    "last_charge": (
        "select provider, extract(epoch from max(charge_date))::bigint "
        "from serving.agg_daily_service_cost group by provider"
    ),
}


class CostlakeCollector(Collector):
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.errors = 0

    def _query(self, sql: str, **params: Any) -> list[tuple[Any, ...]]:
        with self.engine.connect() as conn:
            return [tuple(r) for r in conn.execute(text(sql), params).all()]

    def collect(self) -> Iterator[Any]:
        up = GaugeMetricFamily("costlake_exporter_database_up", "1 if the database answered")
        try:
            yield from self._ledger_metrics()
            up.add_metric([], 1)
        except SQLAlchemyError as exc:
            self.errors += 1
            up.add_metric([], 0)
            log.warning("exporter.ledger_query_failed", error=str(exc).splitlines()[0])
        try:
            yield from self._serving_metrics()
        except SQLAlchemyError as exc:
            # Serving tables appear after the first successful pipeline run.
            log.info("exporter.serving_unavailable", error=str(exc).splitlines()[0])
        yield up
        errors = CounterMetricFamily("costlake_exporter_query_errors", "Failed ledger queries")
        errors.add_metric([], self.errors)
        yield errors

    def _ledger_metrics(self) -> Iterable[Any]:
        deliveries = GaugeMetricFamily(
            "costlake_deliveries", "Deliveries by processing status", labels=["status"]
        )
        for status, count in self._query(_LEDGER_QUERIES["deliveries"]):
            deliveries.add_metric([status], count)
        freshness = GaugeMetricFamily(
            "costlake_source_last_export_timestamp_seconds",
            "Export time of the newest delivery published to silver, per source",
            labels=["source"],
        )
        for source, exported_at in self._query(_LEDGER_QUERIES["freshness"]):
            if exported_at is not None:
                freshness.add_metric([source], exported_at.timestamp())
        dq = GaugeMetricFamily(
            "costlake_quality_check_failures_24h",
            "Failed data-quality checks over the last 24 hours",
            labels=["check", "severity"],
        )
        since = _hours_ago(24)
        for check, severity, count in self._query(_LEDGER_QUERIES["dq"], since=since):
            dq.add_metric([check, severity], count)
        return [deliveries, freshness, dq]

    def _serving_metrics(self) -> Iterable[Any]:
        anomalies = GaugeMetricFamily(
            "costlake_cost_anomalies_recent",
            "Cost anomalies detected over the last 7 days of data",
            labels=["provider", "service", "severity", "direction"],
        )
        for provider, service, severity, direction, count in self._query(
            _SERVING_QUERIES["anomalies"]
        ):
            anomalies.add_metric([provider, service, severity, direction], count)
        labels = ["provider", "currency"]
        mtd = GaugeMetricFamily("costlake_mtd_cost", "Month-to-date effective cost", labels=labels)
        forecast = GaugeMetricFamily(
            "costlake_forecast_cost", "Run-rate forecast for the current period", labels=labels
        )
        previous = GaugeMetricFamily(
            "costlake_previous_period_cost", "Effective cost of the previous period", labels=labels
        )
        for provider, currency, mtd_cost, forecast_cost, previous_cost in self._query(
            _SERVING_QUERIES["mtd"]
        ):
            mtd.add_metric([provider, currency], float(mtd_cost))
            forecast.add_metric([provider, currency], float(forecast_cost))
            previous.add_metric([provider, currency], float(previous_cost))
        coverage = GaugeMetricFamily(
            "costlake_tag_coverage_ratio",
            "Share of usage cost carrying the allocation tag on the latest day",
            labels=["provider"],
        )
        for provider, ratio in self._query(_SERVING_QUERIES["coverage"]):
            if ratio is not None:
                coverage.add_metric([provider], float(ratio))
        last_charge = GaugeMetricFamily(
            "costlake_last_charge_date_timestamp_seconds",
            "Latest charge date present in the gold layer",
            labels=["provider"],
        )
        for provider, epoch in self._query(_SERVING_QUERIES["last_charge"]):
            last_charge.add_metric([provider], float(epoch))
        return [anomalies, mtd, forecast, previous, coverage, last_charge]


def _hours_ago(hours: int) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours)


def serve(database_url: str, port: int) -> None:
    engine = create_engine(database_url, pool_pre_ping=True, pool_size=2)
    REGISTRY.register(CostlakeCollector(engine))
    start_http_server(port)
    log.info("exporter.listening", port=port)
    while True:
        time.sleep(3600)
