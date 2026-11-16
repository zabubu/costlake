"""Ledger on PostgreSQL, serving publication and the Prometheus exporter.

Needs a disposable PostgreSQL: ``COSTLAKE_TEST_POSTGRES_URL=postgresql+psycopg://...``
(CI starts one as a service container). The test owns the database: it drops the
tables it creates.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest
from prometheus_client import CollectorRegistry, generate_latest
from sqlalchemy import create_engine

from costlake.config import Settings
from costlake.generator.synthetic import (
    DEFAULT_PROVIDERS,
    default_anomalies,
    generate_focus,
    write_month_to_date_deliveries,
)
from costlake.ledger.models import metadata
from costlake.observability.exporter import CostlakeCollector
from costlake.pipeline import Runtime, run_all
from costlake.serving.publisher import PUBLISHED_TABLES, to_psycopg_url

URL = os.environ.get("COSTLAKE_TEST_POSTGRES_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.skipif(not URL, reason="COSTLAKE_TEST_POSTGRES_URL is not set"),
]


@pytest.fixture
def pg_runtime(settings: Settings) -> Runtime:
    assert URL
    engine = create_engine(URL)
    metadata.drop_all(engine)
    with psycopg.connect(to_psycopg_url(URL), autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS serving CASCADE")
    engine.dispose()
    return Runtime.from_settings(
        settings.model_copy(update={"database_url": URL, "serving_database_url": URL})
    )


def test_publish_to_serving_and_export_metrics(pg_runtime: Runtime) -> None:
    end = date(2026, 11, 16)
    export_time = datetime(2026, 11, 16, 6, tzinfo=UTC)
    for prefix, provider in (
        ("aws/focus", DEFAULT_PROVIDERS[0]),
        ("gcp/focus", DEFAULT_PROVIDERS[2]),
    ):
        frame = generate_focus(
            provider, end - timedelta(days=90), end, anomalies=default_anomalies(end)
        )
        write_month_to_date_deliveries(
            pg_runtime.store,
            pg_runtime.settings.landing_bucket,
            prefix,
            frame,
            "parquet",
            export_time,
        )

    summary = run_all(pg_runtime)
    assert {t.name for t in summary.published} == set(PUBLISHED_TABLES)

    assert URL
    with psycopg.connect(to_psycopg_url(URL)) as conn:
        anomalies = conn.execute(
            "select provider, service_name from serving.cost_anomalies order by charge_date"
        ).fetchall()
        types = dict(
            conn.execute(
                "select column_name, data_type from information_schema.columns "
                "where table_schema = 'serving' and table_name = 'fct_daily_cost'"
            ).fetchall()
        )
    assert anomalies == [
        ("AWS", "Amazon Elastic Compute Cloud"),
        ("Google Cloud", "BigQuery"),
    ]
    assert types["charge_date"] == "date"
    assert types["effective_cost"] == "double precision"

    # Republishing swaps tables atomically and stays consistent.
    assert run_all(pg_runtime).landed == []

    registry = CollectorRegistry()
    registry.register(CostlakeCollector(create_engine(URL)))
    text = generate_latest(registry).decode()
    assert "costlake_exporter_database_up 1.0" in text
    assert 'costlake_deliveries{status="published"} 8.0' in text  # 2 sources x 4 periods
    # Both incidents are older than the 7-day "recent" window of the exporter.
    assert "costlake_cost_anomalies_recent{" not in text
    assert 'costlake_tag_coverage_ratio{provider="AWS"}' in text
    assert 'costlake_mtd_cost{currency="USD",provider="AWS"}' in text
    assert 'costlake_source_last_export_timestamp_seconds{source="aws-focus"}' in text


def test_exporter_reports_database_down() -> None:
    registry = CollectorRegistry()
    registry.register(CostlakeCollector(create_engine("postgresql+psycopg://x:y@127.0.0.1:1/none")))
    text = generate_latest(registry).decode()
    assert "costlake_exporter_database_up 0.0" in text
    assert "costlake_exporter_query_errors_total 1.0" in text
