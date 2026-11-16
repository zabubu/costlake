from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from costlake.generator.synthetic import (
    DEFAULT_PROVIDERS,
    default_anomalies,
    generate_focus,
    render,
    write_month_to_date_deliveries,
)
from costlake.observability.metrics import push_step_metrics, timed_step
from costlake.serving.publisher import postgres_type, to_psycopg_url
from costlake.storage import LocalObjectStore
from costlake.transform.warehouse import dbt_environment, silver_glob


def test_generator_is_deterministic_and_injects_anomalies() -> None:
    end = date(2026, 11, 1)
    a = generate_focus(DEFAULT_PROVIDERS[2], date(2026, 9, 1), end, seed=3)
    b = generate_focus(DEFAULT_PROVIDERS[2], date(2026, 9, 1), end, seed=3)
    assert a.equals(b)
    incident = next(x for x in default_anomalies(end) if x.provider == "Google Cloud")
    spiked = generate_focus(
        DEFAULT_PROVIDERS[2], date(2026, 9, 1), end, seed=3, anomalies=[incident]
    )

    def day_cost(frame: pl.DataFrame) -> float:
        return float(
            frame.filter(
                (pl.col("ServiceName") == incident.service)
                & (pl.col("ChargePeriodStart").dt.date() == incident.day)
            )["EffectiveCost"].sum()
        )

    assert day_cost(spiked) == pytest.approx(day_cost(a) * incident.multiplier, rel=1e-3)


def test_month_to_date_deliveries_layout(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from datetime import UTC, datetime

    store = LocalObjectStore(tmp_path)
    frame = generate_focus(DEFAULT_PROVIDERS[0], date(2026, 9, 25), date(2026, 10, 5))
    keys = write_month_to_date_deliveries(
        store,
        "landing",
        "aws/focus",
        frame,
        "parquet",
        datetime(2026, 10, 5, tzinfo=UTC),
        rows_per_file=100,
    )
    assert keys[0].startswith("aws/focus/2026-09/20261005T000000Z/part-00001")
    assert {k.split("/")[2] for k in keys} == {"2026-09", "2026-10"}
    with pytest.raises(ValueError, match="unknown"):
        render(frame, "xlsx")


def test_postgres_type_mapping() -> None:
    assert postgres_type("DECIMAL(18,2)") == "NUMERIC(18,2)"
    assert postgres_type("TIMESTAMP WITH TIME ZONE") == "TIMESTAMPTZ"
    assert postgres_type("VARCHAR") == "TEXT"
    assert postgres_type("STRUCT(a INT)") == "TEXT"
    assert to_psycopg_url("postgresql+psycopg://u:p@h/db") == "postgresql://u:p@h/db"


def test_dbt_environment_for_minio(settings) -> None:  # type: ignore[no-untyped-def]
    from pydantic import SecretStr

    from costlake.config import StorageBackend

    s3 = settings.model_copy(
        update={
            "storage_backend": StorageBackend.S3,
            "s3_endpoint_url": "http://minio:9000",
            "s3_access_key_id": SecretStr("k"),
            "s3_secret_access_key": SecretStr("s"),
        }
    )
    env = dbt_environment(s3)
    assert env["COSTLAKE_DBT_TARGET"] == "s3"
    assert env["COSTLAKE_DUCKDB_S3_ENDPOINT"] == "minio:9000"
    assert env["COSTLAKE_DUCKDB_S3_USE_SSL"] == "false"
    assert silver_glob(s3) == "s3://costlake-lake/silver/focus/**/*.parquet"
    assert dbt_environment(settings)["COSTLAKE_DBT_TARGET"] == "local"
    aws = s3.model_copy(
        update={"s3_endpoint_url": None, "s3_access_key_id": None, "s3_secret_access_key": None}
    )
    assert dbt_environment(aws)["COSTLAKE_DBT_TARGET"] == "aws"
    assert "COSTLAKE_S3_ACCESS_KEY_ID" not in dbt_environment(aws)


def test_metrics_push_never_breaks_the_pipeline() -> None:
    push_step_metrics(None, "ingest", True, 1.0, 1)  # disabled: no-op
    push_step_metrics("http://127.0.0.1:9", "ingest", True, 1.0, 1)  # unreachable: logged only
    with pytest.raises(RuntimeError), timed_step("http://127.0.0.1:9", "ingest"):
        raise RuntimeError("step failure still propagates")
