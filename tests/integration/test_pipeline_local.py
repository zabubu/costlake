"""Full pipeline on the local filesystem + SQLite + DuckDB, with the real dbt project."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

import pytest

from costlake.generator.synthetic import (
    DEFAULT_PROVIDERS,
    LEGACY_AWS,
    default_anomalies,
    generate_focus,
    write_month_to_date_deliveries,
)
from costlake.ledger import DeliveryStatus
from costlake.pipeline import Runtime, run_all
from costlake.storage import LocalObjectStore
from costlake.transform.warehouse import connect_warehouse

pytestmark = pytest.mark.integration

END = date(2026, 11, 16)
SOURCES = (
    ("aws/focus", DEFAULT_PROVIDERS[0], "parquet", False),
    ("azure/focus", DEFAULT_PROVIDERS[1], "csv.gz", False),
    ("gcp/focus", DEFAULT_PROVIDERS[2], "csv.gz", False),
    ("aws/cur2", LEGACY_AWS, "csv.gz", True),
)


def _generate(
    rt: Runtime, start: date, end: date, export_time: datetime, anomalies: bool = True
) -> None:
    incidents = default_anomalies(END) if anomalies else []
    for prefix, provider, fmt, as_cur2 in SOURCES:
        frame = generate_focus(provider, start, end, anomalies=incidents)
        write_month_to_date_deliveries(
            rt.store, rt.settings.landing_bucket, prefix, frame, fmt, export_time, as_cur2=as_cur2
        )


def _gold(rt: Runtime, sql: str) -> list[tuple[object, ...]]:
    with connect_warehouse(rt.settings) as con:
        return con.execute(sql).fetchall()


def test_end_to_end_detects_exactly_the_injected_anomalies(runtime: Runtime) -> None:
    _generate(runtime, END - timedelta(days=120), END, datetime(2026, 11, 16, 6, tzinfo=UTC))
    summary = run_all(runtime)

    assert len(summary.landed) == 20  # 4 sources x 5 billing periods
    assert {o.status for o in summary.outcomes} == {"published"}
    assert summary.dbt_nodes > 20
    detected = {
        (str(d), p, s)
        for d, p, s in _gold(
            runtime, "select charge_date, provider, service_name from gold.cost_anomalies"
        )
    }
    injected = {(str(a.day), a.provider, a.service) for a in default_anomalies(END)}
    assert detected == injected

    silver_rows, silver_billed = runtime.ledger.silver_totals()
    (gold_billed,) = _gold(runtime, "select sum(billed_cost) from gold.fct_daily_cost")[0]
    assert float(gold_billed) == pytest.approx(silver_billed, abs=0.05)  # type: ignore[arg-type]

    # Idempotence: nothing new in landing -> nothing re-ingested, same totals.
    again = run_all(runtime)
    assert again.landed == [] and again.outcomes == []
    assert runtime.ledger.silver_totals() == (silver_rows, silver_billed)


def test_restated_period_replaces_previous_delivery(runtime: Runtime) -> None:
    start = date(2026, 10, 1)
    _generate(
        runtime, start, date(2026, 11, 10), datetime(2026, 11, 10, tzinfo=UTC), anomalies=False
    )
    run_all(runtime)
    first_owner = runtime.ledger.current_partition("aws-focus", "2026-11")
    assert first_owner is not None

    _generate(runtime, start, END, datetime(2026, 11, 16, tzinfo=UTC), anomalies=False)
    summary = run_all(runtime)
    assert len(summary.landed) == 8  # every period of every source was re-exported
    owner = runtime.ledger.current_partition("aws-focus", "2026-11")
    assert owner is not None and owner.delivery_id != first_owner.delivery_id
    assert runtime.ledger.get_delivery(first_owner.delivery_id).status is DeliveryStatus.SUPERSEDED

    expected = generate_focus(DEFAULT_PROVIDERS[0], start, END)
    (aws_billed, last_day) = _gold(
        runtime,
        "select sum(billed_cost), max(charge_date) from gold.fct_daily_cost "
        "where billing_account_id = '111111111111'",
    )[0]
    assert float(aws_billed) == pytest.approx(float(expected["BilledCost"].sum()), abs=0.05)  # type: ignore[arg-type]
    assert str(last_day) == "2026-11-15"


def test_late_older_export_does_not_overwrite_newer_data(runtime: Runtime) -> None:
    store = runtime.store
    assert isinstance(store, LocalObjectStore)
    newer = generate_focus(DEFAULT_PROVIDERS[0], date(2026, 11, 1), date(2026, 11, 15))
    older = generate_focus(DEFAULT_PROVIDERS[0], date(2026, 11, 1), date(2026, 11, 5))
    landing = runtime.settings.landing_bucket
    write_month_to_date_deliveries(
        store, landing, "aws/focus", newer, "parquet", datetime(2026, 11, 15, tzinfo=UTC)
    )
    run_all(runtime)
    keys = write_month_to_date_deliveries(
        store, landing, "aws/focus", older, "parquet", datetime(2026, 11, 5, tzinfo=UTC)
    )
    for key in keys:  # the late file carries its original export time
        ts = datetime(2026, 11, 5, tzinfo=UTC).timestamp()
        os.utime(store.root / landing / key, (ts, ts))

    summary = run_all(runtime)
    (late,) = summary.outcomes
    assert late.status == "superseded" and late.periods_skipped == ["2026-11"]
    _, billed = runtime.ledger.silver_totals()
    assert billed == pytest.approx(float(newer["BilledCost"].sum()), abs=0.01)


def test_corrupt_delivery_is_quarantined_and_does_not_block_others(runtime: Runtime) -> None:
    _generate(runtime, date(2026, 10, 1), END, datetime(2026, 11, 16, tzinfo=UTC), anomalies=False)
    runtime.store.put_bytes(
        runtime.settings.landing_bucket, "azure/focus/2026-11/broken/part-1.parquet", b"garbage"
    )
    runtime.store.put_bytes(
        runtime.settings.landing_bucket,
        "gcp/focus/2026-11/no-cost/part-1.csv",
        b"ProviderName,ServiceName\nGoogle Cloud,BigQuery\n",
    )
    summary = run_all(runtime)
    quarantined = {o.delivery_id: o.error for o in summary.outcomes if o.status == "quarantined"}
    assert len(quarantined) == 2
    errors = " ".join(e or "" for e in quarantined.values())
    assert "UnreadableFileError" in errors and "missing required FOCUS columns" in errors
    assert summary.dbt_nodes > 0 and runtime.ledger.delivery_status_counts()["published"] == 8
