"""Batch-job telemetry pushed to the Prometheus Pushgateway.

Pipeline steps are short-lived processes that Prometheus cannot scrape, so each step
pushes its duration, row count and last success/failure timestamps. A push failure is
logged and never fails the pipeline: telemetry must not take production down.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from costlake.logs import get_logger

log = get_logger(__name__)

JOB = "costlake_pipeline"


@dataclass
class StepTelemetry:
    rows: int = 0


def push_step_metrics(
    gateway: str | None, step: str, success: bool, duration: float, rows: int
) -> None:
    if not gateway:
        return
    registry = CollectorRegistry()
    labels = ["step"]
    Gauge(
        "costlake_step_duration_seconds",
        "Duration of the last run of a step",
        labels,
        registry=registry,
    ).labels(step).set(duration)
    Gauge(
        "costlake_step_rows",
        "Rows processed by the last run of a step",
        labels,
        registry=registry,
    ).labels(step).set(rows)
    Gauge(
        "costlake_step_last_run_success",
        "1 if the last run of the step succeeded",
        labels,
        registry=registry,
    ).labels(step).set(1 if success else 0)
    name = (
        "costlake_step_last_success_timestamp_seconds"
        if success
        else "costlake_step_last_failure_timestamp_seconds"
    )
    Gauge(name, "Unix time of the last run with this outcome", labels, registry=registry).labels(
        step
    ).set(time.time())
    try:
        # Grouping by step keeps the success timestamp of a step when another fails.
        push_to_gateway(gateway, job=JOB, grouping_key={"step": step}, registry=registry, timeout=5)
    except OSError as exc:
        log.warning("metrics.push_failed", gateway=gateway, step=step, error=str(exc))


@contextmanager
def timed_step(gateway: str | None, step: str) -> Iterator[StepTelemetry]:
    telemetry = StepTelemetry()
    start = time.perf_counter()
    try:
        yield telemetry
    except BaseException:
        push_step_metrics(gateway, step, False, time.perf_counter() - start, telemetry.rows)
        raise
    push_step_metrics(gateway, step, True, time.perf_counter() - start, telemetry.rows)
