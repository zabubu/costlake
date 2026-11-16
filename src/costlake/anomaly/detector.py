"""Spend anomaly detection on daily cost series.

For every (provider, service) series and every day ``d`` with enough history:

1. take the trailing window of ``window_days`` *before* ``d`` (``d`` itself is never
   part of its own baseline);
2. estimate a robust level ``m`` (median) and a day-of-week profile
   ``f(dow) = median(values on that weekday) / m`` — workloads that sleep on
   weekends must not raise an alert every Saturday;
3. expected cost ``e = m * f(dow(d))``; residual scale ``s = 1.4826 * MAD`` of the
   baseline residuals (the MAD is the median absolute deviation; 1.4826 makes it a
   consistent estimator of the standard deviation for normal noise);
4. robust z-score ``z = (x - e) / s``.

A day is an anomaly when ``|z| >= z_threshold`` **and** the absolute and relative
deltas exceed their floors, which filters statistically significant but financially
irrelevant blips on tiny series. Median/MAD are used rather than mean/stddev because
a single past incident in the window would otherwise inflate the baseline and mask
the next one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

import numpy as np
import polars as pl

from costlake.config import AnomalyConfig

ANOMALY_SCHEMA: dict[str, pl.DataType] = {
    "charge_date": pl.Date(),
    "provider": pl.Utf8(),
    "service_name": pl.Utf8(),
    "currency": pl.Utf8(),
    "actual_cost": pl.Float64(),
    "expected_cost": pl.Float64(),
    "delta": pl.Float64(),
    "z_score": pl.Float64(),
    "direction": pl.Utf8(),
    "severity": pl.Utf8(),
    "detected_at": pl.Datetime("us", "UTC"),
}

_MAD_TO_SIGMA = 1.4826


@dataclass(frozen=True)
class Expectation:
    expected: float
    scale: float


def expectation(
    history: np.ndarray, history_dow: np.ndarray, target_dow: int, rel_floor: float = 0.02
) -> Expectation:
    level = float(np.median(history))
    factors = np.ones(7)
    if level > 0:
        for dow in range(7):
            values = history[history_dow == dow]
            if values.size >= 2:
                factors[dow] = float(np.median(values)) / level
    residuals = history - level * factors[history_dow]
    mad = float(np.median(np.abs(residuals - np.median(residuals))))
    # Floor the scale so a perfectly flat series does not turn a cent into z = inf.
    scale = max(_MAD_TO_SIGMA * mad, rel_floor * abs(level), 1e-6)
    return Expectation(expected=level * float(factors[target_dow]), scale=scale)


def detect_series(
    days: list[date], values: np.ndarray, cfg: AnomalyConfig
) -> list[tuple[int, Expectation, float]]:
    """Return (index, expectation, z) for every anomalous point of one series."""
    dows = np.array([d.weekday() for d in days])
    found: list[tuple[int, Expectation, float]] = []
    for i in range(len(values)):
        start = max(0, i - cfg.window_days)
        if i - start < cfg.min_history_days:
            continue
        exp = expectation(values[start:i], dows[start:i], int(dows[i]))
        delta = float(values[i]) - exp.expected
        z = delta / exp.scale
        relative = abs(delta) / max(abs(exp.expected), 1e-9)
        if (
            abs(z) >= cfg.z_threshold
            and abs(delta) >= cfg.min_abs_delta
            and relative >= cfg.min_relative_delta
        ):
            found.append((i, exp, z))
    return found


def detect_anomalies(
    daily: pl.DataFrame, cfg: AnomalyConfig, now: datetime | None = None
) -> pl.DataFrame:
    """``daily`` needs charge_date, provider, service_name, currency, effective_cost."""
    now = now or datetime.now(UTC)
    rows: list[dict[str, object]] = []
    ordered = daily.sort(["provider", "service_name", "charge_date"])
    for (provider, service), series in ordered.group_by(
        ["provider", "service_name"], maintain_order=True
    ):
        days = series.get_column("charge_date").to_list()
        values = series.get_column("effective_cost").cast(pl.Float64).fill_null(0).to_numpy()
        currency = series.get_column("currency").drop_nulls().first()
        for index, exp, z in detect_series(days, values, cfg):
            actual = float(values[index])
            delta = actual - exp.expected
            critical = abs(z) >= 2 * cfg.z_threshold and abs(delta) >= 4 * cfg.min_abs_delta
            rows.append(
                {
                    "charge_date": days[index],
                    "provider": provider,
                    "service_name": service,
                    "currency": currency,
                    "actual_cost": round(actual, 2),
                    "expected_cost": round(exp.expected, 2),
                    "delta": round(delta, 2),
                    "z_score": round(z, 2),
                    "direction": "spike" if delta > 0 else "drop",
                    "severity": "critical" if critical else "warning",
                    "detected_at": now,
                }
            )
    return pl.DataFrame(rows, schema=ANOMALY_SCHEMA, orient="row")
