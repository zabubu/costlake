from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import polars as pl

from costlake.anomaly.detector import detect_anomalies, detect_series
from costlake.config import AnomalyConfig

CFG = AnomalyConfig()


def _series(
    n: int = 70, weekend_factor: float = 1.0, seed: int = 1
) -> tuple[list[date], np.ndarray]:
    rng = np.random.default_rng(seed)
    days = [date(2026, 8, 1) + timedelta(days=i) for i in range(n)]
    values = np.array(
        [
            200.0 * (weekend_factor if d.weekday() >= 5 else 1.0) * rng.lognormal(0, 0.05)
            for d in days
        ]
    )
    return days, values


def test_stable_series_has_no_anomaly() -> None:
    days, values = _series()
    assert detect_series(days, values, CFG) == []


def test_weekly_seasonality_is_not_flagged() -> None:
    days, values = _series(weekend_factor=0.3)
    assert detect_series(days, values, CFG) == []


def test_spike_and_drop_are_detected_once() -> None:
    days, values = _series()
    values[50] *= 4
    values[60] *= 0.1
    found = detect_series(days, values, CFG)
    assert [i for i, _, _ in found] == [50, 60]
    assert found[0][2] > 0 > found[1][2]


def test_past_incident_does_not_mask_the_next_one() -> None:
    days, values = _series()
    values[40] *= 5
    values[45] *= 5
    assert [i for i, _, _ in detect_series(days, values, CFG)] == [40, 45]


def test_not_enough_history() -> None:
    days, values = _series(n=10)
    values[9] *= 10
    assert detect_series(days, values, CFG) == []


def test_financially_irrelevant_blips_are_ignored() -> None:
    days, values = _series()
    values = values / 100  # ~2 per day: tripling it is significant but below min_abs_delta
    values[50] *= 3
    assert detect_series(days, values, CFG) == []


def test_detect_anomalies_frame() -> None:
    days, values = _series()
    values[55] *= 6
    frame = pl.DataFrame(
        {
            "charge_date": days * 2,
            "provider": ["AWS"] * len(days) + ["Microsoft"] * len(days),
            "service_name": ["EC2"] * len(days) + ["VM"] * len(days),
            "currency": ["USD"] * (2 * len(days)),
            "effective_cost": np.concatenate([values, _series(seed=7)[1]]),
        }
    )
    out = detect_anomalies(frame, CFG)
    assert out.height == 1
    row = out.row(0, named=True)
    assert (row["provider"], row["charge_date"], row["direction"], row["severity"]) == (
        "AWS",
        days[55],
        "spike",
        "critical",
    )
    assert detect_anomalies(frame.head(0), CFG).height == 0
