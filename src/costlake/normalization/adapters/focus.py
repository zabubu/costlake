"""Adapter for native FOCUS 1.0 exports (AWS Data Exports, Azure, Google Cloud)."""

from __future__ import annotations

import polars as pl

from costlake.normalization.adapters.base import AdapterResult, ContractViolationError
from costlake.normalization.readers import CastReport, cast_column
from costlake.normalization.schema import FOCUS_COLUMNS, REQUIRED_FOCUS_COLUMNS


class FocusAdapter:
    def normalize(self, frame: pl.DataFrame) -> AdapterResult:
        missing = sorted(REQUIRED_FOCUS_COLUMNS - set(frame.columns))
        if missing:
            raise ContractViolationError(f"missing required FOCUS columns: {', '.join(missing)}")

        report = CastReport()
        columns: list[pl.Series] = []
        for name, dtype in FOCUS_COLUMNS.items():
            if name in frame.columns:
                columns.append(cast_column(frame, name, dtype, report))
            else:
                columns.append(pl.Series(name, [None] * frame.height, dtype=dtype))
        normalized = pl.DataFrame(columns)

        raw_total = _raw_sum(frame.get_column("BilledCost"))
        return AdapterResult(
            frame=normalized,
            raw_row_count=frame.height,
            raw_billed_total=raw_total,
            cast_report=report,
        )


def _raw_sum(series: pl.Series) -> float:
    if series.dtype == pl.Utf8:
        series = series.str.strip_chars().cast(pl.Float64, strict=False)
    return float(series.cast(pl.Float64, strict=False).fill_null(0).sum())
