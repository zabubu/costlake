"""Read raw export files and coerce columns to the silver contract.

CSV files are read with every column as text: billing exports routinely contain
values that break type inference (empty strings, scientific notation, IDs that look
numeric). Columns are then cast explicitly, and every value that fails to cast is
counted so the quality gate can reject the delivery instead of silently nulling it.
"""

from __future__ import annotations

import gzip
import io
import json
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from costlake.normalization.schema import TS


class UnsupportedFileError(ValueError):
    pass


class UnreadableFileError(ValueError):
    pass


DATA_SUFFIXES = (".csv", ".csv.gz", ".parquet", ".snappy.parquet", ".gz.parquet")


def is_data_file(key: str) -> bool:
    return key.lower().endswith(DATA_SUFFIXES)


def read_export_file(key: str, data: bytes) -> pl.DataFrame:
    lower = key.lower()
    try:
        if lower.endswith(".parquet"):
            return pl.read_parquet(io.BytesIO(data))
        if lower.endswith(".csv.gz"):
            data = gzip.decompress(data)
        if lower.endswith((".csv", ".csv.gz")):
            if not data.strip():
                return pl.DataFrame()
            return pl.read_csv(io.BytesIO(data), infer_schema=False, encoding="utf8-lossy")
    except (pl.exceptions.PolarsError, OSError, EOFError) as exc:
        raise UnreadableFileError(f"{key}: {exc}") from exc
    raise UnsupportedFileError(f"unsupported file type: {key}")


@dataclass
class CastReport:
    """Number of non-null input values that could not be converted, per column."""

    failures: dict[str, int] = field(default_factory=dict)

    def record(self, column: str, count: int) -> None:
        if count:
            self.failures[column] = self.failures.get(column, 0) + count

    @property
    def total(self) -> int:
        return sum(self.failures.values())


def _parse_timestamp(expr: pl.Expr) -> pl.Expr:
    text = expr.str.strip_chars().str.replace(r"Z$", "+00:00").str.replace(" ", "T", literal=True)
    with_tz = text.str.to_datetime("%Y-%m-%dT%H:%M:%S%.f%:z", strict=False, time_unit="us")
    no_tz = text.str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", strict=False, time_unit="us")
    date_only = text.str.to_datetime("%Y-%m-%d", strict=False, time_unit="us")
    return pl.coalesce(
        with_tz.dt.convert_time_zone("UTC"),
        no_tz.dt.replace_time_zone("UTC"),
        date_only.dt.replace_time_zone("UTC"),
    )


def _tags_to_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, pl.Series):
        value = value.to_list()
    if isinstance(value, list):  # Arrow map read by Polars: list of {key, value}
        value = {item["key"]: item["value"] for item in value}
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def cast_column(
    frame: pl.DataFrame, name: str, target: pl.DataType, report: CastReport
) -> pl.Series:
    """Return ``frame[name]`` converted to ``target``, recording conversion failures."""
    series = frame.get_column(name)
    source = series.dtype

    if target == TS:
        if source == pl.Utf8:
            series = series.str.strip_chars().replace("", None)
            converted = pl.select(_parse_timestamp(pl.lit(series))).to_series()
        elif isinstance(source, pl.Datetime):
            converted = (
                series.dt.convert_time_zone("UTC")
                if source.time_zone
                else series.dt.replace_time_zone("UTC")
            ).dt.cast_time_unit("us")
        elif source == pl.Date:
            converted = series.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        else:
            converted = pl.Series(name, [None] * len(series), dtype=target)
        report.record(name, _new_nulls(series, converted))
        return converted.alias(name)

    if target == pl.Float64:
        if source == pl.Utf8:
            text = series.str.strip_chars().replace("", None)
            converted = text.cast(pl.Float64, strict=False)
            report.record(name, _new_nulls(text, converted))
        else:
            converted = series.cast(pl.Float64, strict=False)
            report.record(name, _new_nulls(series, converted))
        return converted.alias(name)

    if name == "Tags" and source != pl.Utf8:
        return series.map_elements(_tags_to_json, return_dtype=pl.Utf8).alias(name)

    converted = series.cast(pl.Utf8, strict=False)
    if converted.dtype == pl.Utf8:
        converted = converted.str.strip_chars().replace("", None)
    return converted.alias(name)


def _new_nulls(before: pl.Series, after: pl.Series) -> int:
    return after.null_count() - before.null_count()
