from __future__ import annotations

import gzip
from datetime import UTC, datetime

import polars as pl
import pytest

from costlake.normalization.readers import (
    CastReport,
    UnreadableFileError,
    UnsupportedFileError,
    cast_column,
    is_data_file,
    read_export_file,
)
from costlake.normalization.schema import TS


def test_timestamp_formats_are_normalised_to_utc() -> None:
    frame = pl.DataFrame(
        {
            "t": [
                "2026-09-01T00:00:00Z",
                "2026-09-01T02:00:00+02:00",
                "2026-09-01 00:00:00",
                "2026-09-01",
                "",
                None,
            ]
        }
    )
    report = CastReport()
    out = cast_column(frame, "t", TS, report)
    expected = datetime(2026, 9, 1, tzinfo=UTC)
    assert out.to_list()[:4] == [expected] * 4
    assert out.null_count() == 2
    assert report.total == 0


def test_unparseable_values_are_counted_not_silently_dropped() -> None:
    frame = pl.DataFrame(
        {"c": ["1.5", "abc", "", "2e3"], "t": ["2026-09-01", "yesterday", None, None]}
    )
    report = CastReport()
    numbers = cast_column(frame, "c", pl.Float64(), report)
    cast_column(frame, "t", TS, report)
    assert numbers.to_list() == [1.5, None, None, 2000.0]
    assert report.failures == {"c": 1, "t": 1}


def test_map_typed_tags_become_json() -> None:
    frame = pl.DataFrame({"Tags": [[{"key": "team", "value": "data"}], None]})
    out = cast_column(frame, "Tags", pl.Utf8(), CastReport())
    assert out.to_list() == ['{"team":"data"}', None]


def test_read_csv_gz_reads_everything_as_text() -> None:
    data = gzip.compress(b"A,B\n001,2.50\n")
    frame = read_export_file("x/part.csv.gz", data)
    assert frame.schema == {"A": pl.Utf8, "B": pl.Utf8}
    assert frame.row(0) == ("001", "2.50")


def test_unreadable_and_unsupported_files() -> None:
    with pytest.raises(UnreadableFileError):
        read_export_file("x.parquet", b"not parquet")
    with pytest.raises(UnsupportedFileError):
        read_export_file("x.json", b"{}")
    assert read_export_file("empty.csv", b"").height == 0


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("a.csv", True),
        ("a.CSV.GZ", True),
        ("a.snappy.parquet", True),
        ("manifest.json", False),
        ("_SUCCESS", False),
    ],
)
def test_is_data_file(key: str, expected: bool) -> None:
    assert is_data_file(key) is expected
