from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import polars as pl

from costlake.normalization.readers import CastReport


class ContractViolationError(ValueError):
    """The input cannot be mapped to the silver contract at all (e.g. missing columns)."""


@dataclass
class AdapterResult:
    frame: pl.DataFrame
    """Rows with exactly the FOCUS columns of the silver contract."""
    raw_row_count: int
    raw_billed_total: float
    """Sum of the provider's billed-cost column before mapping, for reconciliation."""
    cast_report: CastReport


class Adapter(Protocol):
    def normalize(self, frame: pl.DataFrame) -> AdapterResult: ...
