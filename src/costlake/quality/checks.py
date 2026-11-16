"""Data-quality rules applied to every delivery before it can reach the silver layer.

``ERROR`` rules are a gate: one failure quarantines the whole delivery, because a
partial billing period would understate cost. ``WARNING`` rules are recorded and
exported as metrics but do not block publication.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

import polars as pl

from costlake.config import QualityConfig
from costlake.normalization.adapters.base import AdapterResult
from costlake.normalization.schema import CHARGE_CATEGORIES, SERVICE_CATEGORIES


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class CheckResult:
    name: str
    severity: Severity
    passed: bool
    observed: float | None
    message: str

    @property
    def blocking(self) -> bool:
        return self.severity is Severity.ERROR and not self.passed


CheckFn = Callable[[AdapterResult, QualityConfig], CheckResult]

NOT_NULL_COLUMNS = (
    "BilledCost",
    "EffectiveCost",
    "BillingAccountId",
    "BillingCurrency",
    "BillingPeriodStart",
    "BillingPeriodEnd",
    "ChargeCategory",
    "ChargePeriodStart",
    "ChargePeriodEnd",
    "ProviderName",
    "ServiceName",
)


def _result(
    name: str, severity: Severity, passed: bool, observed: float | None, message: str
) -> CheckResult:
    return CheckResult(name, severity, passed, observed, message)


def check_not_empty(result: AdapterResult, _: QualityConfig) -> CheckResult:
    rows = result.frame.height
    return _result("not_empty", Severity.ERROR, rows > 0, rows, f"{rows} rows")


def check_type_casts(result: AdapterResult, _: QualityConfig) -> CheckResult:
    failures = result.cast_report.failures
    detail = ", ".join(f"{k}={v}" for k, v in sorted(failures.items())) or "none"
    return _result(
        "type_casts",
        Severity.ERROR,
        not failures,
        result.cast_report.total,
        f"values that could not be parsed: {detail}",
    )


def check_required_not_null(result: AdapterResult, _: QualityConfig) -> CheckResult:
    nulls = result.frame.select(pl.col(c).null_count() for c in NOT_NULL_COLUMNS).row(0)
    offenders = {c: n for c, n in zip(NOT_NULL_COLUMNS, nulls, strict=True) if n}
    detail = ", ".join(f"{k}={v}" for k, v in offenders.items()) or "none"
    return _result(
        "required_not_null",
        Severity.ERROR,
        not offenders,
        float(sum(offenders.values())),
        f"null values in mandatory columns: {detail}",
    )


def check_charge_period_order(result: AdapterResult, _: QualityConfig) -> CheckResult:
    bad = result.frame.filter(pl.col("ChargePeriodEnd") <= pl.col("ChargePeriodStart")).height
    return _result(
        "charge_period_order",
        Severity.ERROR,
        bad == 0,
        bad,
        f"{bad} rows with ChargePeriodEnd <= ChargePeriodStart",
    )


def check_charge_within_billing_period(result: AdapterResult, _: QualityConfig) -> CheckResult:
    bad = result.frame.filter(
        (pl.col("ChargePeriodStart") < pl.col("BillingPeriodStart"))
        | (pl.col("ChargePeriodStart") >= pl.col("BillingPeriodEnd"))
    ).height
    return _result(
        "charge_within_billing_period",
        Severity.ERROR,
        bad == 0,
        bad,
        f"{bad} rows charged outside their billing period",
    )


def check_charge_category_values(result: AdapterResult, _: QualityConfig) -> CheckResult:
    values = set(result.frame.get_column("ChargeCategory").drop_nulls().unique().to_list())
    unexpected = sorted(values - CHARGE_CATEGORIES)
    return _result(
        "charge_category_values",
        Severity.ERROR,
        not unexpected,
        len(unexpected),
        f"unexpected ChargeCategory values: {unexpected or 'none'}",
    )


def check_single_currency_per_account(result: AdapterResult, _: QualityConfig) -> CheckResult:
    mixed = (
        result.frame.group_by("BillingAccountId")
        .agg(pl.col("BillingCurrency").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
    )
    return _result(
        "single_currency_per_account",
        Severity.ERROR,
        mixed.height == 0,
        mixed.height,
        f"{mixed.height} billing accounts with several currencies",
    )


def check_reconciliation(result: AdapterResult, cfg: QualityConfig) -> CheckResult:
    silver_total = float(result.frame.get_column("BilledCost").fill_null(0).sum())
    delta = abs(silver_total - result.raw_billed_total)
    rows_ok = result.frame.height == result.raw_row_count
    passed = rows_ok and delta <= cfg.reconciliation_tolerance
    return _result(
        "billed_cost_reconciliation",
        Severity.ERROR,
        passed,
        delta,
        f"raw={result.raw_billed_total:.4f} silver={silver_total:.4f} "
        f"rows raw={result.raw_row_count} silver={result.frame.height}",
    )


def check_service_category_values(result: AdapterResult, _: QualityConfig) -> CheckResult:
    values = set(result.frame.get_column("ServiceCategory").drop_nulls().unique().to_list())
    unexpected = sorted(values - SERVICE_CATEGORIES)
    return _result(
        "service_category_values",
        Severity.WARNING,
        not unexpected,
        len(unexpected),
        f"non-standard ServiceCategory values: {unexpected or 'none'}",
    )


def check_effective_not_above_list(result: AdapterResult, _: QualityConfig) -> CheckResult:
    usage = result.frame.filter(
        (pl.col("ChargeCategory") == "Usage") & pl.col("ListCost").is_not_null()
    )
    bad = usage.filter(pl.col("EffectiveCost") > pl.col("ListCost") + 1e-6).height
    return _result(
        "effective_cost_not_above_list",
        Severity.WARNING,
        bad == 0,
        bad,
        f"{bad} usage rows where EffectiveCost exceeds ListCost",
    )


def check_allocation_tag_coverage(result: AdapterResult, cfg: QualityConfig) -> CheckResult:
    usage = result.frame.filter(pl.col("ChargeCategory") == "Usage")
    total = float(usage.get_column("EffectiveCost").fill_null(0).abs().sum())
    if total == 0:
        return _result(
            "allocation_tag_coverage", Severity.WARNING, True, None, "no usage cost to allocate"
        )
    tag_value = pl.col("Tags").str.json_path_match(f"$.{cfg.allocation_tag}")
    tagged = float(
        usage.filter(tag_value.is_not_null() & (tag_value != ""))
        .get_column("EffectiveCost")
        .fill_null(0)
        .abs()
        .sum()
    )
    coverage = tagged / total
    return _result(
        "allocation_tag_coverage",
        Severity.WARNING,
        coverage >= cfg.min_tag_coverage,
        round(coverage, 4),
        f"{coverage:.1%} of usage cost carries the '{cfg.allocation_tag}' tag "
        f"(target {cfg.min_tag_coverage:.0%})",
    )


def check_duplicate_rows(result: AdapterResult, _: QualityConfig) -> CheckResult:
    dupes = result.frame.height - result.frame.unique().height
    return _result(
        "duplicate_rows", Severity.WARNING, dupes == 0, dupes, f"{dupes} fully duplicated rows"
    )


ALL_CHECKS: tuple[CheckFn, ...] = (
    check_not_empty,
    check_type_casts,
    check_required_not_null,
    check_charge_period_order,
    check_charge_within_billing_period,
    check_charge_category_values,
    check_single_currency_per_account,
    check_reconciliation,
    check_service_category_values,
    check_effective_not_above_list,
    check_allocation_tag_coverage,
    check_duplicate_rows,
)


def run_checks(result: AdapterResult, cfg: QualityConfig) -> list[CheckResult]:
    return [check(result, cfg) for check in ALL_CHECKS]
