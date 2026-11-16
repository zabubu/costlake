from __future__ import annotations

import json
from datetime import date

import polars as pl
import pytest

from costlake.config import QualityConfig
from costlake.generator.synthetic import (
    DEFAULT_PROVIDERS,
    LEGACY_AWS,
    focus_to_cur2,
    generate_focus,
    render,
)
from costlake.normalization.adapters import ContractViolationError
from costlake.normalization.adapters.aws_cur2 import AwsCur2Adapter, normalize_tag_keys
from costlake.normalization.adapters.focus import FocusAdapter
from costlake.normalization.readers import read_export_file
from costlake.normalization.schema import FOCUS_COLUMNS
from costlake.quality.checks import Severity, run_checks

START, END = date(2026, 9, 1), date(2026, 9, 11)


def _focus_csv(provider_index: int = 0) -> pl.DataFrame:
    frame = generate_focus(DEFAULT_PROVIDERS[provider_index], START, END)
    data, suffix = render(frame, "csv.gz")
    return read_export_file("part" + suffix, data)


def _failed(
    frame: pl.DataFrame, adapter: FocusAdapter | AwsCur2Adapter | None = None
) -> dict[str, Severity]:
    result = (adapter or FocusAdapter()).normalize(frame)
    return {c.name: c.severity for c in run_checks(result, QualityConfig()) if not c.passed}


def test_clean_focus_export_passes_every_check() -> None:
    assert _failed(_focus_csv()) == {}


def test_focus_output_matches_contract() -> None:
    result = FocusAdapter().normalize(_focus_csv(1).drop("SkuPriceId", "AvailabilityZone"))
    assert result.frame.schema == pl.Schema(FOCUS_COLUMNS)
    assert result.frame.get_column("AvailabilityZone").null_count() == result.frame.height


def test_missing_required_column_is_a_contract_violation() -> None:
    with pytest.raises(ContractViolationError, match="BilledCost"):
        FocusAdapter().normalize(_focus_csv().drop("BilledCost"))


def _first_row(value: pl.Expr, column: str) -> pl.Expr:
    return pl.when(pl.int_range(pl.len()) == 0).then(value).otherwise(pl.col(column)).alias(column)


@pytest.mark.parametrize(
    ("mutation", "check"),
    [
        (_first_row(pl.lit("n/a"), "BilledCost"), "type_casts"),
        (_first_row(pl.lit(None, dtype=pl.Utf8), "ProviderName"), "required_not_null"),
        (pl.col("ChargePeriodStart").alias("ChargePeriodEnd"), "charge_period_order"),
        (pl.lit("2026-09-05T00:00:00Z").alias("BillingPeriodEnd"), "charge_within_billing_period"),
        (pl.lit("Refund").alias("ChargeCategory"), "charge_category_values"),
        (_first_row(pl.lit("EUR"), "BillingCurrency"), "single_currency_per_account"),
    ],
)
def test_blocking_rules(mutation: pl.Expr, check: str) -> None:
    assert _failed(_focus_csv().with_columns(mutation)).get(check) is Severity.ERROR


def test_warning_rules_do_not_block() -> None:
    frame = _focus_csv().with_columns(pl.lit(None, dtype=pl.Utf8).alias("Tags"))
    frame = pl.concat([frame, frame.head(1)])
    failed = _failed(frame)
    assert failed == {
        "allocation_tag_coverage": Severity.WARNING,
        "duplicate_rows": Severity.WARNING,
    }


def test_cur2_mapping_preserves_billed_total_and_amortises_commitments() -> None:
    focus = generate_focus(LEGACY_AWS, START, END)
    data, suffix = render(focus_to_cur2(focus), "csv.gz")
    result = AwsCur2Adapter().normalize(read_export_file("p" + suffix, data))
    mapped = result.frame
    assert result.raw_billed_total == pytest.approx(float(focus["BilledCost"].sum()), abs=1e-6)
    assert float(mapped["BilledCost"].sum()) == pytest.approx(result.raw_billed_total, abs=1e-6)
    # Effective cost after amortisation equals the FOCUS export of the same data.
    assert float(mapped["EffectiveCost"].sum()) == pytest.approx(
        float(focus["EffectiveCost"].sum()), abs=1e-3
    )
    assert set(mapped["ChargeCategory"].unique()) <= {"Usage", "Purchase"}
    covered = mapped.filter(pl.col("CommitmentDiscountType") == "Savings Plan")
    assert covered.height > 0 and set(covered["PricingCategory"]) == {"Committed"}
    assert set(mapped["ServiceCategory"].drop_nulls()) <= {"Compute", "Databases", "Storage"}
    tags = mapped["Tags"].drop_nulls()
    assert all(json.loads(t)["team"] == "erp" for t in tags)
    assert _failed(read_export_file("p" + suffix, data), AwsCur2Adapter()) == {}


def test_unknown_cur2_line_item_type_is_rejected() -> None:
    cur = focus_to_cur2(generate_focus(LEGACY_AWS, START, END)).with_columns(
        pl.lit("BrandNewLineType").alias("line_item_line_item_type")
    )
    assert _failed(cur, AwsCur2Adapter())["required_not_null"] is Severity.ERROR


def test_cur2_missing_columns() -> None:
    with pytest.raises(ContractViolationError, match="line_item_unblended_cost"):
        AwsCur2Adapter().normalize(pl.DataFrame({"bill_payer_account_id": ["1"]}))


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            '{"user_team": "a", "user:Env": "prod", "aws:createdBy": "x"}',
            '{"aws:createdby":"x","env":"prod","team":"a"}',
        ),
        ('{"user_team": ""}', None),
        ("not json", None),
        ("", None),
        (None, None),
    ],
)
def test_cur2_tag_normalisation(raw: str | None, expected: str | None) -> None:
    assert normalize_tag_keys(raw) == expected
