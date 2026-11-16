"""Adapter for legacy AWS Cost and Usage Report 2.0 exports.

Many organisations still deliver CUR 2.0 rather than the FOCUS export. This adapter
maps the CUR 2.0 columns onto FOCUS 1.0. The mapping is documented in
``docs/data-model.md``; the main rules are:

* ``BilledCost`` is ``line_item_unblended_cost`` on every line, so the billed total of
  a delivery is preserved exactly (checked by the reconciliation rule).
* ``EffectiveCost`` amortises commitments: covered usage takes the Savings Plan /
  Reserved Instance effective cost, negation lines and commitment fees take 0.
* Line item types unknown to the mapping produce a null ``ChargeCategory``, which the
  quality gate rejects, so a new AWS line item type cannot silently skew reports.
"""

from __future__ import annotations

import json

import polars as pl

from costlake.normalization.adapters.base import AdapterResult, ContractViolationError
from costlake.normalization.readers import CastReport, cast_column
from costlake.normalization.schema import FOCUS_COLUMNS, TS

REQUIRED = frozenset(
    {
        "bill_payer_account_id",
        "bill_billing_period_start_date",
        "bill_billing_period_end_date",
        "line_item_line_item_type",
        "line_item_usage_start_date",
        "line_item_usage_end_date",
        "line_item_product_code",
        "line_item_unblended_cost",
        "line_item_currency_code",
    }
)

_TIMESTAMPS = (
    "bill_billing_period_start_date",
    "bill_billing_period_end_date",
    "line_item_usage_start_date",
    "line_item_usage_end_date",
)
_NUMBERS = (
    "line_item_unblended_cost",
    "line_item_net_unblended_cost",
    "line_item_usage_amount",
    "line_item_unblended_rate",
    "pricing_public_on_demand_cost",
    "pricing_public_on_demand_rate",
    "savings_plan_savings_plan_effective_cost",
    "reservation_effective_cost",
)

CHARGE_CATEGORY = {
    "Usage": "Usage",
    "DiscountedUsage": "Usage",
    "SavingsPlanCoveredUsage": "Usage",
    "SavingsPlanNegation": "Usage",
    "Fee": "Purchase",
    "RIFee": "Purchase",
    "SavingsPlanRecurringFee": "Purchase",
    "SavingsPlanUpfrontFee": "Purchase",
    "Tax": "Tax",
    "Credit": "Credit",
    "Refund": "Credit",
    "Discount": "Adjustment",
    "BundledDiscount": "Adjustment",
    "EdpDiscount": "Adjustment",
    "PrivateRateDiscount": "Adjustment",
}

CHARGE_FREQUENCY = {
    "Usage": "Usage-Based",
    "DiscountedUsage": "Usage-Based",
    "SavingsPlanCoveredUsage": "Usage-Based",
    "SavingsPlanNegation": "Usage-Based",
    "RIFee": "Recurring",
    "SavingsPlanRecurringFee": "Recurring",
    "Tax": "Recurring",
}

PRICING_TERM = {"OnDemand": "Standard", "Spot": "Dynamic", "Reserved": "Committed"}

# product_code -> (ServiceName, ServiceCategory). Unknown codes fall back to (code, "Other").
SERVICES: dict[str, tuple[str, str]] = {
    "AmazonEC2": ("Amazon Elastic Compute Cloud", "Compute"),
    "AWSLambda": ("AWS Lambda", "Compute"),
    "AmazonECS": ("Amazon Elastic Container Service", "Compute"),
    "AmazonEKS": ("Amazon Elastic Kubernetes Service", "Compute"),
    "AmazonS3": ("Amazon Simple Storage Service", "Storage"),
    "AmazonEFS": ("Amazon Elastic File System", "Storage"),
    "AmazonRDS": ("Amazon Relational Database Service", "Databases"),
    "AmazonDynamoDB": ("Amazon DynamoDB", "Databases"),
    "AmazonElastiCache": ("Amazon ElastiCache", "Databases"),
    "AmazonRedshift": ("Amazon Redshift", "Analytics"),
    "AmazonAthena": ("Amazon Athena", "Analytics"),
    "AWSGlue": ("AWS Glue", "Analytics"),
    "AmazonKinesis": ("Amazon Kinesis", "Analytics"),
    "AmazonCloudFront": ("Amazon CloudFront", "Networking"),
    "AmazonVPC": ("Amazon Virtual Private Cloud", "Networking"),
    "AWSDataTransfer": ("AWS Data Transfer", "Networking"),
    "AmazonRoute53": ("Amazon Route 53", "Networking"),
    "AmazonCloudWatch": ("Amazon CloudWatch", "Management and Governance"),
    "AWSConfig": ("AWS Config", "Management and Governance"),
    "AWSKMS": ("AWS Key Management Service", "Security"),
    "AmazonGuardDuty": ("Amazon GuardDuty", "Security"),
    "AmazonSageMaker": ("Amazon SageMaker", "AI and Machine Learning"),
    "AmazonBedrock": ("Amazon Bedrock", "AI and Machine Learning"),
    "ComputeSavingsPlans": ("Savings Plans for AWS Compute usage", "Compute"),
}


def normalize_tag_keys(raw: str | None) -> str | None:
    """CUR 2.0 prefixes user tags (``user_team`` / ``user:team``); FOCUS uses bare keys."""
    if raw is None or not raw.strip():
        return None
    try:
        tags = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(tags, dict):
        return None
    cleaned: dict[str, str] = {}
    for key, value in tags.items():
        k = str(key)
        for prefix in ("user:", "user_"):
            if k.startswith(prefix):
                k = k[len(prefix) :]
                break
        if value not in (None, ""):
            cleaned[k.lower()] = str(value)
    return json.dumps(cleaned, sort_keys=True, separators=(",", ":")) if cleaned else None


class AwsCur2Adapter:
    def normalize(self, frame: pl.DataFrame) -> AdapterResult:
        missing = sorted(REQUIRED - set(frame.columns))
        if missing:
            raise ContractViolationError(f"missing required CUR 2.0 columns: {', '.join(missing)}")

        report = CastReport()
        typed = frame.with_columns(
            *[cast_column(frame, c, TS, report) for c in _TIMESTAMPS],
            *[cast_column(frame, c, pl.Float64(), report) for c in _NUMBERS if c in frame.columns],
        )
        for optional in (*_NUMBERS, *_OPTIONAL_TEXT):
            if optional not in typed.columns:
                dtype = pl.Float64 if optional in _NUMBERS else pl.Utf8
                typed = typed.with_columns(pl.lit(None, dtype=dtype).alias(optional))

        line_type = pl.col("line_item_line_item_type")
        unblended = pl.col("line_item_unblended_cost")
        net_or_unblended = pl.coalesce(pl.col("line_item_net_unblended_cost"), unblended)
        is_sp_covered = line_type == "SavingsPlanCoveredUsage"
        is_ri_covered = line_type == "DiscountedUsage"
        is_amortised_fee = line_type.is_in(
            ["SavingsPlanNegation", "SavingsPlanRecurringFee", "SavingsPlanUpfrontFee", "RIFee"]
        )
        product = pl.col("line_item_product_code")
        service_name = product.replace_strict(
            {k: v[0] for k, v in SERVICES.items()}, default=product, return_dtype=pl.Utf8
        )
        service_category = product.replace_strict(
            {k: v[1] for k, v in SERVICES.items()}, default="Other", return_dtype=pl.Utf8
        )
        commitment_id = (
            pl.when(is_sp_covered)
            .then(pl.col("savings_plan_savings_plan_a_r_n"))
            .when(is_ri_covered)
            .then(pl.col("reservation_reservation_a_r_n"))
        )

        mapped = typed.select(
            pl.col("line_item_availability_zone").alias("AvailabilityZone"),
            unblended.alias("BilledCost"),
            pl.col("bill_payer_account_id").alias("BillingAccountId"),
            pl.col("bill_payer_account_name").alias("BillingAccountName"),
            pl.col("line_item_currency_code").alias("BillingCurrency"),
            pl.col("bill_billing_period_end_date").alias("BillingPeriodEnd"),
            pl.col("bill_billing_period_start_date").alias("BillingPeriodStart"),
            line_type.replace_strict(CHARGE_CATEGORY, default=None, return_dtype=pl.Utf8).alias(
                "ChargeCategory"
            ),
            pl.lit(None, dtype=pl.Utf8).alias("ChargeClass"),
            pl.col("line_item_line_item_description").alias("ChargeDescription"),
            line_type.replace_strict(
                CHARGE_FREQUENCY, default="One-Time", return_dtype=pl.Utf8
            ).alias("ChargeFrequency"),
            pl.col("line_item_usage_end_date").alias("ChargePeriodEnd"),
            pl.col("line_item_usage_start_date").alias("ChargePeriodStart"),
            pl.when(is_sp_covered)
            .then(pl.lit("Spend"))
            .when(is_ri_covered)
            .then(pl.lit("Usage"))
            .alias("CommitmentDiscountCategory"),
            commitment_id.alias("CommitmentDiscountId"),
            pl.lit(None, dtype=pl.Utf8).alias("CommitmentDiscountName"),
            pl.when(is_sp_covered | is_ri_covered)
            .then(pl.lit("Used"))
            .alias("CommitmentDiscountStatus"),
            pl.when(is_sp_covered)
            .then(pl.lit("Savings Plan"))
            .when(is_ri_covered)
            .then(pl.lit("Reserved Instance"))
            .alias("CommitmentDiscountType"),
            pl.when(line_type.is_in(["Usage", "DiscountedUsage", "SavingsPlanCoveredUsage"]))
            .then(pl.col("line_item_usage_amount"))
            .alias("ConsumedQuantity"),
            pl.col("pricing_unit").alias("ConsumedUnit"),
            net_or_unblended.alias("ContractedCost"),
            pl.col("line_item_unblended_rate").alias("ContractedUnitPrice"),
            pl.when(is_sp_covered)
            .then(pl.col("savings_plan_savings_plan_effective_cost"))
            .when(is_ri_covered)
            .then(pl.col("reservation_effective_cost"))
            .when(is_amortised_fee)
            .then(pl.lit(0.0))
            .otherwise(net_or_unblended)
            .alias("EffectiveCost"),
            pl.col("bill_invoicing_entity").alias("InvoiceIssuerName"),
            pl.coalesce(pl.col("pricing_public_on_demand_cost"), unblended).alias("ListCost"),
            pl.col("pricing_public_on_demand_rate").alias("ListUnitPrice"),
            pl.when(is_sp_covered | is_ri_covered)
            .then(pl.lit("Committed"))
            .otherwise(
                pl.col("pricing_term").replace_strict(
                    PRICING_TERM, default=None, return_dtype=pl.Utf8
                )
            )
            .alias("PricingCategory"),
            pl.col("line_item_usage_amount").alias("PricingQuantity"),
            pl.col("pricing_unit").alias("PricingUnit"),
            pl.lit("AWS").alias("ProviderName"),
            pl.coalesce(pl.col("line_item_legal_entity"), pl.lit("AWS")).alias("PublisherName"),
            pl.col("product_region_code").alias("RegionId"),
            pl.col("product_region_code").alias("RegionName"),
            pl.col("line_item_resource_id").alias("ResourceId"),
            pl.col("line_item_resource_id").alias("ResourceName"),
            pl.col("line_item_usage_type").alias("ResourceType"),
            service_category.alias("ServiceCategory"),
            service_name.alias("ServiceName"),
            pl.col("product_sku").alias("SkuId"),
            pl.lit(None, dtype=pl.Utf8).alias("SkuPriceId"),
            pl.col("line_item_usage_account_id").alias("SubAccountId"),
            pl.col("line_item_usage_account_name").alias("SubAccountName"),
            pl.col("resource_tags")
            .map_elements(normalize_tag_keys, return_dtype=pl.Utf8, skip_nulls=True)
            .alias("Tags"),
        )
        # Ensure exact contract order and dtypes (text columns may be empty strings in CSV).
        mapped = mapped.select(
            [
                (
                    pl.col(name).cast(dtype).str.strip_chars().replace("", None)
                    if dtype == pl.Utf8
                    else pl.col(name).cast(dtype)
                ).alias(name)
                for name, dtype in FOCUS_COLUMNS.items()
            ]
        )
        raw_total = float(typed.get_column("line_item_unblended_cost").fill_null(0).sum())
        return AdapterResult(
            frame=mapped,
            raw_row_count=frame.height,
            raw_billed_total=raw_total,
            cast_report=report,
        )


_OPTIONAL_TEXT = (
    "bill_payer_account_name",
    "bill_invoicing_entity",
    "line_item_usage_account_id",
    "line_item_usage_account_name",
    "line_item_line_item_description",
    "line_item_availability_zone",
    "line_item_resource_id",
    "line_item_usage_type",
    "line_item_legal_entity",
    "pricing_unit",
    "pricing_term",
    "product_region_code",
    "product_sku",
    "savings_plan_savings_plan_a_r_n",
    "reservation_reservation_a_r_n",
    "resource_tags",
)
