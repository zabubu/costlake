"""Silver-layer contract: the subset of FOCUS 1.0 columns kept by costlake.

FOCUS (FinOps Open Cost and Usage Specification, https://focus.finops.org) is the
common billing schema now exported natively by AWS Data Exports, Azure Cost
Management and Google Cloud. Silver tables follow it column-for-column so that
gold models are provider-agnostic. Columns prefixed with ``x_`` are costlake
lineage columns, following the FOCUS convention for custom columns.
"""

from __future__ import annotations

from typing import Final

import polars as pl

TS = pl.Datetime(time_unit="us", time_zone="UTC")

FOCUS_COLUMNS: Final[dict[str, pl.DataType]] = {
    "AvailabilityZone": pl.Utf8(),
    "BilledCost": pl.Float64(),
    "BillingAccountId": pl.Utf8(),
    "BillingAccountName": pl.Utf8(),
    "BillingCurrency": pl.Utf8(),
    "BillingPeriodEnd": TS,
    "BillingPeriodStart": TS,
    "ChargeCategory": pl.Utf8(),
    "ChargeClass": pl.Utf8(),
    "ChargeDescription": pl.Utf8(),
    "ChargeFrequency": pl.Utf8(),
    "ChargePeriodEnd": TS,
    "ChargePeriodStart": TS,
    "CommitmentDiscountCategory": pl.Utf8(),
    "CommitmentDiscountId": pl.Utf8(),
    "CommitmentDiscountName": pl.Utf8(),
    "CommitmentDiscountStatus": pl.Utf8(),
    "CommitmentDiscountType": pl.Utf8(),
    "ConsumedQuantity": pl.Float64(),
    "ConsumedUnit": pl.Utf8(),
    "ContractedCost": pl.Float64(),
    "ContractedUnitPrice": pl.Float64(),
    "EffectiveCost": pl.Float64(),
    "InvoiceIssuerName": pl.Utf8(),
    "ListCost": pl.Float64(),
    "ListUnitPrice": pl.Float64(),
    "PricingCategory": pl.Utf8(),
    "PricingQuantity": pl.Float64(),
    "PricingUnit": pl.Utf8(),
    "ProviderName": pl.Utf8(),
    "PublisherName": pl.Utf8(),
    "RegionId": pl.Utf8(),
    "RegionName": pl.Utf8(),
    "ResourceId": pl.Utf8(),
    "ResourceName": pl.Utf8(),
    "ResourceType": pl.Utf8(),
    "ServiceCategory": pl.Utf8(),
    "ServiceName": pl.Utf8(),
    "SkuId": pl.Utf8(),
    "SkuPriceId": pl.Utf8(),
    "SubAccountId": pl.Utf8(),
    "SubAccountName": pl.Utf8(),
    "Tags": pl.Utf8(),
}

LINEAGE_COLUMNS: Final[dict[str, pl.DataType]] = {
    "x_source_id": pl.Utf8(),
    "x_delivery_id": pl.Utf8(),
    "x_billing_period": pl.Utf8(),
    "x_ingested_at": TS,
}

SILVER_SCHEMA: Final[dict[str, pl.DataType]] = {**FOCUS_COLUMNS, **LINEAGE_COLUMNS}

# Columns an input FOCUS file must provide; the others are filled with nulls.
REQUIRED_FOCUS_COLUMNS: Final[frozenset[str]] = frozenset(
    {
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
    }
)

CHARGE_CATEGORIES: Final[frozenset[str]] = frozenset(
    {"Usage", "Purchase", "Tax", "Credit", "Adjustment"}
)

SERVICE_CATEGORIES: Final[frozenset[str]] = frozenset(
    {
        "AI and Machine Learning",
        "Analytics",
        "Business Applications",
        "Compute",
        "Databases",
        "Developer Tools",
        "Multicloud",
        "Identity",
        "Integration",
        "Internet of Things",
        "Management and Governance",
        "Media",
        "Migration",
        "Mobile",
        "Networking",
        "Security",
        "Storage",
        "Web",
        "Other",
    }
)

NUMERIC_COLUMNS: Final[tuple[str, ...]] = tuple(
    name for name, dtype in FOCUS_COLUMNS.items() if dtype == pl.Float64()
)
TIMESTAMP_COLUMNS: Final[tuple[str, ...]] = tuple(
    name for name, dtype in FOCUS_COLUMNS.items() if dtype == TS
)
