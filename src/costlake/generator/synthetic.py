"""Deterministic synthetic billing data, for demos, tests and load tests.

Real billing exports cannot be shipped in a public repository, so the generator
produces FOCUS 1.0 data with the properties that matter to the pipeline:

* several providers, billing accounts and sub-accounts, tagged and untagged resources;
* daily usage with trend, weekly seasonality and noise;
* Savings Plan coverage with FOCUS semantics (covered usage has ``BilledCost = 0``
  and an amortised ``EffectiveCost``; the commitment is billed as ``Purchase``);
* credits, and injected cost anomalies whose dates are known, so that detection can
  be asserted in tests;
* month-to-date deliveries, to exercise restatement handling;
* the same data rendered as an AWS CUR 2.0 export for the legacy adapter.
"""

from __future__ import annotations

import gzip
import io
import json
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import numpy as np
import polars as pl

from costlake.normalization.schema import FOCUS_COLUMNS
from costlake.storage import ObjectStore


@dataclass(frozen=True)
class Service:
    name: str
    category: str
    daily_cost: float
    unit: str
    unit_price: float
    weekend_factor: float = 1.0
    commitment_coverage: float = 0.0  # share of usage covered by a Savings Plan
    commitment_discount: float = 0.0


@dataclass(frozen=True)
class SubAccount:
    id: str
    name: str
    tags: dict[str, str]
    untagged_share: float  # share of resources without tags
    services: tuple[tuple[Service, float], ...]  # (service, weight)


@dataclass(frozen=True)
class Provider:
    name: str
    billing_account_id: str
    billing_account_name: str
    currency: str
    region: str
    sub_accounts: tuple[SubAccount, ...]
    monthly_credit: float = 0.0


@dataclass(frozen=True)
class Anomaly:
    day: date
    provider: str
    service: str
    multiplier: float


_EC2 = Service("Amazon Elastic Compute Cloud", "Compute", 420.0, "Hours", 0.192, 0.8, 0.55, 0.28)
_S3 = Service("Amazon Simple Storage Service", "Storage", 64.0, "GB-Mo", 0.023)
_RDS = Service("Amazon Relational Database Service", "Databases", 180.0, "Hours", 0.48)
_LAMBDA = Service("AWS Lambda", "Compute", 14.0, "Requests", 0.0000002, 0.7)
_CLOUDFRONT = Service("Amazon CloudFront", "Networking", 38.0, "GB", 0.085)
_VM = Service("Virtual Machines", "Compute", 260.0, "Hours", 0.166, 0.85, 0.4, 0.22)
_AKS = Service("Azure Kubernetes Service", "Compute", 95.0, "Hours", 0.1, 0.9)
_AZSQL = Service("Azure SQL Database", "Databases", 120.0, "vCore Hours", 0.505)
_BLOB = Service("Storage", "Storage", 42.0, "GB/Month", 0.0184)
_GCE = Service("Compute Engine", "Compute", 150.0, "Hours", 0.134, 0.75)
_GCS = Service("Cloud Storage", "Storage", 26.0, "GiB-Month", 0.02)
_BQ = Service("BigQuery", "Analytics", 82.0, "TiB", 6.25, 0.5)
_VERTEX = Service("Vertex AI", "AI and Machine Learning", 64.0, "Hours", 2.48, 0.6)

DEFAULT_PROVIDERS: tuple[Provider, ...] = (
    Provider(
        "AWS",
        "111111111111",
        "acme-payer",
        "USD",
        "eu-west-1",
        (
            SubAccount(
                "222222222222",
                "platform-prod",
                {"team": "platform", "env": "prod", "cost_center": "cc-100"},
                0.0,
                ((_EC2, 0.6), (_RDS, 1.0), (_CLOUDFRONT, 1.0), (_LAMBDA, 0.5)),
            ),
            SubAccount(
                "333333333333",
                "data-prod",
                {"team": "data", "env": "prod", "cost_center": "cc-200"},
                0.1,
                ((_EC2, 0.4), (_S3, 1.0), (_LAMBDA, 0.5)),
            ),
            SubAccount(
                "444444444444",
                "sandbox",
                {"env": "dev"},
                0.6,
                ((_EC2, 0.1), (_S3, 0.15)),
            ),
        ),
    ),
    Provider(
        "Microsoft",
        "ea-80012345",
        "acme-enterprise-agreement",
        "USD",
        "westeurope",
        (
            SubAccount(
                "9f1c2d3e-0000-4a5b-8c7d-111111111111",
                "web-prod",
                {"team": "web", "env": "prod", "cost_center": "cc-300"},
                0.05,
                ((_VM, 0.7), (_AKS, 1.0), (_AZSQL, 1.0), (_BLOB, 0.6)),
            ),
            SubAccount(
                "9f1c2d3e-0000-4a5b-8c7d-222222222222",
                "web-staging",
                {"team": "web", "env": "staging", "cost_center": "cc-300"},
                0.2,
                ((_VM, 0.3), (_BLOB, 0.4)),
            ),
        ),
    ),
    Provider(
        "Google Cloud",
        "01A2B3-C4D5E6-F7A8B9",
        "acme-gcp-billing",
        "USD",
        "europe-west1",
        (
            SubAccount(
                "ml-research-prod",
                "ml-research-prod",
                {"team": "ml", "env": "prod", "cost_center": "cc-400"},
                0.15,
                ((_VERTEX, 1.0), (_BQ, 0.6), (_GCS, 0.7), (_GCE, 0.5)),
            ),
            SubAccount(
                "analytics-prod",
                "analytics-prod",
                {"team": "data", "env": "prod", "cost_center": "cc-200"},
                0.05,
                ((_BQ, 0.4), (_GCS, 0.3), (_GCE, 0.5)),
            ),
        ),
        monthly_credit=150.0,
    ),
)

# A separate, acquired AWS organisation that still delivers CUR 2.0.
LEGACY_AWS = Provider(
    "AWS",
    "555555555555",
    "legacy-payer",
    "USD",
    "us-east-1",
    (
        SubAccount(
            "666666666666",
            "legacy-erp",
            {"team": "erp", "env": "prod"},
            0.2,
            ((_EC2, 0.3), (_RDS, 0.5), (_S3, 0.3)),
        ),
    ),
)


def default_anomalies(end: date) -> list[Anomaly]:
    """Three incidents at known dates relative to ``end`` (exclusive)."""
    return [
        Anomaly(end - timedelta(days=20), "AWS", _EC2.name, 3.0),  # runaway batch fleet
        Anomaly(end - timedelta(days=9), "Google Cloud", _BQ.name, 6.0),  # full-scan queries
        Anomaly(end - timedelta(days=4), "Microsoft", _AKS.name, 0.15),  # node pool lost
    ]


def _resources_per_service(service: Service) -> int:
    return 3 if service.category == "Compute" else 2


def generate_focus(
    provider: Provider,
    start: date,
    end: date,
    seed: int = 42,
    anomalies: Iterable[Anomaly] = (),
    growth_per_month: float = 0.02,
    scale: int = 1,
) -> pl.DataFrame:
    """FOCUS rows with daily granularity for ``[start, end)``."""
    rng = np.random.default_rng([seed, sum(map(ord, provider.billing_account_id))])
    spikes = {(a.day, a.service): a.multiplier for a in anomalies if a.provider == provider.name}
    days = [start + timedelta(days=i) for i in range((end - start).days)]
    rows: list[dict[str, object]] = []
    commitment_id = (
        f"arn:aws:savingsplans::{provider.billing_account_id}:savingsplan/sp-0001"
        if provider.name == "AWS"
        else f"/providers/Microsoft.BillingBenefits/savingsPlanOrders/{provider.billing_account_id}"
    )

    resources = []
    for account in provider.sub_accounts:
        for service, weight in account.services:
            count = _resources_per_service(service) * scale
            for idx in range(count):
                untagged = rng.random() < account.untagged_share
                resource_id = (
                    f"{provider.name.split()[0].lower()}:{account.name}:"
                    f"{service.name.split()[-1].lower()}-{idx:03d}"
                )
                share = weight / count
                resources.append((account, service, resource_id, share, untagged))

    purchases: dict[date, float] = {}
    for day in days:
        t_months = (day - start).days / 30.0
        trend = (1 + growth_per_month) ** t_months
        weekend = day.weekday() >= 5
        period_start, period_end = _month_bounds(day)
        for account, service, resource_id, share, untagged in resources:
            factor = service.weekend_factor if weekend else 1.0
            noise = float(rng.lognormal(0.0, 0.07))
            spike = spikes.get((day, service.name), 1.0)
            cost = round(service.daily_cost * share * trend * factor * noise * spike, 6)
            quantity = round(cost / service.unit_price, 6)
            covered = rng.random() < service.commitment_coverage and spike == 1.0
            tags = None if untagged else json.dumps(account.tags, sort_keys=True)
            if covered:
                effective = round(cost * (1 - service.commitment_discount), 6)
                billed = 0.0
                list_cost = cost
                purchases[day] = purchases.get(day, 0.0) + effective
            else:
                effective = billed = list_cost = cost
            rows.append(
                {
                    "AvailabilityZone": None,
                    "BilledCost": billed,
                    "BillingAccountId": provider.billing_account_id,
                    "BillingAccountName": provider.billing_account_name,
                    "BillingCurrency": provider.currency,
                    "BillingPeriodEnd": period_end,
                    "BillingPeriodStart": period_start,
                    "ChargeCategory": "Usage",
                    "ChargeClass": None,
                    "ChargeDescription": f"{service.name} usage",
                    "ChargeFrequency": "Usage-Based",
                    "ChargePeriodEnd": _ts(day + timedelta(days=1)),
                    "ChargePeriodStart": _ts(day),
                    "CommitmentDiscountCategory": "Spend" if covered else None,
                    "CommitmentDiscountId": commitment_id if covered else None,
                    "CommitmentDiscountName": "compute-savings-plan" if covered else None,
                    "CommitmentDiscountStatus": "Used" if covered else None,
                    "CommitmentDiscountType": "Savings Plan" if covered else None,
                    "ConsumedQuantity": quantity,
                    "ConsumedUnit": service.unit,
                    "ContractedCost": list_cost,
                    "ContractedUnitPrice": service.unit_price,
                    "EffectiveCost": effective,
                    "InvoiceIssuerName": provider.name,
                    "ListCost": list_cost,
                    "ListUnitPrice": service.unit_price,
                    "PricingCategory": "Committed" if covered else "Standard",
                    "PricingQuantity": quantity,
                    "PricingUnit": service.unit,
                    "ProviderName": provider.name,
                    "PublisherName": provider.name,
                    "RegionId": provider.region,
                    "RegionName": provider.region,
                    "ResourceId": resource_id,
                    "ResourceName": resource_id.rsplit(":", 1)[-1],
                    "ResourceType": service.category,
                    "ServiceCategory": service.category,
                    "ServiceName": service.name,
                    "SkuId": f"sku-{zlib.crc32(service.name.encode()) % 10_000:04d}",
                    "SkuPriceId": None,
                    "SubAccountId": account.id,
                    "SubAccountName": account.name,
                    "Tags": tags,
                }
            )

    for day, amount in purchases.items():
        period_start, period_end = _month_bounds(day)
        rows.append(
            _non_usage_row(
                provider,
                day,
                period_start,
                period_end,
                category="Purchase",
                frequency="Recurring",
                description="Savings Plan recurring fee",
                billed=round(amount, 6),
                effective=0.0,
                commitment_id=commitment_id,
            )
        )
    if provider.monthly_credit:
        for day in days:
            if day.day == 15:
                period_start, period_end = _month_bounds(day)
                rows.append(
                    _non_usage_row(
                        provider,
                        day,
                        period_start,
                        period_end,
                        category="Credit",
                        frequency="One-Time",
                        description="Promotional credit",
                        billed=-provider.monthly_credit,
                        effective=-provider.monthly_credit,
                        commitment_id=None,
                    )
                )

    frame = pl.DataFrame(rows, schema=FOCUS_COLUMNS, orient="row")
    return frame.sort(["ChargePeriodStart", "SubAccountId", "ResourceId", "ChargeCategory"])


def _non_usage_row(
    provider: Provider,
    day: date,
    period_start: datetime,
    period_end: datetime,
    *,
    category: str,
    frequency: str,
    description: str,
    billed: float,
    effective: float,
    commitment_id: str | None,
) -> dict[str, object]:
    row: dict[str, object] = dict.fromkeys(FOCUS_COLUMNS)
    row.update(
        {
            "BilledCost": billed,
            "BillingAccountId": provider.billing_account_id,
            "BillingAccountName": provider.billing_account_name,
            "BillingCurrency": provider.currency,
            "BillingPeriodEnd": period_end,
            "BillingPeriodStart": period_start,
            "ChargeCategory": category,
            "ChargeDescription": description,
            "ChargeFrequency": frequency,
            "ChargePeriodEnd": _ts(day + timedelta(days=1)),
            "ChargePeriodStart": _ts(day),
            "CommitmentDiscountId": commitment_id,
            "ContractedCost": billed,
            "EffectiveCost": effective,
            "InvoiceIssuerName": provider.name,
            "ListCost": billed,
            "ProviderName": provider.name,
            "PublisherName": provider.name,
            "ServiceCategory": "Other",
            "ServiceName": "Savings Plans" if category == "Purchase" else "Billing",
        }
    )
    return row


def _ts(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def _month_bounds(day: date) -> tuple[datetime, datetime]:
    start = date(day.year, day.month, 1)
    end = date(day.year + 1, 1, 1) if day.month == 12 else date(day.year, day.month + 1, 1)
    return _ts(start), _ts(end)


# -- CUR 2.0 rendering -----------------------------------------------------------------

_CUR2_PRODUCT_CODES = {
    "Amazon Elastic Compute Cloud": "AmazonEC2",
    "Amazon Simple Storage Service": "AmazonS3",
    "Amazon Relational Database Service": "AmazonRDS",
    "AWS Lambda": "AWSLambda",
    "Amazon CloudFront": "AmazonCloudFront",
    "Savings Plans": "ComputeSavingsPlans",
    "Billing": "AWSBilling",
}


def focus_to_cur2(frame: pl.DataFrame) -> pl.DataFrame:
    """Render FOCUS rows as AWS CUR 2.0 columns (covered usage + negation + fee)."""
    covered = pl.col("CommitmentDiscountStatus") == "Used"
    base = frame.with_columns(
        pl.when(covered)
        .then(pl.lit("SavingsPlanCoveredUsage"))
        .when(pl.col("ChargeCategory") == "Purchase")
        .then(pl.lit("SavingsPlanRecurringFee"))
        .when(pl.col("ChargeCategory") == "Credit")
        .then(pl.lit("Credit"))
        .otherwise(pl.lit("Usage"))
        .alias("line_item_line_item_type"),
        # On covered lines CUR shows the on-demand cost, cancelled by a negation line.
        pl.when(covered)
        .then(pl.col("ListCost"))
        .otherwise(pl.col("BilledCost"))
        .alias("line_item_unblended_cost"),
    )
    negations = base.filter(covered).with_columns(
        pl.lit("SavingsPlanNegation").alias("line_item_line_item_type"),
        (-pl.col("line_item_unblended_cost")).alias("line_item_unblended_cost"),
        pl.lit(None, dtype=pl.Float64).alias("EffectiveCost"),
        pl.lit(0.0).alias("ListCost"),
    )
    rows = pl.concat([base, negations])
    return rows.select(
        pl.col("BillingAccountId").alias("bill_payer_account_id"),
        pl.col("BillingAccountName").alias("bill_payer_account_name"),
        _iso(pl.col("BillingPeriodStart")).alias("bill_billing_period_start_date"),
        _iso(pl.col("BillingPeriodEnd")).alias("bill_billing_period_end_date"),
        pl.lit("Amazon Web Services EMEA SARL").alias("bill_invoicing_entity"),
        pl.col("SubAccountId").alias("line_item_usage_account_id"),
        pl.col("SubAccountName").alias("line_item_usage_account_name"),
        pl.col("line_item_line_item_type"),
        pl.col("ChargeDescription").alias("line_item_line_item_description"),
        _iso(pl.col("ChargePeriodStart")).alias("line_item_usage_start_date"),
        _iso(pl.col("ChargePeriodEnd")).alias("line_item_usage_end_date"),
        pl.col("ServiceName")
        .replace_strict(_CUR2_PRODUCT_CODES, default="AWSOther")
        .alias("line_item_product_code"),
        pl.col("ResourceType").alias("line_item_usage_type"),
        pl.col("ConsumedQuantity").alias("line_item_usage_amount"),
        pl.col("line_item_unblended_cost"),
        pl.col("ListUnitPrice").alias("line_item_unblended_rate"),
        pl.col("BillingCurrency").alias("line_item_currency_code"),
        pl.col("ResourceId").alias("line_item_resource_id"),
        pl.lit("Amazon Web Services, Inc.").alias("line_item_legal_entity"),
        pl.when(pl.col("ChargeCategory") == "Usage")
        .then(pl.col("ListCost"))
        .alias("pricing_public_on_demand_cost"),
        pl.col("ListUnitPrice").alias("pricing_public_on_demand_rate"),
        pl.col("PricingUnit").alias("pricing_unit"),
        pl.when(pl.col("ChargeCategory") == "Usage").then(pl.lit("OnDemand")).alias("pricing_term"),
        pl.col("RegionId").alias("product_region_code"),
        pl.col("SkuId").alias("product_sku"),
        pl.when(pl.col("line_item_line_item_type") == "SavingsPlanCoveredUsage")
        .then(pl.col("CommitmentDiscountId"))
        .alias("savings_plan_savings_plan_a_r_n"),
        pl.when(pl.col("line_item_line_item_type") == "SavingsPlanCoveredUsage")
        .then(pl.col("EffectiveCost"))
        .alias("savings_plan_savings_plan_effective_cost"),
        pl.col("Tags")
        .map_elements(_cur2_tags, return_dtype=pl.Utf8, skip_nulls=True)
        .alias("resource_tags"),
    )


def _cur2_tags(tags: str) -> str:
    return json.dumps({f"user_{k}": v for k, v in json.loads(tags).items()}, sort_keys=True)


def _iso(expr: pl.Expr) -> pl.Expr:
    return expr.dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# -- writing deliveries ------------------------------------------------------------------


def render(frame: pl.DataFrame, file_format: str) -> tuple[bytes, str]:
    """Serialise a frame the way providers deliver it. Returns (bytes, file suffix)."""
    if file_format == "parquet":
        buffer = io.BytesIO()
        frame.write_parquet(buffer, compression="snappy")
        return buffer.getvalue(), ".snappy.parquet"
    if file_format == "csv.gz":
        text = frame.with_columns(
            pl.col(c).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            for c, t in frame.schema.items()
            if isinstance(t, pl.Datetime)
        ).write_csv()
        return gzip.compress(text.encode(), mtime=0), ".csv.gz"
    raise ValueError(f"unknown file format: {file_format}")


def write_month_to_date_deliveries(
    store: ObjectStore,
    bucket: str,
    prefix: str,
    frame: pl.DataFrame,
    file_format: str,
    export_time: datetime,
    rows_per_file: int = 200_000,
    as_cur2: bool = False,
) -> list[str]:
    """Write one delivery folder per billing period present in ``frame``."""
    written: list[str] = []
    periods = frame.get_column("BillingPeriodStart").dt.strftime("%Y-%m").unique().sort()
    stamp = export_time.strftime("%Y%m%dT%H%M%SZ")
    for period in periods.to_list():
        part = frame.filter(pl.col("BillingPeriodStart").dt.strftime("%Y-%m") == period)
        if as_cur2:
            part = focus_to_cur2(part)
        folder = f"{prefix.rstrip('/')}/{period}/{stamp}/"
        for index, offset in enumerate(range(0, part.height, rows_per_file), start=1):
            data, suffix = render(part.slice(offset, rows_per_file), file_format)
            key = f"{folder}part-{index:05d}{suffix}"
            store.put_bytes(bucket, key, data)
            written.append(key)
    return written
