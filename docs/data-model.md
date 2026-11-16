# Data model

## Layers

| Layer | Location | Format | Written by | Semantics |
|---|---|---|---|---|
| Landing | `s3://costlake-landing/<source prefix>/…` | Provider files (CSV, CSV.gz, Parquet) | Cloud provider export (locally: `costlake generate`) | Untouched, expires after 90 days (Terraform lifecycle rule) |
| Bronze | `s3://costlake-lake/bronze/source=<id>/delivery=<id>/…` | Same bytes as landing | `bronze_deliveries` | Immutable audit copy, SHA-256 in the ledger, replayable |
| Silver | `s3://costlake-lake/silver/focus/source=<id>/period=<YYYY-MM>/data.parquet` | Parquet (zstd), FOCUS 1.0 columns + lineage | `silver_focus` | One object per (source, billing period), replaced on restatement |
| Gold | DuckDB `gold` schema | Tables | dbt + `cost_anomalies` | Rebuilt from silver on every run |
| Serving | PostgreSQL `serving` schema | Tables | `serving_tables` | Copy of gold, atomically swapped |
| Ledger | PostgreSQL `public` schema | Tables | every step | Operational metadata (below) |

## Ledger

| Table | Content |
|---|---|
| `deliveries` | One row per delivery: source, folder, status (`bronze`, `published`, `superseded`, `quarantined`), export time, rows, billed total, billing periods, error |
| `delivery_files` | Files of a delivery: landing key, ETag, size, SHA-256, bronze key |
| `silver_partitions` | Current owner (delivery) of each (source, billing period) silver object, with row count and billed total |
| `dq_results` | Result of every quality rule for every delivery |
| `step_runs` | Start, end, status, row count and error of every pipeline step |

## Silver contract (FOCUS 1.0)

Silver keeps 43 FOCUS 1.0 columns (`src/costlake/normalization/schema.py`) with fixed types: timestamps as UTC `Datetime(us)`, amounts and quantities as `Float64`, everything else as text; `Tags` is a JSON object. Lineage columns follow the FOCUS convention for custom columns (`x_` prefix): `x_source_id`, `x_delivery_id`, `x_billing_period`, `x_ingested_at`.

Columns an input must provide: `BilledCost`, `EffectiveCost`, `BillingAccountId`, `BillingCurrency`, `BillingPeriodStart`, `BillingPeriodEnd`, `ChargeCategory`, `ChargePeriodStart`, `ChargePeriodEnd`, `ProviderName`, `ServiceName`. The other columns are filled with nulls when absent.

Amounts are `Float64`, like the provider exports themselves. Totals are reconciled with a 0.01 tolerance at every boundary (adapter, silver → gold dbt test); a decimal type would add conversion cost on every read without changing what is reported at cent precision.

### Quality gate

| Rule | Severity | Fails when |
|---|---|---|
| `not_empty` | error | the delivery has no rows |
| `type_casts` | error | a non-empty value cannot be parsed (number, timestamp); counted per column |
| `required_not_null` | error | a mandatory column has nulls |
| `charge_period_order` | error | `ChargePeriodEnd <= ChargePeriodStart` |
| `charge_within_billing_period` | error | a charge starts outside its billing period |
| `charge_category_values` | error | `ChargeCategory` is not one of Usage, Purchase, Tax, Credit, Adjustment |
| `single_currency_per_account` | error | a billing account mixes currencies |
| `billed_cost_reconciliation` | error | row count or billed total differs between the raw file and silver (> 0.01) |
| `service_category_values` | warning | `ServiceCategory` is not a FOCUS value |
| `effective_cost_not_above_list` | warning | usage effective cost exceeds list cost |
| `allocation_tag_coverage` | warning | less than 80 % of usage cost carries the `team` tag |
| `duplicate_rows` | warning | fully duplicated rows |

## AWS CUR 2.0 → FOCUS mapping

| FOCUS column | CUR 2.0 source |
|---|---|
| `BilledCost` | `line_item_unblended_cost` (every line, so the invoice total is preserved) |
| `EffectiveCost` | `savings_plan_savings_plan_effective_cost` on `SavingsPlanCoveredUsage`; `reservation_effective_cost` on `DiscountedUsage`; `0` on `SavingsPlanNegation`, `SavingsPlanRecurringFee`, `SavingsPlanUpfrontFee`, `RIFee` (amortised into usage); otherwise `line_item_net_unblended_cost`, falling back to unblended |
| `ListCost` | `pricing_public_on_demand_cost`, falling back to unblended |
| `ChargeCategory` | `line_item_line_item_type`: usage types → Usage; fees → Purchase; Tax; Credit/Refund → Credit; discounts → Adjustment; **unknown → null → delivery rejected** |
| `ChargeFrequency` | Usage-Based for usage, Recurring for RI/SP recurring fees and tax, One-Time otherwise |
| `PricingCategory` | Committed when covered by SP/RI, else `pricing_term` (OnDemand → Standard, Spot → Dynamic) |
| `CommitmentDiscount*` | SP or RI ARN, type and category (Spend for SP, Usage for RI), status Used |
| `ChargePeriodStart/End`, `BillingPeriodStart/End` | `line_item_usage_start/end_date`, `bill_billing_period_start/end_date` |
| `BillingAccountId/Name`, `SubAccountId/Name` | `bill_payer_account_id/name`, `line_item_usage_account_id/name` |
| `ServiceName`, `ServiceCategory` | `line_item_product_code` through a lookup table (unknown codes keep the code and category Other) |
| `RegionId`, `AvailabilityZone`, `ResourceId`, `SkuId` | `product_region_code`, `line_item_availability_zone`, `line_item_resource_id`, `product_sku` |
| `Tags` | `resource_tags`, with the `user_` / `user:` prefixes removed and keys lower-cased |

Known simplification: unused Savings Plan commitment is not emitted as a separate `Unused` row (CUR exposes it only through commitment-level columns). The generator's commitments are fully used, so tests are not affected; see the roadmap.

## Gold models (dbt)

| Model | Grain | Used for |
|---|---|---|
| `stg_focus__charges` (view) | FOCUS charge | Renaming, tag extraction (`allocation_owner`, `environment`, `cost_center`) |
| `fct_daily_cost` | day × provider × account × sub-account × service × charge category × owner × environment × currency | Every report |
| `agg_daily_service_cost` | day × provider × service, gap-filled with 0 | Anomaly detection |
| `mart_owner_monthly_cost` | period × provider × owner | Showback |
| `mart_tag_coverage_daily` | day × provider | Allocation hygiene |
| `mart_commitment_savings_monthly` | period × provider | Savings and coverage of commitments |
| `mart_month_to_date` | provider (latest period) | Run-rate forecast vs previous period |

Tests: uniqueness of every grain, not-null keys, accepted values, ratio bounds, and `assert_gold_reconciles_with_silver` (billed and effective totals per provider identical between staging and the fact table).

## Anomaly detection

For each (provider, service) daily series and each day `d` with at least 14 days of history:

* baseline = the 42 days before `d` (never `d` itself);
* level `m` = median of the baseline; weekday factor `f(w)` = median of the baseline days with weekday `w` divided by `m`;
* expected `e = m · f(weekday(d))`; scale `s = 1.4826 · MAD(residuals)`, floored at 2 % of `m`;
* `z = (x − e) / s`; anomaly if `|z| ≥ 3.5` **and** `|x − e| ≥ 25` **and** `|x − e| / e ≥ 30 %` (configurable per environment);
* severity `critical` when `|z| ≥ 7` and `|x − e| ≥ 100`.

Median and MAD are robust: a previous incident inside the window does not inflate the baseline and hide the next one (`test_past_incident_does_not_mask_the_next_one`). The weekday profile prevents an alert every weekend for workloads that scale down (`test_weekly_seasonality_is_not_flagged`). The absolute and relative floors keep statistically significant but financially irrelevant blips out of the alert channel.

The method is univariate and explainable on purpose: each alert carries the actual cost, the expected cost and the z-score, which a FinOps analyst can verify by hand.
