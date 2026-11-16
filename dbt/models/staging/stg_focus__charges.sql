{#- One row per FOCUS charge, renamed to snake_case, with allocation tags extracted. -#}
with source as (
    select * from {{ source('silver', 'focus') }}
)

select
    x_source_id                                         as source_id,
    x_delivery_id                                       as delivery_id,
    x_billing_period                                    as billing_period,
    ProviderName                                        as provider,
    BillingAccountId                                    as billing_account_id,
    BillingAccountName                                  as billing_account_name,
    coalesce(SubAccountId, BillingAccountId)            as sub_account_id,
    coalesce(SubAccountName, SubAccountId, BillingAccountName) as sub_account_name,
    BillingCurrency                                     as currency,
    cast(ChargePeriodStart as date)                     as charge_date,
    ChargePeriodStart                                   as charge_period_start,
    ChargePeriodEnd                                     as charge_period_end,
    ChargeCategory                                      as charge_category,
    ChargeFrequency                                     as charge_frequency,
    PricingCategory                                     as pricing_category,
    coalesce(ServiceCategory, 'Other')                  as service_category,
    ServiceName                                         as service_name,
    RegionId                                            as region_id,
    ResourceId                                          as resource_id,
    CommitmentDiscountId                                as commitment_discount_id,
    CommitmentDiscountType                              as commitment_discount_type,
    coalesce(BilledCost, 0)                             as billed_cost,
    coalesce(EffectiveCost, 0)                          as effective_cost,
    coalesce(ListCost, BilledCost, 0)                   as list_cost,
    ConsumedQuantity                                    as consumed_quantity,
    ConsumedUnit                                        as consumed_unit,
    Tags                                                as tags_json,
    nullif(json_extract_string(Tags, '$.{{ var("allocation_tag") }}'), '') as allocation_owner,
    nullif(json_extract_string(Tags, '$.env'), '')      as environment,
    nullif(json_extract_string(Tags, '$.cost_center'), '') as cost_center
from source
