{#- Daily cost at the finest grain used by reports. Amounts are in billing currency. -#}
select
    charge_date,
    billing_period,
    provider,
    billing_account_id,
    sub_account_id,
    any_value(sub_account_name)                     as sub_account_name,
    service_category,
    service_name,
    charge_category,
    coalesce(allocation_owner, 'unallocated')       as owner,
    coalesce(environment, 'unknown')                as environment,
    currency,
    round(sum(billed_cost), 6)                      as billed_cost,
    round(sum(effective_cost), 6)                   as effective_cost,
    round(sum(list_cost), 6)                        as list_cost,
    count(*)                                        as charge_count
from {{ ref('stg_focus__charges') }}
group by all
