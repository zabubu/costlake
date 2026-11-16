{#-
  Commitment discounts (Savings Plans, reservations) per provider and month.
  savings = what usage would have cost at list price minus what it effectively cost.
-#}
with usage as (
    select * from {{ ref('stg_focus__charges') }}
    where charge_category = 'Usage'
)

select
    billing_period,
    provider,
    any_value(currency)                                                  as currency,
    round(sum(list_cost), 2)                                             as list_cost,
    round(sum(effective_cost), 2)                                        as effective_cost,
    round(sum(list_cost) - sum(effective_cost), 2)                       as savings,
    round((sum(list_cost) - sum(effective_cost)) / nullif(sum(list_cost), 0), 4) as savings_rate,
    round(
        sum(effective_cost) filter (where pricing_category = 'Committed')
        / nullif(sum(effective_cost), 0),
        4
    ) as commitment_coverage
from usage
group by all
