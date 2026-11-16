{#- Showback: monthly effective cost per owner (allocation tag) and provider. -#}
with monthly as (
    select billing_period, provider, owner, currency, sum(effective_cost) as effective_cost
    from {{ ref('fct_daily_cost') }}
    group by all
)

select
    billing_period,
    provider,
    owner,
    currency,
    round(effective_cost, 2) as effective_cost,
    round(
        effective_cost / nullif(sum(effective_cost) over (partition by billing_period, provider), 0),
        4
    ) as share_of_provider_cost
from monthly
