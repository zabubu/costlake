{#-
  Month-to-date spend and linear run-rate forecast per provider, for the latest
  billing period of each provider, compared with the previous period.
-#}
with periods as (
    select
        provider,
        billing_period,
        any_value(currency)       as currency,
        sum(effective_cost)       as effective_cost,
        max(charge_date)          as last_charge_date
    from {{ ref('fct_daily_cost') }}
    group by all
),

ranked as (
    select *,
        row_number() over (partition by provider order by billing_period desc) as recency,
        lead(effective_cost) over (partition by provider order by billing_period desc)
            as previous_period_cost
    from periods
)

select
    provider,
    billing_period,
    currency,
    last_charge_date,
    day(last_charge_date)                                              as days_elapsed,
    day(last_day(last_charge_date))                                    as days_in_period,
    round(effective_cost, 2)                                           as mtd_cost,
    round(effective_cost / day(last_charge_date) * day(last_day(last_charge_date)), 2)
                                                                       as forecast_cost,
    round(previous_period_cost, 2)                                     as previous_period_cost,
    round(
        (effective_cost / day(last_charge_date) * day(last_day(last_charge_date)))
        / nullif(previous_period_cost, 0) - 1,
        4
    ) as forecast_vs_previous
from ranked
where recency = 1
