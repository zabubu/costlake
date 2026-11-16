{#-
  Daily usage cost per (provider, service): the series monitored for anomalies.
  Days without any charge are filled with 0 so that a service going silent is
  visible as a drop instead of a gap.
-#}
with usage as (
    select charge_date, provider, service_name, currency, sum(effective_cost) as effective_cost
    from {{ ref('fct_daily_cost') }}
    where charge_category = 'Usage'
    group by all
),

series as (
    select provider, service_name, any_value(currency) as currency,
           min(charge_date) as first_day
    from usage
    group by all
),

calendar as (
    select cast(day as date) as charge_date
    from range(
        (select min(charge_date) from usage),
        (select max(charge_date) from usage) + interval 1 day,
        interval 1 day
    ) as t(day)
)

select
    c.charge_date,
    s.provider,
    s.service_name,
    s.currency,
    round(coalesce(u.effective_cost, 0), 6) as effective_cost
from series s
join calendar c on c.charge_date >= s.first_day
left join usage u
    on u.provider = s.provider
   and u.service_name = s.service_name
   and u.charge_date = c.charge_date
