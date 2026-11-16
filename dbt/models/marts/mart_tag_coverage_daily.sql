{#- Share of usage cost that can be allocated to an owner, per provider and day. -#}
select
    charge_date,
    provider,
    round(sum(abs(effective_cost)), 2)                                        as usage_cost,
    round(sum(abs(effective_cost)) filter (where owner <> 'unallocated'), 2) as allocated_cost,
    round(
        coalesce(sum(abs(effective_cost)) filter (where owner <> 'unallocated'), 0)
        / nullif(sum(abs(effective_cost)), 0),
        4
    ) as coverage_ratio
from {{ ref('fct_daily_cost') }}
where charge_category = 'Usage'
group by all
