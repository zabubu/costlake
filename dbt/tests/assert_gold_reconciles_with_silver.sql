{#- Aggregation must neither lose nor create cost: gold totals equal silver totals. -#}
with silver as (
    select provider, sum(billed_cost) as billed, sum(effective_cost) as effective
    from {{ ref('stg_focus__charges') }}
    group by provider
),

gold as (
    select provider, sum(billed_cost) as billed, sum(effective_cost) as effective
    from {{ ref('fct_daily_cost') }}
    group by provider
)

select s.provider, s.billed, g.billed as gold_billed, s.effective, g.effective as gold_effective
from silver s
full outer join gold g using (provider)
where abs(coalesce(s.billed, 0) - coalesce(g.billed, 0)) > 0.01
   or abs(coalesce(s.effective, 0) - coalesce(g.effective, 0)) > 0.01
