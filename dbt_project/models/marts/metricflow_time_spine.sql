-- Required by the dbt Semantic Layer / MetricFlow whenever a semantic model
-- uses a `type: time` dimension (see order_date in _semantic_models.yml).
-- This is a generic date spine, not business data — it doesn't depend on
-- any other model and just needs to cover the full range dates in
-- fct_orders could ever fall in.
-- Docs: https://docs.getdbt.com/docs/build/metricflow-time-spine

{{
    config(
        materialized = 'table',
    )
}}

with days as (
    {{
        dbt_utils.date_spine(
            'day',
            "cast('2020-01-01' as date)",
            "cast('2030-01-01' as date)"
        )
    }}
)

select cast(date_day as date) as date_day
from days
