
    
    

with all_values as (

    select
        region as value_field,
        count(*) as n_records

    from "warehouse"."main_staging"."stg_customers"
    group by region

)

select *
from all_values
where value_field not in (
    'US-West','US-East','US-Central','EU','APAC'
)


