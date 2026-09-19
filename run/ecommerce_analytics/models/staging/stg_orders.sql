
  
  create view "warehouse"."main_staging"."stg_orders__dbt_tmp" as (
    with source as (
    select * from "warehouse"."raw"."orders"
),

cleaned as (
    select
        order_id::int as order_id,
        try_cast(customer_id as int) as customer_id,
        product_id::int as product_id,
        cast(order_ts as timestamp) as order_ts,
        quantity::int as quantity,
        device_type,
        status
    from source
)

-- Drop rows with a null customer_id: a small number of records arrive this
-- way from the upstream feed (simulated here) and are quarantined rather
-- than silently joined and dropped downstream. In a production pipeline
-- these would also be routed to a `rejected_records` table for monitoring.
select *
from cleaned
where customer_id is not null
  );
