
    

    create  table
      "warehouse"."main_marts"."fct_orders__dbt_tmp"
  
    
    as (
      with orders as (
    select * from "warehouse"."main_staging"."stg_orders"
),

products as (
    select * from "warehouse"."main_marts"."dim_products"
)

select
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_ts,
    date_trunc('day', o.order_ts) as order_date,
    o.quantity,
    o.device_type,
    o.status,
    p.category,
    p.unit_price,
    round(o.quantity * p.unit_price, 2) as gross_revenue,
    case when o.status = 'completed' then round(o.quantity * p.unit_price, 2) else 0 end as net_revenue
from orders o
left join products p on o.product_id = p.product_id
    );
    
  