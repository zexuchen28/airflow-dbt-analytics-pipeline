with source as (
    select * from "warehouse"."raw"."products"
)

select
    product_id::int as product_id,
    category,
    unit_price::decimal(10,2) as unit_price
from source