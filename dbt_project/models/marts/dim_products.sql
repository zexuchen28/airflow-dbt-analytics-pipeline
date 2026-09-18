select
    product_id,
    category,
    unit_price
from {{ ref('stg_products') }}
