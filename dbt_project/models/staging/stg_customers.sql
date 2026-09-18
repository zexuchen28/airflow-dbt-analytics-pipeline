with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id::int as customer_id,
    cast(signup_date as date) as signup_date,
    region,
    is_member::boolean as is_member
from source
