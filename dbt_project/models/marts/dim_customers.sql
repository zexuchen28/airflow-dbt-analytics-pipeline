select
    customer_id,
    signup_date,
    region,
    is_member,
    datediff('day', signup_date, current_date) as customer_tenure_days
from {{ ref('stg_customers') }}
