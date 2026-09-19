
    

    create  table
      "warehouse"."main_marts"."dim_customers__dbt_tmp"
  
    
    as (
      select
    customer_id,
    signup_date,
    region,
    is_member,
    datediff('day', signup_date, current_date) as customer_tenure_days
from "warehouse"."main_staging"."stg_customers"
    );
    
  