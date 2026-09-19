
    

    create  table
      "warehouse"."main_marts"."dim_products__dbt_tmp"
  
    
    as (
      select
    product_id,
    category,
    unit_price
from "warehouse"."main_staging"."stg_products"
    );
    
  