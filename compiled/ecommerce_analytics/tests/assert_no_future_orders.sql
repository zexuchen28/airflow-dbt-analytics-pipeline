-- A singular test: fails (returns rows) if any order has a timestamp in the future.
-- Mirrors the kind of upstream-freshness sanity check used in production
-- reporting pipelines (e.g. the GDPR reporting checks referenced in the README).

select order_id, order_ts
from "warehouse"."main_marts"."fct_orders"
where order_ts > current_timestamp