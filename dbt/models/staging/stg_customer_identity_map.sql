-- Found via a real dbt test failure, not designed up front: identity
-- resolution in Spark (customers.py) collapses ~3,000 duplicate
-- customer_ids down to one canonical row per real person -- but
-- oltp.orders.customer_id was generated against the FULL original
-- 153,000-id space (see seeders/seed_postgres_oltp.py), so ~1,073
-- orders legitimately reference a customer_id that no longer has its
-- own row in stg_customers, only its canonical replacement does.
-- Caught directly: `relationships_stg_orders_customer_id` failed with
-- 1,073 results on the first real `dbt build`. This is the fix -- unnest
-- silver.customers' `source_customer_ids` (every customer_id
-- resolution actually merged, tracked for exactly this reason -- see
-- customers.py's docstring) into a lookup, so any fact table can remap
-- a pre-resolution customer_id to its canonical one instead of
-- referencing a row that no longer exists.
select
    unnest(source_customer_ids) as original_customer_id,
    customer_id as canonical_customer_id
from {{ ref('stg_customers') }}
