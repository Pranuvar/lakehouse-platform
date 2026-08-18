-- SCD2 dimension, passed through as-is from silver (the MERGE that
-- built this history runs in Spark -- see
-- spark_jobs/bronze_to_silver/products_scd2.py). A surrogate key is
-- added here because `product_id` alone is not unique in this table
-- (that's the whole point of SCD2): fct_order_items joins on the
-- surrogate, resolved via a point-in-time lookup, not on product_id
-- directly -- see that model for why that distinction actually matters
-- for margin calculations, not just as a modelling formality.
--
-- Surrogate key is (product_id, _silver_processed_at), NOT
-- (product_id, valid_from) -- found the hard way. `valid_from` is a
-- business DATE (day granularity, by design -- see products_scd2.py),
-- and this whole project's build compressed what would normally be
-- weeks of separate demo days into one calendar day: the initial SCD2
-- load and two live mutation demos (a price change, then an is_active
-- flip) ALL ran on the same date. Every product touched by either
-- mutation ended up with two rows sharing an identical `valid_from`,
-- which broke `unique(product_key)` -- 695 duplicates on the first real
-- `dbt build`. `_silver_processed_at` is a real timestamp set once per
-- Spark job invocation, so it's genuinely unique across separate MERGE
-- runs even when they land on the same calendar day, while `valid_from`
-- keeps its clean business-date meaning for anyone actually reading it.
select
    {{ dbt_utils.generate_surrogate_key(['product_id', '_silver_processed_at']) }} as product_key,
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    unit_cost_eur,
    unit_price_eur,
    is_active,
    -- valid_from/valid_to land as strings out of the Spark SCD2 MERGE
    -- (see products_scd2.py) -- cast explicitly here rather than
    -- relying on DuckDB's implicit string/date coercion in every
    -- downstream join (fct_order_items.sql compares these against a
    -- real DATE column).
    cast(valid_from as date) as valid_from,
    cast(valid_to as date) as valid_to,
    is_current
from {{ ref('stg_products') }}
