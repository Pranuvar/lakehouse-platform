-- Passthrough of silver.products, SCD2 columns intact. The SCD2 MERGE
-- logic itself lives in spark_jobs/bronze_to_silver/products_scd2.py --
-- this project deliberately puts SCD2 in Spark, not a dbt snapshot (see
-- that file's docstring for why). dbt's job here is just point-in-time
-- correct joins downstream (see marts/fct_order_items.sql), not
-- re-deriving history dbt never saw being built.
select
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    unit_cost_eur,
    unit_price_eur,
    is_active,
    valid_from,
    valid_to,
    is_current,
    _silver_processed_at
from {{ delta_source('silver', 'products') }}
