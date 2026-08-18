-- Passthrough of silver.customers -- identity resolution already
-- happened in Spark (see spark_jobs/bronze_to_silver/customers.py).
-- Nothing left for dbt to clean; this model exists so marts never
-- reference delta_source() directly, keeping the engine-specific read
-- isolated to this one layer (see macros/delta_source.sql).
--
-- CI fixture note: `source_customer_ids` is a real BIGINT[] column
-- coming out of Spark (customers.py tracks it as an array on purpose --
-- see stg_customer_identity_map.sql for why). CI's seed fixtures are
-- small, hand-written CSVs with no duplicate identities to resolve, so
-- rather than fight CSV/array type inference for a column that would
-- only ever hold a single-element list in the fixture anyway, `{{
-- var('use_seeds', false) }}` mode just synthesises it as
-- `[customer_id]` directly. The real unnest-and-remap logic
-- (stg_customer_identity_map.sql) still runs unchanged against it --
-- only the shape of the input array differs, not the logic under test.
select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    city,
    signup_date,
    is_marketing_opt_in,
    merged_identity_count,
{% if var('use_seeds', false) -%}
    [customer_id] as source_customer_ids
{%- else -%}
    source_customer_ids
{%- endif %}
from {{ delta_source('silver', 'customers') }}
