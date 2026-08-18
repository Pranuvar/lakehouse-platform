-- The one INCREMENTAL dbt materialization in this project, deliberately
-- singular: the SCD2 story (dim_products) and the upsert/late-arriving-
-- fact story (fct via silver.orders) are both demonstrated at the Spark/
-- Delta MERGE layer in this project instead of dbt, on purpose (see
-- spark_jobs/bronze_to_silver/{products_scd2,orders}.py) -- that's
-- already fully evidenced elsewhere and repeating it in dbt would prove
-- nothing new. This one model exists so dbt's OWN incremental
-- materialization -- a different mechanism, filter-and-append/delete-
-- insert on every run rather than a MERGE -- is still demonstrably
-- present for what it's actually good at: cheaply reprocessing only
-- rows changed since the last run, keyed on the source's own
-- `updated_at` (the restatement cursor -- see docker/mock-api/app.py).
{{
    config(
        materialized='incremental',
        unique_key=['campaign_id', 'ad_set_id', 'performance_date'],
        incremental_strategy='delete+insert',
    )
}}

select
    campaign_id,
    campaign_name,
    ad_set_id,
    channel,
    performance_date,
    impressions,
    clicks,
    conversions,
    spend_eur,
    case when impressions > 0 then round(clicks::double / impressions, 4) else 0 end as ctr,
    case when clicks > 0 then round(conversions::double / clicks, 4) else 0 end as cvr,
    case when conversions > 0 then round(spend_eur / conversions, 2) end as cost_per_conversion_eur,
    updated_at
from {{ ref('stg_campaign_performance') }}

{% if is_incremental() %}
where updated_at > (select coalesce(max(updated_at), '1970-01-01'::timestamp) from {{ this }})
{% endif %}
