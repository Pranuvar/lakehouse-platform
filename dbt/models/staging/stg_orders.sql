-- customer_id is remapped through stg_customer_identity_map so every
-- order points at a customer_id that actually has a row in
-- stg_customers -- see that model's docstring for why the remap is
-- necessary (identity resolution consolidates customer_ids; orders
-- were generated against the pre-resolution id space).
with orders as (
    select * from {{ delta_source('silver', 'orders') }}
),

identity_map as (
    select * from {{ ref('stg_customer_identity_map') }}
)

select
    o.order_id,
    coalesce(m.canonical_customer_id, o.customer_id) as customer_id,
    o.store_id,
    o.channel,
    o.order_status,
    o.currency,
    o.order_ts,
    cast(o.order_ts as date) as order_date,
    o.created_at,
    o.updated_at,
    o.order_month
from orders as o
left join identity_map as m on o.customer_id = m.original_customer_id
