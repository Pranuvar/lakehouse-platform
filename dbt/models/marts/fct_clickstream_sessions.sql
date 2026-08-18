-- Session-grain rollup of the raw event stream -- funnel-analysis-ready
-- (page_view -> product_view -> add_to_cart -> checkout_start ->
-- purchase counts per session) and, for converting sessions, the real
-- join back to fct_orders via order_id (see
-- seeders/seed_kafka_events.py: converting sessions were built FROM
-- live orders, not a fabricated relationship).
with events as (
    select * from {{ ref('stg_clickstream_events') }}
)

select
    session_id,
    min(customer_id) as customer_id,  -- constant per session; min() just picks the non-null value
    min(device_type) as device_type,
    min(referrer) as referrer,
    min(event_ts) as session_start_ts,
    max(event_ts) as session_end_ts,
    date_diff('second', min(event_ts), max(event_ts)) as session_duration_seconds,
    count(*) as event_count,
    count(*) filter (where event_type = 'page_view') as page_view_count,
    count(*) filter (where event_type = 'product_view') as product_view_count,
    count(*) filter (where event_type = 'add_to_cart') as add_to_cart_count,
    count(*) filter (where event_type = 'checkout_start') as checkout_start_count,
    count(*) filter (where event_type = 'purchase') as purchase_count,
    (count(*) filter (where event_type = 'purchase')) > 0 as converted,
    max(order_id) as order_id
from events
group by session_id
