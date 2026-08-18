select
    event_id,
    session_id,
    customer_id,
    event_type,
    cast(event_ts as timestamp) as event_ts,
    device_type,
    referrer,
    product_sku,
    order_id,
    event_date
from {{ delta_source('silver', 'clickstream_events') }}
