select
    store_id,
    store_name,
    channel,
    country,
    city,
    opened_date
from {{ ref('stg_stores') }}
