select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price_eur,
    discount_pct,
    round(quantity * unit_price_eur * (1 - discount_pct / 100.0), 2) as line_total_eur
from {{ delta_source('silver', 'order_items') }}
