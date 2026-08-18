select
    payment_id,
    order_id,
    payment_method,
    amount_eur,
    payment_status,
    paid_at
from {{ delta_source('silver', 'payments') }}
