-- Order-grain rollup (fct_order_items is the line-grain fact) -- for
-- dashboards that want "orders and revenue by day/store/customer"
-- without re-aggregating line items every time.
with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(*) as item_count,
        sum(quantity) as total_quantity,
        sum(line_total_eur) as gross_line_total_eur
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(amount_eur) as total_paid_eur,
        count(*) as payment_count
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.store_id,
    o.channel,
    o.order_status,
    o.currency,
    o.order_date,
    o.order_month,
    coalesce(i.item_count, 0) as item_count,
    coalesce(i.total_quantity, 0) as total_quantity,
    coalesce(i.gross_line_total_eur, 0) as gross_line_total_eur,
    coalesce(p.total_paid_eur, 0) as total_paid_eur,
    coalesce(p.payment_count, 0) as payment_count
from orders as o
left join items as i on o.order_id = i.order_id
left join payments as p on o.order_id = p.order_id
