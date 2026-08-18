-- The actual payoff of doing SCD2 properly, not just structurally: this
-- joins each order line to the version of dim_products that was ACTUALLY
-- CURRENT on the order's date (valid_from <= order_date < valid_to),
-- not whatever the product's price happens to be today. `unit_price_eur`
-- on the order line is already the real transaction price (stored at
-- sale time, nothing to look up) -- but `unit_cost_eur` for margin isn't
-- captured on the order line at all, so a naive `join dim_products on
-- product_id` (ignoring SCD2 entirely) would silently compute every
-- historical margin using TODAY's cost. For the products whose price
-- changed during this build (see docs/incident-log.md's SCD2 entries in
-- BUILD_LOG), that's a real, demonstrable difference, not a theoretical
-- one.
--
-- `earliest_version` fallback, and why it's needed: the ORIGINAL seed
-- data (seeders/seed_postgres_oltp.py, Day 1) never constrained
-- order_item product selection by the product's own creation date --
-- an order dated early in the 2-year history can reference a product
-- whose `created_at` (-> its earliest SCD2 `valid_from`) is later than
-- the order itself, which isn't physically sensible (can't sell
-- something before it exists) but was never enforced at generation
-- time. A strict point-in-time join leaves those rows with no matching
-- product version at all -- caught directly: 2,251,220 of 5,625,033
-- order_items (40%) came back with a NULL margin on this project's
-- first correctly-backdated `dbt build`, and confirmed by direct query
-- that EVERY one of them is exactly this case (order_date before the
-- product's earliest tracked version), not some other failure mode.
-- Re-seeding to fix the root cause would mean redoing everything built
-- on top of oltp.products since Day 1; the honest, defensible fix at
-- this layer is to fall back to the EARLIEST known version of the
-- product for these rows -- "our dimension history doesn't reach back
-- as far as this transaction, so the oldest cost on record is the best
-- available estimate" is a real, common judgement call in point-in-time
-- modelling, not a hack, as long as it's flagged (`used_earliest_
-- version_fallback`) rather than silently blended in.
with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

earliest_version as (
    select *
    from products
    qualify row_number() over (partition by product_id order by valid_from asc) = 1
),

joined as (
    select
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        o.store_id,
        coalesce(p.product_key, ev.product_key) as product_key,
        oi.product_id,
        o.order_date,
        o.order_month,
        oi.quantity,
        oi.unit_price_eur,
        oi.discount_pct,
        oi.line_total_eur,
        coalesce(p.unit_cost_eur, ev.unit_cost_eur) as unit_cost_eur_at_sale,
        round(oi.quantity * coalesce(p.unit_cost_eur, ev.unit_cost_eur), 2) as cost_total_eur,
        round(oi.line_total_eur - (oi.quantity * coalesce(p.unit_cost_eur, ev.unit_cost_eur)), 2) as gross_margin_eur,
        p.product_key is null and ev.product_key is not null as used_earliest_version_fallback
    from order_items as oi
    inner join orders as o on oi.order_id = o.order_id
    left join products as p
        on
            oi.product_id = p.product_id
            and o.order_date >= p.valid_from
            and (p.valid_to is null or o.order_date < p.valid_to)
    left join earliest_version as ev on oi.product_id = ev.product_id
)

select * from joined
