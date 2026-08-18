-- Singular test: fails if it returns any rows. This invariant was
-- designed INTO the seed data on purpose (payments.amount_eur is
-- DERIVED from order_items, not generated independently -- see
-- seeders/seed_postgres_oltp.py's generate_payments()) specifically so
-- it could be a real, meaningful test rather than an assertion about
-- data that was never actually guaranteed to satisfy it. Also enforced
-- as a Spark-side gate before gold is even built (spark_jobs/
-- quality_gate.py) -- this is the SAME invariant checked a second way,
-- at a different layer, which is deliberate: a dbt test here catches a
-- regression introduced in the gold SQL itself, not just upstream.
with order_item_totals as (
    select
        order_id,
        sum(line_total_eur) as item_total
    from {{ ref('stg_order_items') }}
    group by order_id
),

payment_totals as (
    select
        order_id,
        sum(amount_eur) as paid_total
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    i.order_id,
    i.item_total,
    p.paid_total,
    abs(i.item_total - p.paid_total) as discrepancy_eur
from order_item_totals as i
inner join payment_totals as p on i.order_id = p.order_id
where abs(i.item_total - p.paid_total) > 0.05
