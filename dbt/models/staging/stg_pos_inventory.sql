select
    store_id,
    product_sku,
    cast(snapshot_date as date) as snapshot_date,
    quantity_on_hand,
    unit_cost_eur,
    reorder_point,
    drop_year_month
from {{ delta_source('silver', 'pos_inventory_snapshots') }}
