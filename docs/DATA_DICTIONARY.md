# Data Dictionary

Generated from `dbt/target/catalog.json` + `dbt/target/manifest.json` by `governance/generate_data_dictionary.py` -- **do not hand-edit**; regenerate after `dbt docs generate` instead. Column types are introspected from the real, built warehouse, not asserted; test coverage shown is what's actually enforced by CI/the quality gate, not a claim.

Generated against 76 manifest nodes, 18 models.

## Staging (silver passthrough)

### `stg_campaign_performance`

Daily ad-platform performance per campaign/ad-set, cast from the mock API's raw JSON types (updated_at in particular -- see this model's inline comment).

| Column | Type | Description | Enforced |
|---|---|---|---|
| `campaign_id` | BIGINT | - | not_null |
| `campaign_name` | VARCHAR | - | - |
| `ad_set_id` | BIGINT | - | - |
| `channel` | VARCHAR | - | - |
| `performance_date` | DATE | - | - |
| `impressions` | BIGINT | - | - |
| `clicks` | BIGINT | - | - |
| `conversions` | BIGINT | - | - |
| `spend_eur` | DOUBLE | - | - |
| `updated_at` | TIMESTAMP | Restatement cursor -- the mock API re-surfaces a rotating slice of historical rows with a fresh updated_at to simulate attribution corrections. See docker/mock-api/app.py. | - |

### `stg_clickstream_events`

Deduplicated web/app clickstream (event_id is the dedup key -- defends against at-least-once redelivery from the Kafka consumer, see ingestion/pipelines/kafka_events.py).

| Column | Type | Description | Enforced |
|---|---|---|---|
| `event_id` | VARCHAR | - | not_null, unique |
| `session_id` | VARCHAR | - | - |
| `customer_id` | BIGINT | - | - |
| `event_type` | VARCHAR | - | - |
| `event_ts` | TIMESTAMP | - | - |
| `device_type` | VARCHAR | - | - |
| `referrer` | VARCHAR | - | - |
| `product_sku` | VARCHAR | - | - |
| `order_id` | DOUBLE | Populated only on 'purchase' events for converting sessions -- a REAL join to a live order (see seeders/seed_kafka_events.py), not a fabricated relationship. | - |
| `event_date` | VARCHAR | - | - |

### `stg_customer_identity_map`

original_customer_id -> canonical_customer_id lookup, unnested from stg_customers.source_customer_ids. Used to remap fact-table customer_id references generated against the pre-resolution id space -- see this model's own inline docstring for why it exists.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `original_customer_id` | BIGINT | - | - |
| `canonical_customer_id` | BIGINT | - | - |

### `stg_customers`

One row per resolved real-world person (see spark_jobs/bronze_to_silver/customers.py for the identity-resolution logic).

| Column | Type | Description | Enforced |
|---|---|---|---|
| `customer_id` | BIGINT | Canonical customer identity -- the earliest customer_id among any duplicate identities merged into this person. | relationships -> ref('stg_customers').customer_id, not_null, unique |
| `first_name` | VARCHAR | - | - |
| `last_name` | VARCHAR | - | - |
| `email` | VARCHAR | Normalised (lower/trim) email, unique per resolved person. | not_null, unique |
| `country` | VARCHAR | - | - |
| `city` | VARCHAR | - | - |
| `signup_date` | DATE | - | - |
| `is_marketing_opt_in` | BOOLEAN | - | - |
| `merged_identity_count` | INTEGER | How many original OLTP customer_ids collapsed into this one row. 1 = no duplication found. | - |
| `source_customer_ids` | BIGINT[] | Every original customer_id that resolved to this canonical identity (array). Feeds stg_customer_identity_map. | - |

### `stg_order_items`

Line-item grain -- one row per product per order.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `order_item_id` | BIGINT | - | not_null, unique |
| `order_id` | BIGINT | - | not_null |
| `product_id` | BIGINT | Natural product key (not the SCD2 product_key) -- resolved to the correct historical version downstream in fct_order_items via a point-in-time join. | not_null |
| `quantity` | BIGINT | - | - |
| `unit_price_eur` | DECIMAL(4,2) | - | - |
| `discount_pct` | DECIMAL(4,2) | - | - |
| `line_total_eur` | DOUBLE | quantity * unit_price_eur * (1 - discount_pct / 100), rounded to 2dp. | - |

### `stg_orders`

customer_id remapped through stg_customer_identity_map (see that model) so every order resolves to a customer_id that actually has a row in stg_customers.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `order_id` | BIGINT | - | not_null, unique, relationships -> ref('stg_orders').order_id, relationships -> ref('stg_orders').order_id |
| `customer_id` | BIGINT | Canonical customer_id (post identity-resolution remap). | not_null |
| `store_id` | BIGINT | - | not_null |
| `channel` | VARCHAR | - | - |
| `order_status` | VARCHAR | - | accepted_values ['completed', 'shipped', 'cancelled', 'refunded'] |
| `currency` | VARCHAR | - | - |
| `order_ts` | TIMESTAMP WITH TIME ZONE | Business event time -- when the order actually happened. | - |
| `order_date` | DATE | - | - |
| `created_at` | TIMESTAMP WITH TIME ZONE | System insert time -- when this row first appeared in the OLTP database. Diverges from order_ts for ~1% of in-store orders (late-arriving facts, batch-synced tills) -- see docs/incident-log.md #2. | - |
| `updated_at` | TIMESTAMP WITH TIME ZONE | - | - |
| `order_month` | VARCHAR | - | - |

### `stg_payments`

One or more rows per order (a small fraction of orders split across 2 payments -- see seeders/seed_postgres_oltp.py). sum(amount_eur) per order_id reconciles exactly to sum(line_total_eur) in stg_order_items by construction of the seed data -- enforced by tests/assert_payments_reconcile_to_order_items.sql and spark_jobs/quality_gate.py.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `payment_id` | BIGINT | - | not_null, unique |
| `order_id` | BIGINT | - | not_null |
| `payment_method` | VARCHAR | - | - |
| `amount_eur` | DECIMAL(5,2) | - | - |
| `payment_status` | VARCHAR | - | - |
| `paid_at` | TIMESTAMP WITH TIME ZONE | - | - |

### `stg_pos_inventory`

Cleaned POS inventory snapshots -- the payoff of the flat-file schema-drift story (see docs/incident-log.md #1): unit_cost_eur/quantity_on_hand coerced back to real numerics regardless of which of the 4 source schema-drift stages a given row came from.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `store_id` | BIGINT | - | not_null |
| `product_sku` | VARCHAR | - | - |
| `snapshot_date` | DATE | - | - |
| `quantity_on_hand` | INTEGER | NULL is a genuine, expected value here (~2% of rows -- simulated till-export truncation), not a parse failure. See spark_jobs/quality_gate.py's pos_inventory check for how the two are distinguished. | - |
| `unit_cost_eur` | DECIMAL(10,2) | - | - |
| `reorder_point` | BIGINT | - | - |
| `drop_year_month` | VARCHAR | - | - |

### `stg_products`

SCD2 -- multiple rows per product_id over time, one per tracked price/cost/status version. Uniqueness is on (product_id, valid_from), not product_id alone.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `product_id` | BIGINT | Natural product key -- NOT unique in this table by design (SCD2). Use dim_products.product_key downstream for a unique row identifier. | not_null |
| `sku` | VARCHAR | - | - |
| `product_name` | VARCHAR | - | - |
| `category` | VARCHAR | - | - |
| `subcategory` | VARCHAR | - | - |
| `brand` | VARCHAR | - | - |
| `unit_cost_eur` | DECIMAL(4,2) | Cost basis for THIS version only -- valid for valid_from <= date < valid_to. | - |
| `unit_price_eur` | DECIMAL(4,2) | - | - |
| `is_active` | BOOLEAN | - | - |
| `valid_from` | DATE | Inclusive start of this version's validity. Backdated to the product's real created_at on its first version, not the date the SCD2 pipeline first ran -- see products_scd2.py's docstring for why that distinction matters. | - |
| `valid_to` | VARCHAR | Exclusive end of this version's validity. NULL means still current. | - |
| `is_current` | BOOLEAN | True for exactly one row per product_id -- the currently-active version. | not_null |
| `_silver_processed_at` | TIMESTAMP WITH TIME ZONE | - | - |

### `stg_stores`

150 stores (1 online + 149 physical) -- passthrough of silver.stores.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `store_id` | BIGINT | Store identity, stable for the store's lifetime (no SCD2 -- store attributes don't change in this dataset). | relationships -> ref('stg_stores').store_id, not_null, unique |
| `store_name` | VARCHAR | - | - |
| `channel` | VARCHAR | 'online' (1 store) or 'physical' (149 stores). | - |
| `country` | VARCHAR | - | - |
| `city` | VARCHAR | - | - |
| `opened_date` | DATE | - | - |

## Marts (gold: dims, facts, measures)

### `dim_customers`

One row per identity-resolved real person. Gold-layer passthrough of stg_customers with a friendlier column set for BI (see powerbi/model/tables/DimCustomers.tmdl).

| Column | Type | Description | Enforced |
|---|---|---|---|
| `customer_id` | BIGINT | Canonical customer identity. | not_null, unique |
| `first_name` | VARCHAR | - | - |
| `last_name` | VARCHAR | - | - |
| `email` | VARCHAR | - | - |
| `country` | VARCHAR | - | - |
| `city` | VARCHAR | - | - |
| `signup_date` | DATE | - | - |
| `is_marketing_opt_in` | BOOLEAN | - | - |
| `is_resolved_duplicate_identity` | BOOLEAN | True for the ~3,000 customers whose row absorbed at least one duplicate identity during Spark-side identity resolution. | - |

### `dim_date`

Generated date dimension, 2024-01-01 to 2027-01-01 (dbt_utils.date_spine) -- doesn't come from any of the 4 sources, deliberately derived rather than sourced.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `date_day` | DATE | - | not_null, unique |
| `year` | BIGINT | - | - |
| `month` | BIGINT | - | - |
| `day_of_month` | BIGINT | - | - |
| `day_of_week` | BIGINT | - | - |
| `year_month` | VARCHAR | - | - |
| `month_name` | VARCHAR | - | - |
| `day_name` | VARCHAR | - | - |
| `is_weekend` | BOOLEAN | - | - |

### `dim_products`

SCD2 -- product_key is the row-level unique key, product_id repeats across versions by design. See spark_jobs/bronze_to_silver/products_scd2.py for the Delta MERGE that builds this history, and powerbi/model/roles.tmdl for how this table's Country-adjacent sibling (dim_stores) drives Power BI RLS.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `product_key` | VARCHAR | Surrogate key = hash(product_id, _silver_processed_at) -- see this model's inline comment for why _silver_processed_at, not valid_from, was needed for genuine uniqueness. | not_null, unique |
| `product_id` | BIGINT | Natural product key -- NOT unique in this table (SCD2, multiple versions per product). | not_null |
| `sku` | VARCHAR | - | - |
| `product_name` | VARCHAR | - | - |
| `category` | VARCHAR | - | - |
| `subcategory` | VARCHAR | - | - |
| `brand` | VARCHAR | - | - |
| `unit_cost_eur` | DECIMAL(4,2) | - | - |
| `unit_price_eur` | DECIMAL(4,2) | - | - |
| `is_active` | BOOLEAN | - | - |
| `valid_from` | DATE | - | - |
| `valid_to` | DATE | - | - |
| `is_current` | BOOLEAN | True for exactly one version per product_id. | - |

### `dim_stores`

150 stores (1 online + 149 physical).

| Column | Type | Description | Enforced |
|---|---|---|---|
| `store_id` | BIGINT | - | not_null, unique |
| `store_name` | VARCHAR | - | - |
| `channel` | VARCHAR | - | - |
| `country` | VARCHAR | - | - |
| `city` | VARCHAR | - | - |
| `opened_date` | DATE | - | - |

### `fct_campaign_performance`

The one INCREMENTAL dbt model in this project (deliberately singular -- see this model's docstring for why). Daily ad-platform performance with derived CTR/CVR/cost-per-conversion.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `campaign_id` | BIGINT | - | not_null |
| `campaign_name` | VARCHAR | - | - |
| `ad_set_id` | BIGINT | - | - |
| `channel` | VARCHAR | - | - |
| `performance_date` | DATE | - | - |
| `impressions` | BIGINT | - | - |
| `clicks` | BIGINT | - | - |
| `conversions` | BIGINT | - | - |
| `spend_eur` | DOUBLE | - | - |
| `ctr` | DOUBLE | - | - |
| `cvr` | DOUBLE | - | - |
| `cost_per_conversion_eur` | DOUBLE | - | - |
| `updated_at` | TIMESTAMP | - | - |

### `fct_clickstream_sessions`

Session-grain funnel rollup. order_id is a REAL join to fct_orders for converting sessions (seeders/seed_kafka_events.py built converting sessions FROM live orders), not a fabricated relationship.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `session_id` | VARCHAR | - | not_null, unique |
| `customer_id` | BIGINT | - | - |
| `device_type` | VARCHAR | - | - |
| `referrer` | VARCHAR | - | - |
| `session_start_ts` | TIMESTAMP | - | - |
| `session_end_ts` | TIMESTAMP | - | - |
| `session_duration_seconds` | BIGINT | - | - |
| `event_count` | BIGINT | - | - |
| `page_view_count` | BIGINT | - | - |
| `product_view_count` | BIGINT | - | - |
| `add_to_cart_count` | BIGINT | - | - |
| `checkout_start_count` | BIGINT | - | - |
| `purchase_count` | BIGINT | - | - |
| `converted` | BOOLEAN | - | - |
| `order_id` | DOUBLE | NULL for non-converting sessions. | - |

### `fct_order_items`

Line-item grain sales fact -- the main analytical fact table. Joins to dim_products on product_key via a point-in-time match (order_date within [valid_from, valid_to)) so margin uses the cost that was actually current on the sale date. See this model's own docstring for the full reasoning, including the documented earliest-version fallback for orders that predate a product's tracked SCD2 history.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `order_item_id` | BIGINT | - | not_null, unique |
| `order_id` | BIGINT | - | not_null |
| `customer_id` | BIGINT | - | - |
| `store_id` | BIGINT | - | - |
| `product_key` | VARCHAR | - | - |
| `product_id` | BIGINT | - | - |
| `order_date` | DATE | - | - |
| `order_month` | VARCHAR | - | - |
| `quantity` | BIGINT | - | - |
| `unit_price_eur` | DECIMAL(4,2) | - | - |
| `discount_pct` | DECIMAL(4,2) | - | - |
| `line_total_eur` | DOUBLE | - | - |
| `unit_cost_eur_at_sale` | DECIMAL(4,2) | Cost basis from the SCD2-correct product version as of order_date -- NOT necessarily today's cost. | - |
| `cost_total_eur` | DECIMAL(23,2) | - | - |
| `gross_margin_eur` | DOUBLE | line_total_eur - (quantity * unit_cost_eur_at_sale). | not_null |
| `used_earliest_version_fallback` | BOOLEAN | True when this line's cost came from a product's earliest tracked SCD2 version rather than the version genuinely current on order_date, because the order predates the product's earliest known valid_from -- a documented data-generation gap (seeders/seed_postgres_oltp.py never constrained order_item product selection by the product's own creation date), not a pipeline defect. ~40% of rows. See docs/BUILD_LOG.md. | - |

### `fct_orders`

Order grain -- one row per order, pre-aggregated from fct_order_items and stg_payments for dashboards that don't need line-level detail.

| Column | Type | Description | Enforced |
|---|---|---|---|
| `order_id` | BIGINT | - | relationships -> ref('fct_orders').order_id, not_null, unique, relationships -> ref('fct_orders').order_id |
| `customer_id` | BIGINT | - | - |
| `store_id` | BIGINT | - | - |
| `channel` | VARCHAR | - | - |
| `order_status` | VARCHAR | - | - |
| `currency` | VARCHAR | - | - |
| `order_date` | DATE | - | - |
| `order_month` | VARCHAR | - | - |
| `item_count` | BIGINT | - | - |
| `total_quantity` | HUGEINT | - | - |
| `gross_line_total_eur` | DOUBLE | - | - |
| `total_paid_eur` | DECIMAL(38,2) | - | - |
| `payment_count` | BIGINT | - | - |
