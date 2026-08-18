"""
SOURCE 1: Postgres OLTP -- Fjord Mart's transactional system of record.

Generates and bulk-loads:
    customers    ~153,000  (incl. ~2% deliberate near-duplicates -- silver
                             layer has to dedupe these, see docs/incident-log.md)
    stores            150  (1 online "store" + 149 physical, IE/GB/DE/NL/FR/ES)
    products       20,000
    orders      2,000,000  (2 years of trading history)
    order_items ~5,500,000 (avg 2.75 lines/order)
    payments    ~2,060,000 (~3% of orders split across two payments)
                ----------
                ~9.73M rows in this source alone.

Design choices worth defending in an interview:

  * Bulk load via COPY, not row-by-row INSERT / ORM. At millions of rows,
    COPY FROM STDIN is roughly an order of magnitude faster than batched
    INSERTs, and it's what a real initial-load or backfill job would use.
  * PKs, FKs and indexes are added AFTER the load (add_constraints()),
    not declared up front. Building a unique/FK index once over the full
    dataset is much cheaper than maintaining it incrementally through
    ~9.7M row-by-row COPY inserts -- the standard bulk-load pattern.
  * `orders.order_ts` (business event time) vs `orders.created_at`
    (system insert time) are deliberately allowed to diverge for ~1% of
    rows by several days -- that's the seed of the "late-arriving fact"
    interview scenario: a naive incremental extract keyed on order_ts
    would silently miss these; one keyed on created_at/updated_at won't.
  * `payments.amount_eur` is derived FROM the generated order_items, not
    generated independently, so "sum(payments) == sum(order line totals)
    per order" is a true invariant of the dataset -- and becomes a real
    dbt singular test in the gold layer, not a fact I have to fake.

Run: `python seeders/seed_postgres_oltp.py` (needs the `sources` compose
profile up: `docker compose up -d postgres`).
"""
from __future__ import annotations

import io
import time

import numpy as np
import pandas as pd
import psycopg2
from faker import Faker

from common import (
    BRANDS,
    CITIES_BY_COUNTRY,
    COUNTRIES,
    COUNTRY_WEIGHTS,
    HISTORY_DAYS,
    HISTORY_START,
    ORDER_STATUSES,
    PAYMENT_METHODS,
    PG_HOST,
    PG_OLTP_DB,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    PRODUCT_CATEGORIES,
    SEED,
    progress,
)

N_CUSTOMERS_BASE = 150_000
CUSTOMER_DUPLICATE_RATE = 0.02  # ~2% guest-checkout-then-registered duplicates
N_STORES = 150
N_PRODUCTS = 20_000
N_ORDERS = 2_000_000
LATE_ARRIVAL_RATE = 0.01  # ~1% of orders land in the OLTP DB days after order_ts
SPLIT_PAYMENT_RATE = 0.03

rng = np.random.default_rng(SEED)
Faker.seed(SEED)
fake = Faker()


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_OLTP_DB, user=PG_USER, password=PG_PASSWORD
    )


# --------------------------------------------------------------------- #
# Schema (constraint-free on purpose -- see module docstring)
# --------------------------------------------------------------------- #
DDL = """
DROP TABLE IF EXISTS oltp.payments CASCADE;
DROP TABLE IF EXISTS oltp.order_items CASCADE;
DROP TABLE IF EXISTS oltp.orders CASCADE;
DROP TABLE IF EXISTS oltp.products CASCADE;
DROP TABLE IF EXISTS oltp.stores CASCADE;
DROP TABLE IF EXISTS oltp.customers CASCADE;

CREATE TABLE oltp.customers (
    customer_id           BIGINT,
    first_name            TEXT,
    last_name             TEXT,
    email                 TEXT,
    country                CHAR(2),
    city                  TEXT,
    signup_date           DATE,
    is_marketing_opt_in   BOOLEAN,
    created_at            TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ
);

CREATE TABLE oltp.stores (
    store_id     INT,
    store_name   TEXT,
    channel      TEXT,
    country      CHAR(2),
    city         TEXT,
    opened_date  DATE
);

CREATE TABLE oltp.products (
    product_id      BIGINT,
    sku             TEXT,
    product_name    TEXT,
    category        TEXT,
    subcategory     TEXT,
    brand           TEXT,
    unit_cost_eur   NUMERIC(10,2),
    unit_price_eur  NUMERIC(10,2),
    is_active       BOOLEAN,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);

CREATE TABLE oltp.orders (
    order_id      BIGINT,
    customer_id   BIGINT,
    store_id      INT,
    channel       TEXT,
    order_status  TEXT,
    currency      TEXT,
    order_ts      TIMESTAMPTZ,
    created_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ
);

CREATE TABLE oltp.order_items (
    order_item_id   BIGINT,
    order_id        BIGINT,
    product_id      BIGINT,
    quantity        INT,
    unit_price_eur  NUMERIC(10,2),
    discount_pct    NUMERIC(5,2)
);

CREATE TABLE oltp.payments (
    payment_id      BIGINT,
    order_id        BIGINT,
    payment_method  TEXT,
    amount_eur      NUMERIC(10,2),
    payment_status  TEXT,
    paid_at         TIMESTAMPTZ
);
"""

CONSTRAINTS_SQL = """
ALTER TABLE oltp.customers ADD PRIMARY KEY (customer_id);
ALTER TABLE oltp.stores ADD PRIMARY KEY (store_id);
ALTER TABLE oltp.products ADD PRIMARY KEY (product_id);
ALTER TABLE oltp.orders ADD PRIMARY KEY (order_id);
ALTER TABLE oltp.order_items ADD PRIMARY KEY (order_item_id);
ALTER TABLE oltp.payments ADD PRIMARY KEY (payment_id);

CREATE INDEX ix_customers_email ON oltp.customers (email);
CREATE INDEX ix_customers_updated_at ON oltp.customers (updated_at);

CREATE INDEX ix_orders_customer_id ON oltp.orders (customer_id);
CREATE INDEX ix_orders_store_id ON oltp.orders (store_id);
CREATE INDEX ix_orders_order_ts ON oltp.orders (order_ts);
-- the index an incremental extractor actually needs: watermark scans on
-- created_at/updated_at, not on the business-date column.
CREATE INDEX ix_orders_created_at ON oltp.orders (created_at);
CREATE INDEX ix_orders_updated_at ON oltp.orders (updated_at);

CREATE INDEX ix_order_items_order_id ON oltp.order_items (order_id);
CREATE INDEX ix_order_items_product_id ON oltp.order_items (product_id);

CREATE INDEX ix_payments_order_id ON oltp.payments (order_id);

ALTER TABLE oltp.orders
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES oltp.customers (customer_id),
    ADD CONSTRAINT fk_orders_store FOREIGN KEY (store_id) REFERENCES oltp.stores (store_id);

ALTER TABLE oltp.order_items
    ADD CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES oltp.orders (order_id),
    ADD CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES oltp.products (product_id);

ALTER TABLE oltp.payments
    ADD CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES oltp.orders (order_id);
"""


def bulk_copy(conn, df: pd.DataFrame, table: str, columns: list[str], chunk_size: int = 500_000, label: str = "") -> None:
    total = len(df)
    start = time.time()
    with conn.cursor() as cur:
        for i in range(0, total, chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            buf = io.StringIO()
            chunk.to_csv(buf, index=False, header=False, columns=columns, na_rep="\\N")
            buf.seek(0)
            cur.copy_expert(
                f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '\\N')",
                buf,
            )
            conn.commit()
            progress(label or table, min(i + chunk_size, total), total, start)


def random_dates(n: int, start_date, days: int, growth_bias: bool = True) -> np.ndarray:
    """Days-since-start, optionally weighted so more volume falls in recent
    months (a growing retailer), not uniform -- cheap realism for the
    time-series charts this feeds in Power BI."""
    if growth_bias:
        weights = np.linspace(0.6, 1.4, days)
        weights = weights / weights.sum()
        offsets = rng.choice(days, size=n, p=weights)
    else:
        offsets = rng.integers(0, days, size=n)
    return offsets


# --------------------------------------------------------------------- #
# Dimension generation
# --------------------------------------------------------------------- #
def generate_customers() -> pd.DataFrame:
    n = N_CUSTOMERS_BASE
    countries = rng.choice(COUNTRIES, size=n, p=COUNTRY_WEIGHTS)
    cities = np.array([rng.choice(CITIES_BY_COUNTRY[c]) for c in countries])
    first_names = [fake.first_name() for _ in range(n)]
    last_names = [fake.last_name() for _ in range(n)]
    emails = [
        f"{fn.lower()}.{ln.lower()}{i}@{fake.free_email_domain()}"
        for i, (fn, ln) in enumerate(zip(first_names, last_names))
    ]
    signup_offsets = random_dates(n, HISTORY_START, HISTORY_DAYS, growth_bias=True)
    signup_dates = [HISTORY_START + pd.Timedelta(days=int(o)) for o in signup_offsets]
    created_at = [pd.Timestamp(d) + pd.Timedelta(hours=int(rng.integers(0, 24))) for d in signup_dates]

    df = pd.DataFrame(
        {
            "customer_id": np.arange(1, n + 1, dtype=np.int64),
            "first_name": first_names,
            "last_name": last_names,
            "email": emails,
            "country": countries,
            "city": cities,
            "signup_date": signup_dates,
            "is_marketing_opt_in": rng.random(n) < 0.55,
            "created_at": created_at,
            "updated_at": created_at,
        }
    )

    # Deliberate near-duplicates: same person, new customer_id, minor
    # cosmetic differences (whitespace / casing) -- the classic
    # guest-checkout-then-registers-an-account pattern. Silver has to
    # collapse these on a normalised (lower, trimmed) email.
    n_dupes = int(n * CUSTOMER_DUPLICATE_RATE)
    dupe_source_idx = rng.choice(n, size=n_dupes, replace=False)
    dupes = df.iloc[dupe_source_idx].copy()
    dupes["customer_id"] = np.arange(n + 1, n + 1 + n_dupes, dtype=np.int64)
    # cosmetic drift: extra whitespace, inconsistent casing on the email
    dupes["email"] = dupes["email"].apply(
        lambda e: e.upper() if rng.random() < 0.5 else f" {e} "
    )
    dupes["created_at"] = dupes["created_at"] + pd.Timedelta(days=1) + pd.to_timedelta(
        rng.integers(0, 200, size=n_dupes), unit="D"
    )
    dupes["updated_at"] = dupes["created_at"]
    dupes["signup_date"] = dupes["created_at"].dt.date

    return pd.concat([df, dupes], ignore_index=True)


def generate_stores() -> pd.DataFrame:
    rows = [
        {
            "store_id": 1,
            "store_name": "Fjord Mart Online",
            "channel": "online",
            "country": "IE",
            "city": "Dublin",
            "opened_date": HISTORY_START,
        }
    ]
    store_id = 2
    for country, weight in zip(COUNTRIES, COUNTRY_WEIGHTS):
        n_stores_here = max(1, round(weight * (N_STORES - 1)))
        cities = CITIES_BY_COUNTRY[country]
        for i in range(n_stores_here):
            if store_id > N_STORES:
                break
            city = cities[i % len(cities)]
            rows.append(
                {
                    "store_id": store_id,
                    "store_name": f"Fjord Mart {city} {i // len(cities) + 1}",
                    "channel": "physical",
                    "country": country,
                    "city": city,
                    "opened_date": HISTORY_START + pd.Timedelta(days=int(rng.integers(0, HISTORY_DAYS // 2))),
                }
            )
            store_id += 1
    # pad/truncate to exactly N_STORES
    while len(rows) < N_STORES:
        rows.append(dict(rows[-1]))
        rows[-1]["store_id"] = len(rows) + 1
    return pd.DataFrame(rows[:N_STORES])


def generate_products() -> pd.DataFrame:
    n = N_PRODUCTS
    categories = list(PRODUCT_CATEGORIES.keys())
    cat_choice = rng.choice(categories, size=n)
    subcats = np.array([rng.choice(PRODUCT_CATEGORIES[c]) for c in cat_choice])
    brands = rng.choice(BRANDS, size=n)
    sizes = rng.choice(["250g", "500g", "1kg", "1L", "2L", "6pk", "12pk", "single"], size=n)
    product_names = [f"{b} {s} {sz}" for b, s, sz in zip(brands, subcats, sizes)]
    unit_cost = np.round(rng.gamma(shape=2.0, scale=2.5, size=n) + 0.5, 2)
    margin = rng.uniform(1.25, 1.9, size=n)
    unit_price = np.round(unit_cost * margin, 2)
    created_at = [
        pd.Timestamp(HISTORY_START) + pd.Timedelta(days=int(o))
        for o in random_dates(n, HISTORY_START, HISTORY_DAYS, growth_bias=False)
    ]

    return pd.DataFrame(
        {
            "product_id": np.arange(1, n + 1, dtype=np.int64),
            "sku": [f"SKU-{i:06d}" for i in range(1, n + 1)],
            "product_name": product_names,
            "category": cat_choice,
            "subcategory": subcats,
            "brand": brands,
            "unit_cost_eur": unit_cost,
            "unit_price_eur": unit_price,
            "is_active": rng.random(n) < 0.93,
            "created_at": created_at,
            "updated_at": created_at,
        }
    )


# --------------------------------------------------------------------- #
# Fact generation
# --------------------------------------------------------------------- #
def generate_orders(customers: pd.DataFrame, stores: pd.DataFrame) -> pd.DataFrame:
    n = N_ORDERS
    n_customers = len(customers)

    # Zipf-ish repeat-purchase skew: a minority of customers place a
    # disproportionate share of orders (loyalty-card regulars), rather
    # than every customer being equally likely -- realistic and it gives
    # a customer-lifetime-value gold model something to compute.
    zipf_ranks = rng.zipf(a=1.3, size=n)
    customer_idx = (zipf_ranks % n_customers)
    customer_ids = customers["customer_id"].values[customer_idx]
    customer_countries = customers["country"].values[customer_idx]

    online_store_id = 1
    physical_store_ids = stores.loc[stores["channel"] == "physical", "store_id"].values
    is_online = rng.random(n) < 0.55
    store_ids = np.where(
        is_online, online_store_id, rng.choice(physical_store_ids, size=n)
    )
    channel = np.where(is_online, "online", "in_store")

    order_offsets = random_dates(n, HISTORY_START, HISTORY_DAYS, growth_bias=True)
    order_ts = pd.to_datetime(HISTORY_START) + pd.to_timedelta(order_offsets, unit="D") + pd.to_timedelta(
        rng.integers(0, 86400, size=n), unit="s"
    )

    currency = np.where(customer_countries == "GB", "GBP", "EUR")
    status = rng.choice(ORDER_STATUSES, size=n)

    # Normal case: created_at ~= order_ts (system captured it same-day).
    created_at = order_ts + pd.to_timedelta(rng.integers(0, 3600, size=n), unit="s")

    # Late-arriving fact: ~1% of orders (in-store, batch-synced tills are
    # the realistic culprit) are only visible in the OLTP DB days after
    # they actually happened.
    late_mask = (rng.random(n) < LATE_ARRIVAL_RATE) & (channel == "in_store")
    late_delay_days = rng.integers(3, 11, size=n)
    created_at = created_at.where(~late_mask, order_ts + pd.to_timedelta(late_delay_days, unit="D"))

    # A further ~5% get touched again later (status change, e.g. refund
    # processed a week on) -- updated_at moves, order_ts does not.
    touched_mask = rng.random(n) < 0.05
    touch_delay_days = rng.integers(1, 14, size=n)
    updated_at = created_at.where(~touched_mask, created_at + pd.to_timedelta(touch_delay_days, unit="D"))

    # Clamp both to "now" (the moment this script is running). order_ts
    # already has headroom before TODAY (HISTORY_START..TODAY), but the
    # +delay_days nudges above push created_at/updated_at for orders
    # placed near the end of that window PAST the actual present moment
    # -- a timestamp claiming a row was last touched in the future. That
    # doesn't just look odd, it actively breaks anything watermark-based:
    # an incremental extractor's watermark can race ahead of wall-clock
    # "now", so a genuinely new late-arriving row inserted moments later
    # (updated_at = real now) reads as OLDER than the watermark and gets
    # silently skipped. Caught this exact way: see docs/BUILD_LOG.md for
    # the late-arriving-fact demo run that surfaced it.
    now_ceiling = pd.Timestamp.utcnow().tz_localize(None)
    created_at = created_at.where(created_at <= now_ceiling, now_ceiling)
    updated_at = updated_at.where(updated_at <= now_ceiling, now_ceiling)

    return pd.DataFrame(
        {
            "order_id": np.arange(1, n + 1, dtype=np.int64),
            "customer_id": customer_ids,
            "store_id": store_ids,
            "channel": channel,
            "order_status": status,
            "currency": currency,
            "order_ts": order_ts,
            "created_at": created_at,
            "updated_at": updated_at,
        }
    )


def generate_order_items(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    n_orders = len(orders)
    item_counts = rng.poisson(lam=2.75, size=n_orders).clip(min=1)
    n_items = int(item_counts.sum())

    order_id_expanded = np.repeat(orders["order_id"].values, item_counts)

    # Popularity skew on products too (best-sellers), via a Zipf draw
    # mapped into the product_id space. product_id is a contiguous
    # 1..n_products range, so a plain numpy gather (not a pandas .loc
    # lookup) keeps this fast at millions of rows.
    n_products = len(products)
    product_ranks = rng.zipf(a=1.15, size=n_items) % n_products
    product_ids = products["product_id"].values[product_ranks]
    price_by_product_id = products.sort_values("product_id")["unit_price_eur"].to_numpy()
    unit_prices = price_by_product_id[product_ids - 1]

    quantity = rng.choice([1, 1, 1, 2, 2, 3, 4, 5], size=n_items)
    discount_pct = np.where(rng.random(n_items) < 0.12, np.round(rng.uniform(5, 30, n_items), 2), 0.0)

    order_item_id = np.arange(1, n_items + 1, dtype=np.int64)

    return pd.DataFrame(
        {
            "order_item_id": order_item_id,
            "order_id": order_id_expanded,
            "product_id": product_ids,
            "quantity": quantity,
            "unit_price_eur": unit_prices,
            "discount_pct": discount_pct,
        }
    )


def generate_payments(orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    line_total = (
        order_items["quantity"]
        * order_items["unit_price_eur"]
        * (1 - order_items["discount_pct"] / 100.0)
    )
    order_totals = line_total.groupby(order_items["order_id"]).sum().round(2)
    order_totals = order_totals.reindex(orders["order_id"]).fillna(0.0)

    n_orders = len(orders)
    split_mask = rng.random(n_orders) < SPLIT_PAYMENT_RATE
    order_ts = orders["order_ts"].values

    rows = []
    payment_id = 1
    method_choices = np.array(PAYMENT_METHODS)
    methods = rng.choice(method_choices, size=n_orders)
    statuses = np.where(orders["order_status"].values == "refunded", "refunded", "captured")

    totals = order_totals.values
    split_first_share = rng.uniform(0.3, 0.7, size=n_orders)

    order_ids = orders["order_id"].values
    paid_offsets_sec = rng.integers(0, 600, size=n_orders)

    single_idx = ~split_mask
    payment_ids_single = np.arange(payment_id, payment_id + single_idx.sum(), dtype=np.int64)
    payment_id += single_idx.sum()
    single_df = pd.DataFrame(
        {
            "payment_id": payment_ids_single,
            "order_id": order_ids[single_idx],
            "payment_method": methods[single_idx],
            "amount_eur": totals[single_idx],
            "payment_status": statuses[single_idx],
            "paid_at": order_ts[single_idx] + pd.to_timedelta(paid_offsets_sec[single_idx], unit="s"),
        }
    )

    n_split = int(split_mask.sum())
    first_ids = np.arange(payment_id, payment_id + n_split, dtype=np.int64)
    payment_id += n_split
    second_ids = np.arange(payment_id, payment_id + n_split, dtype=np.int64)
    payment_id += n_split

    first_df = pd.DataFrame(
        {
            "payment_id": first_ids,
            "order_id": order_ids[split_mask],
            "payment_method": methods[split_mask],
            "amount_eur": np.round(totals[split_mask] * split_first_share[split_mask], 2),
            "payment_status": statuses[split_mask],
            "paid_at": order_ts[split_mask] + pd.to_timedelta(paid_offsets_sec[split_mask], unit="s"),
        }
    )
    second_df = pd.DataFrame(
        {
            "payment_id": second_ids,
            "order_id": order_ids[split_mask],
            "payment_method": rng.choice(method_choices, size=n_split),
            "amount_eur": np.round(totals[split_mask] * (1 - split_first_share[split_mask]), 2),
            "payment_status": statuses[split_mask],
            "paid_at": order_ts[split_mask] + pd.to_timedelta(paid_offsets_sec[split_mask] + 3600, unit="s"),
        }
    )

    return pd.concat([single_df, first_df, second_df], ignore_index=True)


def main() -> None:
    t0 = time.time()
    conn = get_conn()
    conn.autocommit = False

    print("Creating schema (oltp.*) ...")
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    print("Generating & loading customers ...")
    customers = generate_customers()
    bulk_copy(conn, customers, "oltp.customers", list(customers.columns))

    print("Generating & loading stores ...")
    stores = generate_stores()
    bulk_copy(conn, stores, "oltp.stores", list(stores.columns))

    print("Generating & loading products ...")
    products = generate_products()
    bulk_copy(conn, products, "oltp.products", list(products.columns))

    print("Generating orders ...")
    orders = generate_orders(customers, stores)
    print("Loading orders ...")
    bulk_copy(conn, orders, "oltp.orders", list(orders.columns))

    print("Generating order_items ...")
    order_items = generate_order_items(orders, products)
    print(f"  -> {len(order_items):,} order_items rows")
    print("Loading order_items ...")
    bulk_copy(conn, order_items, "oltp.order_items", list(order_items.columns))

    print("Generating payments ...")
    payments = generate_payments(orders, order_items)
    print(f"  -> {len(payments):,} payments rows")
    print("Loading payments ...")
    bulk_copy(conn, payments, "oltp.payments", list(payments.columns))

    print("Adding primary keys, indexes, foreign keys ...")
    with conn.cursor() as cur:
        cur.execute(CONSTRAINTS_SQL)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("ANALYZE oltp.customers, oltp.stores, oltp.products, oltp.orders, oltp.order_items, oltp.payments;")
    conn.commit()

    total_rows = len(customers) + len(stores) + len(products) + len(orders) + len(order_items) + len(payments)
    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:,.1f}s. Row counts:")
    print(f"  customers   : {len(customers):>12,}")
    print(f"  stores      : {len(stores):>12,}")
    print(f"  products    : {len(products):>12,}")
    print(f"  orders      : {len(orders):>12,}")
    print(f"  order_items : {len(order_items):>12,}")
    print(f"  payments    : {len(payments):>12,}")
    print(f"  TOTAL       : {total_rows:>12,}")

    conn.close()


if __name__ == "__main__":
    main()
