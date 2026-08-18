"""
SOURCE 4: Append-only event stream -- Fjord Mart web/app clickstream,
produced onto Redpanda's `web.clickstream.events` topic (Kafka wire
protocol; see docker-compose.yml for why Redpanda over vanilla Kafka).

Design choices worth defending in an interview:

  * Retention window, not full history. Unlike the OLTP tables (2 years)
    or the flat-file drops (2 years), this only covers the last
    WINDOW_DAYS -- a real clickstream topic has a retention policy (say
    30-90 days), it is NOT a system of record for all time. If you need
    clickstream older than that, you already had to have landed it in
    the lake; that's exactly why bronze exists.

  * Real cross-source join, not a fabricated one. "Converting" sessions
    are built FROM actual online orders already sitting in
    oltp.orders (queried live from Postgres, not re-derived from the
    same RNG seed) -- so `events.order_id` genuinely joins to
    `oltp.orders.order_id`. That gives the silver/gold layer a true
    session-to-purchase attribution story, and gives you a real answer
    when an interviewer asks "how do your sources actually relate to
    each other?" rather than four independent tables that happen to
    share a similar id range.

  * Deliberately out of order. Events are generated in causal order per
    session but the PRODUCE order across sessions is locally shuffled
    (see `locally_shuffle`) -- exactly the kind of clock-skew/retry
    reordering a real HTTP-collector-fed topic exhibits, and the reason
    a bronze consumer keys off event_ts, not offset/arrival order.

Run: `python seeders/seed_kafka_events.py` (needs `docker compose up -d
redpanda` AND the Postgres OLTP seeder to have already run, since
converting sessions are sampled from real oltp.orders rows).
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import psycopg2
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from common import (
    PG_HOST,
    PG_OLTP_DB,
    PG_PASSWORD,
    PG_PORT,
    PG_USER,
    REDPANDA_BROKER,
    REDPANDA_TOPIC_EVENTS,
    SEED,
    TODAY,
)

WINDOW_DAYS = 45
N_CONVERTING_SESSIONS = 40_000   # sampled from real online orders
N_BROWSE_ONLY_SESSIONS = 200_000  # never convert -- funnel drop-off
N_PRODUCTS_TOTAL = 20_000
N_CUSTOMERS_TOTAL = 153_000

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.62, 0.30, 0.08]
REFERRERS = ["direct", "organic_search", "paid_search", "social", "email", "affiliate"]
REFERRER_WEIGHTS = [0.30, 0.22, 0.18, 0.14, 0.11, 0.05]

rng = np.random.default_rng(SEED + 2)


def fetch_converting_orders(n: int) -> list[dict]:
    """Real online orders from the last WINDOW_DAYS, pulled live from
    Postgres -- these become the sessions that end in a `purchase` event
    with a genuinely joinable order_id."""
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_OLTP_DB, user=PG_USER, password=PG_PASSWORD)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT order_id, customer_id, order_ts
                FROM oltp.orders
                WHERE channel = 'online'
                  AND order_ts >= %s
                ORDER BY random()
                LIMIT %s
                """,
                (TODAY - timedelta(days=WINDOW_DAYS), n),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return [{"order_id": r[0], "customer_id": r[1], "order_ts": r[2]} for r in rows]


def make_event(session_id: str, customer_id, event_type: str, event_ts, device: str, referrer: str,
               product_sku: str | None = None, order_id: int | None = None) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "customer_id": int(customer_id) if customer_id is not None else None,
        "event_type": event_type,
        "event_ts": event_ts.isoformat(),
        "device_type": device,
        "referrer": referrer,
        "product_sku": product_sku,
        "order_id": int(order_id) if order_id is not None else None,
    }


def build_session_events(session_id: str, customer_id, end_ts, converting: bool, order_id=None) -> list[dict]:
    device = rng.choice(DEVICE_TYPES, p=DEVICE_WEIGHTS)
    referrer = rng.choice(REFERRERS, p=REFERRER_WEIGHTS)

    n_page_views = int(rng.poisson(3)) + 1
    n_product_views = int(rng.poisson(2)) + (1 if converting else 0)

    # Work backwards from end_ts (order time, or a random browse-session
    # end) so the funnel reads in causal order: browse -> view -> cart ->
    # checkout -> purchase.
    events: list[dict] = []
    cursor = end_ts - timedelta(seconds=int(rng.integers(120, 900)))

    for _ in range(n_page_views):
        events.append(make_event(session_id, customer_id, "page_view", cursor, device, referrer))
        cursor += timedelta(seconds=int(rng.integers(5, 60)))

    for _ in range(n_product_views):
        sku = f"SKU-{int(rng.integers(1, N_PRODUCTS_TOTAL + 1)):06d}"
        events.append(make_event(session_id, customer_id, "product_view", cursor, device, referrer, product_sku=sku))
        cursor += timedelta(seconds=int(rng.integers(5, 90)))

    if converting or rng.random() < 0.20:
        sku = f"SKU-{int(rng.integers(1, N_PRODUCTS_TOTAL + 1)):06d}"
        events.append(make_event(session_id, customer_id, "add_to_cart", cursor, device, referrer, product_sku=sku))
        cursor += timedelta(seconds=int(rng.integers(10, 120)))

    if converting:
        events.append(make_event(session_id, customer_id, "checkout_start", cursor, device, referrer))
        events.append(make_event(session_id, customer_id, "purchase", end_ts, device, referrer, order_id=order_id))

    return events


def locally_shuffle(events: list[dict], window: int = 500) -> list[dict]:
    """Reorders the produce sequence within a small sliding window so the
    stream isn't perfectly time-sorted -- see module docstring."""
    n = len(events)
    idx = np.arange(n)
    for start in range(0, n, window):
        end = min(start + window, n)
        rng.shuffle(idx[start:end])
    return [events[i] for i in idx]


def ensure_topic(admin: KafkaAdminClient) -> None:
    existing = admin.list_topics()
    if REDPANDA_TOPIC_EVENTS in existing:
        return
    admin.create_topics([NewTopic(name=REDPANDA_TOPIC_EVENTS, num_partitions=6, replication_factor=1)])


def main() -> None:
    t0 = time.time()

    print(f"Sampling up to {N_CONVERTING_SESSIONS:,} real online orders from the last {WINDOW_DAYS} days ...")
    converting_orders = fetch_converting_orders(N_CONVERTING_SESSIONS)
    print(f"  -> got {len(converting_orders):,} orders to build converting sessions from")

    print("Building event list ...")
    all_events: list[dict] = []

    for order in converting_orders:
        session_id = str(uuid.uuid4())
        all_events.extend(
            build_session_events(session_id, order["customer_id"], order["order_ts"], converting=True, order_id=order["order_id"])
        )

    # NB: TODAY (from common.py) is a datetime.date. Subtracting/adding
    # timedeltas to a bare date silently drops any sub-day component
    # (date.__add__ only honours timedelta.days) -- an earlier version of
    # this line left window_start as a date, so every downstream
    # `end_ts`/`event_ts` for non-converting sessions collapsed to
    # midnight and serialised as "2026-07-26" instead of a real
    # timestamp, which then broke pd.to_datetime() in the bronze
    # ingestion consumer (ingestion/pipelines/kafka_events.py). Anchoring
    # explicitly to midnight UTC as a real datetime here is the fix.
    window_start = datetime.combine(TODAY, datetime.min.time(), tzinfo=timezone.utc) - timedelta(days=WINDOW_DAYS)
    for _ in range(N_BROWSE_ONLY_SESSIONS):
        session_id = str(uuid.uuid4())
        is_known_customer = rng.random() < 0.6
        customer_id = int(rng.integers(1, N_CUSTOMERS_TOTAL + 1)) if is_known_customer else None
        offset_seconds = int(rng.integers(0, WINDOW_DAYS * 86400))
        end_ts = window_start + timedelta(seconds=offset_seconds)
        all_events.extend(build_session_events(session_id, customer_id, end_ts, converting=False))

    print(f"  -> {len(all_events):,} raw events, shuffling produce order ...")
    all_events = locally_shuffle(all_events)

    print(f"Connecting to Redpanda at {REDPANDA_BROKER} ...")
    # kafka-python-ng's auto version-probe doesn't reliably negotiate
    # against Redpanda; pinning a Kafka protocol version it supports
    # skips the probe and just works.
    KAFKA_API_VERSION = (2, 8, 0)
    admin = KafkaAdminClient(bootstrap_servers=REDPANDA_BROKER, client_id="seeder-admin", api_version=KAFKA_API_VERSION)
    ensure_topic(admin)
    admin.close()

    producer = KafkaProducer(
        bootstrap_servers=REDPANDA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        batch_size=64 * 1024,
        compression_type="gzip",
        api_version=KAFKA_API_VERSION,
    )

    total = len(all_events)
    for i, event in enumerate(all_events, start=1):
        producer.send(REDPANDA_TOPIC_EVENTS, key=event["session_id"], value=event)
        if i % 100_000 == 0 or i == total:
            elapsed = time.time() - t0
            print(f"  produced {i:,}/{total:,}  ({i/elapsed:,.0f} msg/s)")

    producer.flush()
    producer.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:,.1f}s.")
    print(f"  converting sessions   : {len(converting_orders):,}")
    print(f"  browse-only sessions  : {N_BROWSE_ONLY_SESSIONS:,}")
    print(f"  total events produced : {total:,}")
    print(f"  topic                 : {REDPANDA_TOPIC_EVENTS}")


if __name__ == "__main__":
    main()
