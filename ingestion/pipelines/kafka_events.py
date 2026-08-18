"""
COPY ACTIVITY: Redpanda clickstream topic -> bronze.

    linked_service : Redpanda (in-network broker, see ingestion/config.py)
    activity       : Copy, consumer-group offsets as the watermark
    sink           : s3://lakehouse/bronze/clickstream_events

This is the "streaming / near-real-time ingestion alongside batch"
requirement, implemented as a bounded micro-batch consumer triggered on
an Airflow schedule (e.g. every 5 minutes), not a standalone
always-running Spark Structured Streaming job. That's a deliberate
choice, not a shortcut: Airflow-scheduled micro-batching is a completely
standard way to get near-real-time freshness out of a batch orchestrator
without running a second class of long-lived service -- the same
distinction Databricks draws between Structured Streaming's
"continuous" vs "triggered (available-now)" modes.

The consumer group's committed offsets ARE the watermark here -- unlike
the other three pipelines, no entry in ingestion.pipeline_watermarks is
needed; Kafka already tracks "how far this consumer group has read" per
partition, which is exactly the cursor a copy activity needs.

Correctness detail that matters: `enable_auto_commit=False`, and the
offset commit happens only AFTER the batch is durably written to bronze.
Committing first (or auto-committing) risks acknowledging messages that
never made it to a Delta table if the write fails between poll and
write -- this ordering is what makes the pipeline at-least-once rather
than best-effort.
"""
from __future__ import annotations

import json
import time

import pandas as pd
from kafka import KafkaConsumer

from ingestion.config import REDPANDA_BROKER, REDPANDA_TOPIC_EVENTS
from ingestion.delta_writer import write_bronze

PIPELINE_NAME = "kafka_clickstream_events"
CONSUMER_GROUP = "bronze-ingestion-clickstream"
KAFKA_API_VERSION = (2, 8, 0)  # see seeders/seed_kafka_events.py for why this is pinned against Redpanda
MAX_POLL_SECONDS = 30
POLL_TIMEOUT_MS = 5000
MAX_BATCH_ROWS = 200_000


def run() -> dict:
    consumer = KafkaConsumer(
        REDPANDA_TOPIC_EVENTS,
        bootstrap_servers=REDPANDA_BROKER,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        api_version=KAFKA_API_VERSION,
        consumer_timeout_ms=POLL_TIMEOUT_MS,
    )

    events: list[dict] = []
    start = time.time()

    try:
        while time.time() - start < MAX_POLL_SECONDS and len(events) < MAX_BATCH_ROWS:
            batch = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=5000)
            if not batch:
                break  # caught up -- nothing waiting, don't burn the rest of the window
            for records in batch.values():
                events.extend(r.value for r in records)

        if not events:
            print(f"[{PIPELINE_NAME}] no new events (consumer group already caught up)")
            return {"pipeline": PIPELINE_NAME, "rows": 0}

        df = pd.DataFrame(events)
        # format="ISO8601" rather than a fixed strptime format: producers
        # aren't guaranteed to be perfectly consistent (caught a real
        # case of this -- see docs/BUILD_LOG.md -- where a seeder bug
        # emitted bare "2026-07-26" dates instead of full timestamps for
        # a slice of events; a rigid format string turned that into a
        # hard pipeline crash instead of a parseable, if degenerate,
        # timestamp). Bronze should absorb producer inconsistency where
        # it safely can, same principle as the flat-file pipeline.
        df["event_date"] = pd.to_datetime(df["event_ts"], format="ISO8601", utc=True).dt.strftime("%Y-%m-%d")
        rows_written = write_bronze(
            df, "clickstream_events", source_pipeline=PIPELINE_NAME,
            mode="append", schema_mode="merge", partition_by=["event_date"],
        )

        # Only commit offsets once the batch is durably in bronze -- see module docstring.
        consumer.commit()
        print(f"[{PIPELINE_NAME}] wrote {rows_written:,} events, offsets committed")
        return {"pipeline": PIPELINE_NAME, "rows": rows_written}
    finally:
        consumer.close()


if __name__ == "__main__":
    print(run())
