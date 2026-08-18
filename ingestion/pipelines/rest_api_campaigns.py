"""
COPY ACTIVITY: mock ad-platform REST API -> bronze.

    linked_service : mock-api (API key auth, see ingestion/config.py)
    activity       : Copy, paginated, with explicit retry/backoff
    sink           : s3://lakehouse/bronze/campaign_performance

This is the pipeline the brief specifically wants "proper retry and
incremental fetch logic" for, so the retry/backoff logic is written out
explicitly here rather than delegated to a generic HTTP-adapter retry
policy -- the whole point is that this is inspectable, not a black box.

Two distinct failure modes, two distinct responses (see docker/mock-api/app.py
for why the API produces both):

  * 429 (rate limited): NOT a retriable error in the usual sense -- the
    server tells us exactly how long to wait via `Retry-After`, so we
    sleep for exactly that long and retry the same page. No backoff
    needed; the server already told us the answer.
  * 500 (transient upstream error): genuinely unpredictable, so
    exponential backoff with jitter, capped at MAX_RETRIES, and the
    pipeline fails loudly (not silently skips a page) if a page never
    recovers -- a silently-dropped page is worse than a failed run.

Incremental sync: `updated_since=<watermark>` on every request. The
watermark advances to "now" (the run's start time) only after every page
has been swept successfully -- not to the max `updated_at` seen in the
data, because the mock API's restatement behaviour (see the API's own
docstring) means a row's `updated_at` can be earlier than another row
already seen in the same run. Using wall-clock run-start time as the new
watermark is the correct, simpler invariant: "as of this run, we have
everything that existed as of when it started."
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from ingestion.config import MOCK_API_BASE_URL, MOCK_API_KEY
from ingestion.control_table import get_watermark, set_watermark
from ingestion.delta_writer import write_bronze

PIPELINE_NAME = "rest_api_campaigns"
PAGE_SIZE = 500
MAX_RETRIES = 6
BACKOFF_BASE_SECONDS = 1.0
FLUSH_EVERY_ROWS = 20_000
DEFAULT_WATERMARK = "1970-01-01T00:00:00+00:00"


MAX_RATE_LIMIT_WAITS = 100  # sanity cap only -- see note below on why this isn't MAX_RETRIES


def _get_page(session: requests.Session, page: int, updated_since: str) -> dict:
    """
    Two independent retry budgets, not one shared counter -- this is a
    fix, not the original design. An earlier version used a single `for
    attempt in range(MAX_RETRIES)` loop for both 429s and 5xxs, on the
    theory that `continue`-ing on a 429 "didn't count" against the
    budget. It does: `continue` still advances a `for` loop's counter on
    its next pass, so the two failure modes were silently sharing one
    small budget. That's wrong for a different reason for each:

      * 429 is not a failure, it's the server telling you exactly how
        long to wait. A full historical sync against a 60 req/min limit
        (this run makes >2,000 page requests) is GOING to get rate
        limited repeatedly and routinely -- that's not a fault
        condition, it's the shape of the job. Bounding it at
        MAX_RETRIES=6 meant the pipeline died partway through every
        real backfill. Caught directly: see docs/BUILD_LOG.md for the
        run that hit `RuntimeError: page 59 failed after 6 attempts`
        while every one of those "attempts" was a clean 429 the server
        told us exactly how to resolve.
      * 500 IS a failure condition and needs a tight, bounded budget --
        a page that's still failing after 6 exponential-backoff
        attempts against a genuinely broken upstream should stop the
        pipeline loudly, not spin forever.

    So: rate-limit waits get their own high sanity cap (MAX_RATE_LIMIT_WAITS,
    not a "this is broken" threshold), transient-error attempts get the
    tight MAX_RETRIES budget, and they no longer share state.
    """
    url = f"{MOCK_API_BASE_URL}/v1/campaigns/performance"
    params = {"page": page, "page_size": PAGE_SIZE, "updated_since": updated_since}
    headers = {"X-API-Key": MOCK_API_KEY}

    rate_limit_waits = 0
    transient_attempts = 0

    while True:
        resp = session.get(url, params=params, headers=headers, timeout=15)

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 429:
            rate_limit_waits += 1
            if rate_limit_waits > MAX_RATE_LIMIT_WAITS:
                raise RuntimeError(f"page {page}: rate limited {rate_limit_waits} times in a row -- giving up as a sanity check, not a real retry budget")
            retry_after = int(resp.headers.get("Retry-After", "5"))
            print(f"  page {page}: rate limited, sleeping {retry_after}s (server-directed, not backoff -- doesn't count against MAX_RETRIES)")
            time.sleep(retry_after)
            continue

        if 500 <= resp.status_code < 600:
            transient_attempts += 1
            if transient_attempts > MAX_RETRIES:
                raise RuntimeError(f"page {page} failed after {MAX_RETRIES} attempts against transient errors")
            sleep_s = BACKOFF_BASE_SECONDS * (2 ** (transient_attempts - 1)) + random.uniform(0, 0.5)
            print(f"  page {page}: {resp.status_code} transient error, attempt {transient_attempts}/{MAX_RETRIES}, backing off {sleep_s:.1f}s")
            time.sleep(sleep_s)
            continue

        resp.raise_for_status()  # anything else (4xx auth/validation) is a real failure, fail fast


def run() -> dict:
    watermark = get_watermark(PIPELINE_NAME, default=DEFAULT_WATERMARK)
    run_started_at = datetime.now(timezone.utc).isoformat()
    print(f"[{PIPELINE_NAME}] syncing updated_since={watermark}")

    session = requests.Session()
    buffer: list[dict] = []
    total_rows = 0
    page = 1

    while True:
        body = _get_page(session, page, watermark)
        buffer.extend(body["data"])
        meta = body["meta"]
        print(f"  page {meta['page']}/{meta['total_pages']}  ({len(body['data'])} rows)")

        if len(buffer) >= FLUSH_EVERY_ROWS:
            total_rows += write_bronze(
                pd.DataFrame(buffer), "campaign_performance", source_pipeline=PIPELINE_NAME,
                mode="append", schema_mode="merge", partition_by=["channel"],
            )
            buffer = []

        if meta["next_page"] is None:
            break
        page = meta["next_page"]

    if buffer:
        total_rows += write_bronze(
            pd.DataFrame(buffer), "campaign_performance", source_pipeline=PIPELINE_NAME,
            mode="append", schema_mode="merge", partition_by=["channel"],
        )

    set_watermark(PIPELINE_NAME, run_started_at, status="success", rows=total_rows)
    print(f"[{PIPELINE_NAME}] done: {total_rows:,} rows, watermark -> {run_started_at}")
    return {"pipeline": PIPELINE_NAME, "rows": total_rows, "watermark": run_started_at}


if __name__ == "__main__":
    import json

    print(json.dumps(run(), indent=2, default=str))
