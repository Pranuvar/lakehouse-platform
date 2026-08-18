"""
Mock ad-platform REST API -- stand-in for a real third-party marketing API
(think Google Ads / Meta Marketing API shape) used as SOURCE 2 of the
platform's four heterogeneous sources.

This is NOT a toy that just returns a fixed JSON blob. It deliberately
implements the three things that make ingesting a real third-party API
hard, so the ingestion layer (ingestion/pipelines/rest_api_campaigns.py)
has to implement real client-side logic against it:

  1. Pagination      -- `page` / `page_size`, capped page_size, `next_page`
                         in the response envelope.
  2. Rate limiting    -- a fixed per-minute request budget per API key,
                         429 + Retry-After when exceeded (token bucket).
  3. Incremental sync -- `updated_since` cursor filter, PLUS a background
                         "restatement" process: a small slice of historical
                         rows gets a fresh `updated_at` on every request,
                         simulating an ad platform correcting attribution
                         after the fact. A naive "only ever fetch new dates"
                         ingestion job will silently miss these corrections;
                         a correct one re-pulls on `updated_at`, not on the
                         business date.

A transient-failure flag additionally injects random 500s so the client
has to implement retry/backoff, not just pagination.

The dataset is generated once at startup with numpy (no pandas dependency,
keeps the image and the container's memory footprint small) and held in
process memory -- this is a mock, not a database, on purpose.
"""
from __future__ import annotations

import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
from fastapi import FastAPI, Header, HTTPException, Query, Response
from pydantic import BaseModel

# --------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------- #
API_KEY = os.environ.get("MOCK_API_KEY", "dev-local-key-do-not-use-in-prod")
RATE_LIMIT_PER_MIN = int(os.environ.get("RATE_LIMIT_PER_MIN", "60"))
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.03"))  # ~3% of requests -> 500
MAX_PAGE_SIZE = 1000
DEFAULT_PAGE_SIZE = 200

N_CAMPAIGNS = 500
AD_SETS_PER_CAMPAIGN = 3
DAYS_OF_HISTORY = 730  # ~2 years, ends "today" (container start date)
RESTATEMENT_FRACTION = 0.005  # ~0.5% of rows look "just updated" each request

CHANNELS = np.array(
    ["google_search", "google_display", "meta_feed", "instagram_story",
     "tiktok", "linkedin", "email", "affiliate"]
)

RNG_SEED = 42

app = FastAPI(title="Mock Ad Platform API", version="1.0.0")


# --------------------------------------------------------------------- #
# Deterministic synthetic dataset, built once at startup.
# One row per (campaign, ad_set, date). ~500 * 3 * 730 = 1,095,000 rows.
# --------------------------------------------------------------------- #
class CampaignPerformanceStore:
    def __init__(self) -> None:
        rng = np.random.default_rng(RNG_SEED)
        n_campaigns = N_CAMPAIGNS
        n_ad_sets = AD_SETS_PER_CAMPAIGN
        n_days = DAYS_OF_HISTORY

        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=n_days - 1)
        dates = np.array([start_date + timedelta(days=i) for i in range(n_days)])

        campaign_ids = np.repeat(np.arange(1, n_campaigns + 1), n_ad_sets * n_days)
        ad_set_offsets = np.tile(np.repeat(np.arange(1, n_ad_sets + 1), n_days), n_campaigns)
        date_idx = np.tile(np.arange(n_days), n_campaigns * n_ad_sets)

        n_rows = n_campaigns * n_ad_sets * n_days
        self.campaign_id = campaign_ids
        self.ad_set_id = campaign_ids * 10 + ad_set_offsets
        self.date = dates[date_idx]
        # channel is stable per campaign (a campaign doesn't change channel mid-flight)
        campaign_channel = rng.choice(CHANNELS, size=n_campaigns)
        self.channel = campaign_channel[campaign_ids - 1]
        # base daily budget per campaign drives spend/impressions magnitude
        base_budget = rng.gamma(shape=2.0, scale=150.0, size=n_campaigns)[campaign_ids - 1]
        noise = rng.normal(1.0, 0.18, size=n_rows).clip(0.3, 2.5)
        self.spend_eur = np.round(base_budget * noise / n_ad_sets, 2)
        self.impressions = np.round(self.spend_eur * rng.uniform(80, 260, size=n_rows)).astype(int)
        ctr = rng.beta(2, 60, size=n_rows)
        self.clicks = np.round(self.impressions * ctr).astype(int)
        cvr = rng.beta(1.5, 40, size=n_rows)
        self.conversions = np.round(self.clicks * cvr).astype(int)

        # `updated_at`: originally == date (ingested next day), except a
        # rotating slice we treat as "just restated" -- see current_updated_at().
        self.original_updated_at = np.array(
            [datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
             for d in self.date]
        )
        self.n_rows = n_rows
        self._lock = threading.Lock()

    def current_updated_at(self) -> np.ndarray:
        """
        Returns updated_at timestamps where a small, time-rotating slice of
        historical rows appear freshly updated -- simulating an ad platform
        restating attribution after the fact. The slice rotates by minute so
        repeated polling eventually surfaces (and re-surfaces) corrections,
        the way a real incremental sync against a live API would observe.
        """
        n_restated = max(1, int(self.n_rows * RESTATEMENT_FRACTION))
        minute_bucket = int(time.time() // 60)
        rng = np.random.default_rng(minute_bucket)
        restated_idx = rng.choice(self.n_rows, size=n_restated, replace=False)
        updated = self.original_updated_at.copy()
        updated[restated_idx] = datetime.now(timezone.utc)
        return updated

    def query(self, updated_since: Optional[datetime], page: int, page_size: int):
        updated_at = self.current_updated_at()
        if updated_since is not None:
            mask = updated_at >= updated_since
        else:
            mask = np.ones(self.n_rows, dtype=bool)
        idx = np.nonzero(mask)[0]
        # stable order: by date then campaign/ad_set, so pagination is deterministic
        idx = idx[np.lexsort((self.ad_set_id[idx], self.campaign_id[idx], self.date[idx]))]

        total_records = len(idx)
        total_pages = max(1, (total_records + page_size - 1) // page_size)
        start = (page - 1) * page_size
        end = start + page_size
        page_idx = idx[start:end]

        rows = [
            {
                "campaign_id": int(self.campaign_id[i]),
                "campaign_name": f"campaign_{int(self.campaign_id[i]):04d}",
                "ad_set_id": int(self.ad_set_id[i]),
                "channel": str(self.channel[i]),
                "date": self.date[i].isoformat(),
                "impressions": int(self.impressions[i]),
                "clicks": int(self.clicks[i]),
                "conversions": int(self.conversions[i]),
                "spend_eur": float(self.spend_eur[i]),
                "updated_at": updated_at[i].isoformat(),
            }
            for i in page_idx
        ]
        return rows, total_records, total_pages


STORE = CampaignPerformanceStore()


# --------------------------------------------------------------------- #
# Rate limiting: fixed 60s window, per API key, single-process token count.
# Deliberately simple (no Redis) -- this is one uvicorn worker by design,
# so an in-memory dict is consistent for the life of the container.
# --------------------------------------------------------------------- #
class _RateLimiter:
    def __init__(self, limit_per_min: int) -> None:
        self.limit = limit_per_min
        self._lock = threading.Lock()
        self._window_start = time.time()
        self._count = 0

    def check(self) -> Optional[int]:
        """Returns None if allowed, else seconds to wait before retrying."""
        with self._lock:
            now = time.time()
            if now - self._window_start >= 60:
                self._window_start = now
                self._count = 0
            self._count += 1
            if self._count > self.limit:
                return int(60 - (now - self._window_start)) + 1
            return None


RATE_LIMITER = _RateLimiter(RATE_LIMIT_PER_MIN)


class PageMeta(BaseModel):
    page: int
    page_size: int
    total_pages: int
    total_records: int
    next_page: Optional[int]


class CampaignPerformanceResponse(BaseModel):
    meta: PageMeta
    data: list[dict]


@app.get("/healthz")
def healthz():
    return {"status": "ok", "rows": STORE.n_rows}


@app.get("/v1/campaigns/performance", response_model=CampaignPerformanceResponse)
def get_campaign_performance(
    response: Response,
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    updated_since: Optional[str] = Query(
        None, description="ISO8601 timestamp; only rows updated at/after this are returned"
    ),
    x_api_key: Optional[str] = Header(None),
):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="missing or invalid X-API-Key")

    retry_after = RATE_LIMITER.check()
    if retry_after is not None:
        response.headers["Retry-After"] = str(retry_after)
        raise HTTPException(status_code=429, detail="rate limit exceeded, back off and retry")

    if random.random() < FAILURE_RATE:
        raise HTTPException(status_code=500, detail="transient upstream error, retry")

    parsed_since = None
    if updated_since:
        try:
            parsed_since = datetime.fromisoformat(updated_since.replace("Z", "+00:00"))
            if parsed_since.tzinfo is None:
                parsed_since = parsed_since.replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(status_code=400, detail="updated_since must be ISO8601")

    rows, total_records, total_pages = STORE.query(parsed_since, page, page_size)
    next_page = page + 1 if page < total_pages else None

    return CampaignPerformanceResponse(
        meta=PageMeta(
            page=page,
            page_size=page_size,
            total_pages=total_pages,
            total_records=total_records,
            next_page=next_page,
        ),
        data=rows,
    )
