select
    campaign_id,
    campaign_name,
    ad_set_id,
    channel,
    cast(date as date) as performance_date,
    impressions,
    clicks,
    conversions,
    spend_eur,
    -- lands as a VARCHAR (the mock API serialises it via .isoformat() --
    -- see docker/mock-api/app.py -- and nothing between there and here
    -- casts it back). Cast explicitly rather than leaving it as a
    -- string: an earlier version left this uncast, which compiled fine
    -- everywhere EXCEPT fct_campaign_performance.sql's incremental
    -- filter (`coalesce(max(updated_at), '1970-01-01'::timestamp)`) --
    -- that's a VARCHAR/TIMESTAMP mix DuckDB correctly refuses, but only
    -- on a genuine incremental run (is_incremental() is false, and the
    -- comparison never happens, on the first build / any --full-refresh
    -- -- which is exactly why this passed a `--full-refresh` build
    -- clean and then failed the very next normal `dbt build`).
    cast(updated_at as timestamp) as updated_at
from {{ delta_source('silver', 'campaign_performance') }}
