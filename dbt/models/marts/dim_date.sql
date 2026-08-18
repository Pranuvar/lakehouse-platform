-- Generated, not sourced -- a date dimension doesn't come from any of
-- the 4 sources, it's derived. Spans generously wider than the actual
-- data (2024-01-01 to 2027-01-01) so it never needs regenerating as the
-- seeded history rolls forward.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
)

select
    cast(date_day as date) as date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    strftime(date_day, '%Y-%m') as year_month,
    strftime(date_day, '%B') as month_name,
    strftime(date_day, '%A') as day_name,
    extract(dow from date_day) in (0, 6) as is_weekend
from spine
