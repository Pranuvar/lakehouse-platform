select
    customer_id,
    first_name,
    last_name,
    email,
    country,
    city,
    signup_date,
    is_marketing_opt_in,
    merged_identity_count > 1 as is_resolved_duplicate_identity
from {{ ref('stg_customers') }}
