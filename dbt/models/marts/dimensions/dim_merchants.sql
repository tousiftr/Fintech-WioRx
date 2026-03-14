select
    merchant_id,
    merchant_name,
    category as merchant_category
from {{ ref('stg_merchants') }}
