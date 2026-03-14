select
    payment_id,
    user_id,
    merchant_id,
    amount,
    currency,
    payment_status,
    created_at
from {{ ref('int_payments_enriched') }}
