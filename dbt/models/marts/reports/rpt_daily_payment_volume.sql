select
    date(created_at) as payment_date,
    sum(amount) as total_payment_amount,
    count(*) as total_transactions
from {{ ref('fct_payments') }}
group by 1
