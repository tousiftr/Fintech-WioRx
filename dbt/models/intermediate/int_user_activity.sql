with payments as (

    select * from {{ ref('stg_payments') }}

),

users as (

    select * from {{ ref('stg_users') }}

),

merchants as (

    select * from {{ ref('stg_merchants') }}

)

select
    p.payment_id,
    p.user_id,
    p.merchant_id,
    m.merchant_name,
    p.amount,
    p.currency,
    p.status as payment_status,
    p.created_at
from payments p
left join users u
    on p.user_id = u.user_id
left join merchants m
    on p.merchant_id = m.merchant_id
