with payments as (

    select * from {{ ref('stg_payments') }}

),

merchants as (

    select * from {{ ref('stg_merchants') }}

)

select
    m.merchant_id,
    m.merchant_name,
    m.category,
    m.country,
    count(p.payment_id)                                                        as total_transactions,
    sum(p.amount)                                                              as total_amount,
    count(case when p.status = 'completed' then p.payment_id end)             as successful_transactions,
    sum(case when p.status = 'completed' then p.amount end)                   as successful_amount
from merchants m
left join payments p
    on m.merchant_id = p.merchant_id
group by
    m.merchant_id,
    m.merchant_name,
    m.category,
    m.country
