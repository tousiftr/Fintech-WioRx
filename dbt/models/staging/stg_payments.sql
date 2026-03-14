with source as (

    select * from {{ source('raw', 'payments') }}

),

cleaned as (

    select
        payment_id,
        user_id,
        merchant_id,
        amount,
        currency,
        payment_method,
        gateway,
        status,
        created_at,
        source_file,
        ingested_at,
        batch_id

    from source

)

select * from cleaned
