with source as (

    select * from {{ source('raw', 'product_events') }}

),

cleaned as (

    select
        event_id,
        event_name,
        event_timestamp,
        user_id,
        payment_id,
        session_id,
        platform,
        country,
        source_file,
        ingested_at,
        batch_id

    from source

)

select * from cleaned
