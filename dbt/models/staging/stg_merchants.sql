with source as (

    select * from {{ source('raw', 'merchants') }}

),

cleaned as (

    select
       merchant_id,
       merchant_name,
       category,
       country,
       created_at,
       source_file,
       ingested_at,
       batch_id

    from source

)

select * from cleaned
