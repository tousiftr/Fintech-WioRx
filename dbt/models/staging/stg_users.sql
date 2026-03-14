with source as (

    select * from {{ source('raw', 'users') }}

)

select *
from source