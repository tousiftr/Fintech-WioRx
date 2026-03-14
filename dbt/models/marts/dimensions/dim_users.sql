select
    user_id,
    created_at
from {{ ref('stg_users') }}
