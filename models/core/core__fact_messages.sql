{{ config(
    materialized = 'view',
    secure = true
) }}

with messages as (

    select
        *
    from
        {{ ref('silver__messages') }}
)
select
    message_id,
    block_timestamp,
    block_id,
    tx_id,
    tx_succeeded,
    chain_id,
    message_index,
    message_type,
    attributes
from
    messages
    
