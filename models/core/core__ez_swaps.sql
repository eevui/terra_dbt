{{ config(
    materialized = 'view',
    secure = true
) }}

With swap as (

    Select *
    From 
        {{ ref('silver__dex_swaps') }}
)

SELECT 
    BLOCK_ID,
    BLOCK_TIMESTAMP,
    BLOCKCHAIN,
    CHAIN_ID,
    tx_id,
    tx_succeeded,
    trader,
    From_amount,
    from_currency,
    from_decimal,
    to_amount,
    to_currency,
    to_decimal,
    pool_id

FROM 
    swap 