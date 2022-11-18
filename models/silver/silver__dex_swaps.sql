{{ config(
    materialized = "incremental",
    unique_key = "swap_id",
    incremental_strategy = "delete+insert",
    cluster_by = ["block_timestamp::DATE", "_inserted_timestamp::DATE"],
) }}




with swap AS (
SELECT 
    block_id,
    block_timestamp,
    'Terra' as blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    message_value:sender :: STRING AS trader,
    message_value:msg:swap:offer_asset:amount::integer as from_amount,
    COALESCE ( attributes:coin_received:currency_0 :: STRING,
               attributes:coin_received:currency :: STRING ) AS from_currency,
    from_amount / pow(10,6) as from_decimal,
    COALESCE ( attributes:coin_received:amount_1 :: INTEGER,
               attributes:wasm:return_amount :: INTEGER ) AS to_amount,
    attributes:wasm:ask_asset :: STRING As to_currency,
    to_amount / pow(10,6) AS to_dcecimal,
    message_value:contract :: STRING as contract_address
   
FROM 
   TERRA_DEV.silver.messages
where message_type ilike '%msgexecutecontract%'   
and message_value:msg:swap is not null 
)

,execute_swap_operation AS (
 SELECT 
    block_id,
    block_timestamp,
    'Terra' as blockchain,
    chain_id,
    tx_id,
    tx_succeeded,
    message_value:sender :: STRING AS trader,
    COALESCE( attributes:wasm:amount_0 :: INTEGER,
              attributes:coin_received:amount_1 :: INTEGER,
              attributes:coin_received:amount_2 :: INTEGER) AS from_amount,
    COALESCE( attributes:coin_received:currency_0 :: STRING,
            attributes:wasm:ask_asset_0 :: STRING ) AS from_currency,
    from_amount / pow(10,6) as from_decimal,
    COALESCE( attributes:wasm:return_amount_1 :: INTEGER,
              attributes:wasm:return_amount :: INTEGER,
              attributes:coin_received:amount_5 :: INTEGER,
              attributes:coin_received:amount_2 :: INTEGER) as to_amount,
    COALESCE (attributes:coin_received:currency_2 :: STRING,
              attributes:coin_received:currency_1 :: STRING) as to_currency,
    to_amount / pow(10, 6) as to_decimal,
    message_value:contract :: STRING as contract_address,
    
    
    message_value,
    attributes
FROM 
    
    TERRA_DEV.silver.messages
    where message_type ilike '%msgexecutecontract%' 
    and message_value:msg:execute_swap_operations is not null
    
)

,Union_swaps As (

Select 
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        FROM_AMOUNT,
        FROM_CURRENCY,
        FROM_DECIMAL,
        TO_AMOUNT,
        TO_CURRENCY,
        CONTRACT_ADDRESS
FROM 
     SWAP 
UNION ALL 
SELECT 
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        tx_id,
        tx_succeeded,
        FROM_AMOUNT,
        FROM_CURRENCY,
        FROM_DECIMAL,
        TO_AMOUNT,
        TO_CURRENCY,
        CONTRACT_ADDRESS
FROM 
    execute_swap_operations

)

,signer_address AS (

SELECT u.*, t.tx_sender as trader
    from union_swaps u 
    left join transactions t 
    ON u.tx_id = t.tx_id

)

Final AS (

    SELECT 
        signer_address.* , 
)

