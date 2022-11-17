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
    try_parse_json(message_value) as message_json,
    attributes,
    CASE 
        WHEN message_json is not null and message_json:msg[0] ilike '%swap%'
        THEN message_json:msg:swap:offer_asset :amount :: INTEGER END as from_amount,
    message_json:contract :: STRING as contract_address,
    CASE
        WHEN contract_address = 'terra1zrs8p04zctj0a0f9azakwwennrqfrkh3l6zkttz9x89e7vehjzmqzg8v7n' THEN 'terraswap'
        WHEN contract_address = 'terra1fd68ah02gr2y8ze7tm9te7m70zlmc7vjyyhs6xlhsdmqqcjud4dql4wpxr' THEN 'astroport' END AS Project_name
FROM 
   TERRA_DEV.silver.messages
where message_type ilike '%msgexecutecontract%'
and message_json:msg in ('swap')
)

execute_swap_operations AS (


    
)