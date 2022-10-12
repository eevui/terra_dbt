{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id'
) }}


--Data pulled from messages / delegate
with delegated AS (
SELECT 
    BLOCK_ID,
    BLOCK_TIMESTAMP,
    TX_ID,
    TX_SUCCEEDED,
    CHAIN_ID,
    MESSAGE_INDEX As MSG_INDEX,
    'Delegate' AS Action,
    ATTRIBUTES:transfer :recipient :: STRING AS delegator_address,
    ATTRIBUTES:delegate :amount/pow(10,6) :: INTEGER AS amount,
    ATTRIBUTES:delegate :validator :: STRING AS validator_address
    
FROM 
    {{ref ('silver__messages')}}
WHERE 
    message_type ilike '%msgdelegate%'
    and ATTRIBUTES:message :module :: STRING = 'staking'
    and TX_SUCCEEDED = 'True'
    and {{ incremental_load_filter("_inserted_timestamp") }}
) 
 select *
 from delegated 
 where tx_id = '197CE8367FCB5059D4B3650C389EABDEDA9B6464C75B92D8689F6C83891EC631'
 
 --Data pulled from messages / undelegate
undelegated AS (
SELECT 
    BLOCK_ID,
    BLOCK_TIMESTAMP,
    TX_ID,
    TX_SUCCEEDED,
    CHAIN_ID,
    MESSAGE_INDEX As MSG_INDEX,
    'Undelegate' AS Action,
    ATTRIBUTES:transfer :recipient_0 :: STRING AS delegator_address,
    ATTRIBUTES:unbond :amount/pow(10,6) :: INTEGER AS amount,
    ATTRIBUTES:unbond :validator :: STRING AS validator_address
FROM 
    {{ref ('silver__messages')}}
WHERE 
    message_type ilike '%msgundelegate%'
    and ATTRIBUTES:message :module :: STRING = 'staking'
    and TX_SUCCEEDED = 'True'
    and {{ incremental_load_filter("_inserted_timestamp") }}
 )
 
 --Dta pulled from message/redelegate action
 ,redelegate AS (
    SELECT 
        BLOCK_ID,
        BLOCK_TIMESTAMP,
        TX_ID,
        TX_SUCCEEDED,
        CHAIN_ID,
        MESSAGE_INDEX As MSG_INDEX,
        'Redelegate' AS Action,
        ATTRIBUTES:transfer :recipient :: STRING AS delegator_address,
        ATTRIBUTES:redelegate :amount/pow(10,6) :: INTEGER AS amount,
        ATTRIBUTES:redelegate :destination_validator :: STRING AS validator_address,
        ATTRIBUTES:redelegate :source_validator :: STRING AS validator_src_address
    FROM 
       {{ref ('silver__messages')}}
    WHERE 
        message_type ilike '%MsgBeginRedelegate%'
        and ATTRIBUTES:message :module :: STRING = 'staking'
        and TX_SUCCEEDED = 'True'
        and {{ incremental_load_filter("_inserted_timestamp") }}
     
 )

  --src_address/redelegate
 ,src_address AS (
    SELECT
        TX_ID,
        ATTRIBUTES:redelegate :source_validator :: STRING AS validator_src_address
    FROM 
        terra_dev.silver.messages
    WHERE 
        message_type ilike '%MsgBeginRedelegate%'
        and ATTRIBUTES:message :module :: STRING = 'staking'
        and TX_SUCCEEDED = 'True'
        and {{ incremental_load_filter("_inserted_timestamp") }}
) 
 
  --union them all
 ,union_delegations AS (
    SELECT *
    FROM 
        delegate
    UNION ALL
    SELECT *
    FROM
        undelegate
    UNION ALL
    SELECT *
    FROM 
        redelegate
 ) 

 SELECT union_delegations.*, src_address.validator_src_address
 FROM 
    union_delegations 
    LEFT OUTER JOIN src_address
    ON union_delegations.tx_id = src_address.tx_id