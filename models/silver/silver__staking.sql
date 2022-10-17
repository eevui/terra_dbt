{{
    config(
        materialized="incremental",
        cluster_by=["_inserted_timestamp::DATE"],
        unique_key="tx_id",
    )
}}

with
    delegated as (
        select
            'terra' AS BLOCKCHAIN,
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Delegate' as action,
            MESSAGE_VALUE :delegator_address :: STRING AS delegator_address,
            {{change_decimal('MESSAGE_VALUE:amount:amount')}} AS amount,
            MESSAGE_VALUE :validator_address :: STRING AS validator_address,
            _ingested_at,
            _inserted_timestamp

        from {{ ref("silver__messages") }}
        where
            message_type ilike '%msgdelegate%'
            and attributes:message:module::string = 'staking'
            and tx_succeeded = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}
    
    ),
    message_exec AS (
        SELECT 
            'terra' AS BLOCKCHAIN,
            BLOCK_ID,
            BLOCK_TIMESTAMP,
            TX_ID,
            TX_SUCCEEDED, 
            CHAIN_ID,
            MESSAGE_INDEX As MSG_INDEX,
            'Delegate' AS Action,
            message_value.value :delegator_address :: STRING AS delegator_address,
            message_value.value:amount :amount/pow(10,6) :: INTEGER AS amount,
            message_value.value :validator_address :: STRING AS validator_address,
            _INGESTED_AT,
            _INSERTED_TIMESTAMP
    
    
        FROM 
            terra_dev.silver.messages,
            LATERAL flatten (message_value:msgs) message_value
        WHERE 
            tx_id = '40F38644FB9C32F8AF134BAEC61C07AFE92F1A247903D6CF192663F4B52D227C'
            and message_value.value :"@type" ilike '%MsgDelegate'
            and message_type ilike'%MsgExec%'
            and TX_SUCCEEDED = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}
    ),
    undelegated as (
        select
            'terra' AS BLOCKCHAIN,
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Undelegate' as action,
            MESSAGE_VALUE :delegator_address :: STRING AS delegator_address,
            {{change_decimal('MESSAGE_VALUE:amount:amount')}} AS amount,
            MESSAGE_VALUE :validator_address :: STRING AS validator_address,
            _ingested_at,
            _inserted_timestamp
        from {{ ref("silver__messages") }}
        where
            message_type ilike '%msgundelegate%'
            and attributes:message:module::string = 'staking'
            and tx_succeeded = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}
    
    ),
    redelegated as (
        select
            'terra' AS BLOCKCHAIN,
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Redelegate' as action,
            MESSAGE_VALUE :delegator_address :: STRING AS delegator_address,
            {{change_decimal('MESSAGE_VALUE:amount:amount')}} AS amount,
            --MESSAGE_VALUE:amount:amount / pow(10, 6)::integer as amount,
            MESSAGE_VALUE :validator_dst_address :: STRING AS validator_address,
            _ingested_at,
            _inserted_timestamp

        from {{ ref("silver__messages") }}
        where
            message_type ilike '%MsgBeginRedelegate%'
            and attributes:message:module::string = 'staking'
            and tx_succeeded = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}

   
    ),
    src_address as (
        select
            tx_id,
            MESSAGE_VALUE :validator_src_address :: STRING AS validator_src_address
        from {{ ref("silver__messages") }}
        where
            message_type ilike '%MsgBeginRedelegate%'
            and attributes:message:module::string = 'staking'
            and tx_succeeded = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}
    
    ),
    union_delegations as (
        select *
        from delegated
        union all 
        select *
        from message_exec
        union all
        select *
        from undelegated
        union all
        select *
        from redelegated
    )

select union_delegations.*, src_address.validator_src_address
from union_delegations
left outer join src_address on union_delegations.tx_id = src_address.tx_id
