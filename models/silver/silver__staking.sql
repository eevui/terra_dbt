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
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Delegate' as action,
            attributes:transfer:recipient::string as delegator_address,
            attributes:delegate:amount / pow(10, 6)::integer as amount,
            attributes:delegate:validator::string as validator_address,
            _ingested_at,
            _inserted_timestamp

        from {{ ref("silver__messages") }}
        where
            message_type ilike '%msgdelegate%'
            and attributes:message:module::string = 'staking'
            and tx_succeeded = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}
    
    ),
    undelegated as (
        select
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Undelegate' as action,
            attributes:transfer:recipient_0::string as delegator_address,
            attributes:unbond:amount / pow(10, 6)::integer as amount,
            attributes:unbond:validator::string as validator_address,
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
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Redelegate' as action,
            attributes:transfer:recipient::string as delegator_address,
            attributes:redelegate:amount / pow(10, 6)::integer as amount,
            attributes:redelegate:destination_validator::string as validator_address,
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
            attributes:redelegate:source_validator::string as validator_src_address
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
        from undelegated
        union all
        select *
        from redelegated
    )

select union_delegations.*, src_address.validator_src_address
from union_delegations
left outer join src_address on union_delegations.tx_id = src_address.tx_id
