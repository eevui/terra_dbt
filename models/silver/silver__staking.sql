{{
    config(
        materialized="incremental",
        cluster_by=["_inserted_timestamp::DATE"],
        unique_key = "staking_id",
    )
}}

with
    delegated as (
        select
            'terra' as blockchain,
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Delegate' as action,
            message_value:delegator_address::string as delegator_address,
            {{ change_decimal("MESSAGE_VALUE:amount:amount") }} as amount,
            message_value:validator_address::string as validator_address,
            _ingested_at,
            _inserted_timestamp

        from {{ ref("silver__messages") }}
        where
            message_type ilike '%msgdelegate%'
            and attributes:message:module::string = 'staking'
            and tx_succeeded = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}

    ),
    message_exec as (
        select
            'terra' as blockchain,
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Delegate' as action,
            message_value.value:delegator_address::string as delegator_address,
            message_value.value:amount:amount / pow(10, 6)::integer as amount,
            message_value.value:validator_address::string as validator_address,
            _ingested_at,
            _inserted_timestamp


        from
            {{ ref("silver__messages") }},
            lateral flatten(message_value:msgs) message_value
        where
            message_value.value:"@type" ilike '%MsgDelegate'
            and message_type ilike '%MsgExec%'
            and tx_succeeded = 'True'
            and {{ incremental_load_filter("_inserted_timestamp") }}
    ),
    undelegated as (
        select
            'terra' as blockchain,
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Undelegate' as action,
            message_value:delegator_address::string as delegator_address,
            {{ change_decimal("MESSAGE_VALUE:amount:amount") }} as amount,
            message_value:validator_address::string as validator_address,
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
            'terra' as blockchain,
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index as msg_index,
            'Redelegate' as action,
            message_value:delegator_address::string as delegator_address,
            {{ change_decimal("MESSAGE_VALUE:amount:amount") }} as amount,
            -- MESSAGE_VALUE:amount:amount / pow(10, 6)::integer as amount,
            message_value:validator_dst_address::string as validator_address,
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
            tx_id, message_value:validator_src_address::string as validator_src_address
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
    ),

final_table AS (
select 
    DISTINCT CONCAT(
            union_delegations.tx_id,
            '-',
            action,
            '-',
            msg_index,
            '-',
            delegator_address
        ) AS staking_id,
    blockchain,
    block_id,
    block_timestamp,
    union_delegations.tx_id,
    tx_succeeded,
    chain_id,
    msg_index,
    action,
    delegator_address,
    amount,
    validator_address,
    _ingested_at,
    _inserted_timestamp, 
    src_address.validator_src_address
from union_delegations
left outer join src_address on union_delegations.tx_id = src_address.tx_id
)

SELECT
    *
FROM 
    final_table