{{
    config(
        materialized="incremental",
        cluster_by=["_inserted_timestamp"],
        unique_key="action_id",
    )
}}

with
    pools as (
        select *
        from {{ ref("core__dim_address_labels") }}
        where label_subtype = 'pool'
    ),
    prelim_table as (
        select
            block_id,
            block_timestamp,
            'terra' as blockchain,
            tx_id,
            tx_succeeded,
            chain_id,
            message_value,
            message_type,
            message_index,
            nullif(
                message_value:contract, message_value:msg:send:contract
            )::string as pool_address,
            message_value:sender::string as liquidity_provider_address,
            attributes,
            path,
            value::string as obj_value,
            _ingested_at,
            _inserted_timestamp
        from
            {{ ref("silver__messages") }},
            lateral flatten(input => attributes:wasm)
        where
            attributes:wasm is not null
            and message_type = '/cosmwasm.wasm.v1.MsgExecuteContract'
            and {{ incremental_load_filter("_inserted_timestamp") }}
    ),
    intermediate_table as (
        select prelim_table.*, label
        from prelim_table
        join pools on prelim_table.pool_address = pools.address
    ),
final_table as (
    select
    block_id,
    block_timestamp,
    blockchain,
    row_number() over (
        partition by tx_id order by _inserted_timestamp desc
    ) as action_index,
    concat(tx_id, '-', action_index -1) as action_id,
    tx_id,
    tx_succeeded,
    chain_id,
    pool_address,
    liquidity_provider_address,
    case
        when path = 'refund_assets'
        then 'withdraw_liquidity'
        when path = 'assets'
        then 'provide_liquidity'
        when path = 'withdrawn_share'
        then 'burn_lp_token'
        when path = 'share'
        then 'mint_lp_token'
        else null
    end as action,
    regexp_substr(value, '[0-9]+')::bigint as amount,
    iff(
        path in ('withdrawn_share', 'share'),
        label,
        regexp_substr(value, '[^[:digit:]](.*)')
    ) as currency,
    null as decimals,
    _ingested_at,
    _inserted_timestamp
from
    intermediate_table,
    lateral split_to_table(intermediate_table.obj_value, ', ')
where
    tx_id in (
        select tx_id
        from intermediate_table
        where obj_value in ('provide_liquidity', 'withdraw_liquidity')
    )
    and path in ('refund_assets', 'withdrawn_share', 'share', 'assets')
)

select 
    block_id,
    block_timestamp,
    action_id,
    tx_id,
    tx_succeeded,
    blockchain,
    chain_id,
    pool_address,
    liquidity_provider_address,
    action,
    amount,
    currency,
    decimals,
    _ingested_at,
    _inserted_timestamp
from
    final_table