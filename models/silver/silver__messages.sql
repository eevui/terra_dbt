{{
    config(
        materialized="incremental",
        cluster_by=["_inserted_timestamp"],
        unique_key="message_id",
    )
}}

with
    txs as (

        select
            tx_id,
            block_timestamp,
            block_id,
            tx,
            tx_succeeded,
            value:events as logs,
            value:message_index::number as message_index,
            tx:body:messages[0]:"@type"::string as message_type,
            _ingested_at,
            _inserted_timestamp
        from
            {{ ref("silver__transactions") }},
            lateral flatten(input => tx:tx_result:log)
        where {{ incremental_load_filter("_inserted_timestamp") }}
    ),
    blocks as (
        select block_id, chain_id
        from {{ ref("silver__blocks") }}
        where {{ incremental_load_filter("_inserted_timestamp") }}
    ),
    events as (
        select
            tx_id,
            tx,
            block_timestamp,
            block_id,
            message_index,
            tx_succeeded,
            tx:body:messages[0] as message_value,
            message_type,
            value as logs,
            value:attributes as event_attributes,
            value:type::string as event_type,
            index as event_index,
            _ingested_at,
            _inserted_timestamp
        from txs, lateral flatten(input => logs)
    ),
    attributes as (
        select
            tx_id,
            tx,
            block_timestamp,
            block_id,
            event_attributes,
            event_type,
            event_index,
            tx_succeeded,
            message_index,
            message_type,
            index as attribute_index,
            value as attribute,
            value:key::string as attribute_key,
            iff(
                value:key = 'amount',
                regexp_substr(value:value::string, '[0-9]+'),
                value:value::string
            ) as attribute_value,
            iff(
                value:key = 'amount',
                regexp_substr(value:value::string, '[A-Za-z]+'),
                null
            ) as currency,
            last_value(currency) over (
                partition by tx_id, event_type order by currency desc
            ) as last_currency,
            count(attribute_key) over (
                partition by attribute_key, event_index, message_index, tx_id
            ) as key_frequency,
            row_number() over (
                partition by attribute_key, event_index, message_index, tx_id
                order by attribute_key
            )
            - 1 as key_index,
            _ingested_at,
            _inserted_timestamp
        from events, lateral flatten(input => event_attributes)
    ),
    window_functions as (
        select
            tx_id,
            tx,
            event_type,
            event_attributes,
            event_index,
            message_index,
            message_type,
            tx_succeeded,
            attributes.block_id,
            chain_id,
            block_timestamp,
            iff(
                key_frequency > 1,
                concat(attribute_key, '_', key_index),
                attribute_key
            ) as unique_attribute_key,
            attribute_value,
            object_agg(unique_attribute_key, attribute_value::variant) over (
                partition by tx_id, message_index, event_type
            ) as attribute_obj,
            object_insert(
                attribute_obj, 'currency', last_currency
            ) as final_attrib_obj,
            _ingested_at,
            _inserted_timestamp
        from attributes
        join blocks on attributes.block_id = blocks.block_id
    ),
    distinct_events_table as (
        select distinct
            tx_id,
            message_index,
            event_type,
            chain_id,
            message_type,
            tx_succeeded,
            block_timestamp,
            block_id,
            final_attrib_obj,
            _ingested_at,
            _inserted_timestamp
        from window_functions
    ),
    final_table as (
        select distinct
            concat(tx_id, '-', message_index) as message_id,
            block_timestamp,
            block_id,
            tx_id,
            tx_succeeded,
            chain_id,
            message_index,
            message_type,
            object_agg(event_type, final_attrib_obj) over (
                partition by tx_id, message_index
            ) as attributes,
            _ingested_at,
            _inserted_timestamp
        from distinct_events_table
        order by tx_id, message_index
    )
select *
from final_table
