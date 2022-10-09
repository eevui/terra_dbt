{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id'
) }}

WITH txs AS (

    SELECT
        tx_id,
        block_timestamp,
        block_id,
        tx,
        tx_succeeded,
        VALUE :events AS logs,
        VALUE :msg_index :: NUMBER AS msg_index,
        tx :body :messages [0] :"@type" :: STRING AS msg_type,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }},
        LATERAL FLATTEN(
            input => tx :tx_result :log
        )
),
blocks AS (
    SELECT
        block_id,
        chain_id
    FROM
        {{ ref('silver__blocks') }}
),
events AS (
    SELECT
        tx_id,
        tx,
        block_timestamp,
        block_id,
        msg_index,
        tx_succeeded,
        tx :body :messages [0] AS msg_value,
        msg_type,
        VALUE AS logs,
        VALUE :attributes AS event_attributes,
        VALUE :type :: STRING AS event_type,
        INDEX AS event_index,
        _ingested_at,
        _inserted_timestamp
    FROM
        txs,
        LATERAL FLATTEN(
            input => logs
        )
),
attributes AS (
    SELECT
        tx_id,
        tx,
        block_timestamp,
        block_id,
        event_attributes,
        event_type,
        event_index,
        tx_succeeded,
        msg_index,
        msg_type,
        INDEX AS attribute_index,
        VALUE AS ATTRIBUTE,
        VALUE :key :: STRING AS attribute_key,
        IFF(
            VALUE :key = 'amount',
            SPLIT_PART(
                TRIM(
                    REGEXP_REPLACE(
                        VALUE :value :: STRING,
                        '[^[:digit:]]',
                        ' '
                    )
                ),
                ' ',
                0
            ),
            VALUE :value :: STRING
        ) AS attribute_value,
        IFF(
            VALUE :key = 'amount',
            REGEXP_SUBSTR(
                VALUE :value :: STRING,
                '[A-Za-z]+'
            ),
            NULL
        ) AS currency,
        LAST_VALUE(currency) over (
            PARTITION BY tx_id,
            event_type
            ORDER BY
                currency DESC
        ) AS last_currency,
        COUNT(attribute_key) over (
            PARTITION BY attribute_key,
            event_index,
            msg_index,
            tx_id
        ) AS key_frequency,
        ROW_NUMBER() over (
            PARTITION BY attribute_key,
            event_index,
            msg_index,
            tx_id
            ORDER BY
                attribute_key
        ) - 1 AS key_index,
        _ingested_at,
        _inserted_timestamp
    FROM
        events,
        LATERAL FLATTEN(
            input => event_attributes
        )
    ORDER BY
        tx_id,
        msg_index,
        event_type,
        attribute_index
),
third_table AS (
    SELECT
        tx_id,
        tx,
        event_type,
        event_attributes,
        event_index,
        msg_index,
        msg_type,
        tx_succeeded,
        attributes.block_id,
        chain_id,
        block_timestamp,
        IFF(
            key_frequency > 1,
            CONCAT(
                attribute_key,
                '_',
                key_index
            ),
            attribute_key
        ) AS unique_attribute_key,
        attribute_value,
        OBJECT_AGG(
            unique_attribute_key,
            attribute_value :: variant
        ) over (
            PARTITION BY tx_id,
            msg_index,
            event_type
        ) AS attribute_obj,
        OBJECT_INSERT(
            attribute_obj,
            'currency',
            last_currency
        ) AS final_attrib_obj,
        _ingested_at,
        _inserted_timestamp
    FROM
        attributes
        JOIN blocks
        ON attributes.block_id = blocks.block_id
),
final_table AS (
    SELECT
        DISTINCT tx_id,
        msg_index,
        event_type,
        chain_id,
        msg_type,
        tx_succeeded,
        block_timestamp,
        block_id,
        final_attrib_obj,
        _ingested_at,
        _inserted_timestamp
    FROM
        third_table
),
FINAL AS (
    SELECT
        DISTINCT CONCAT(
            tx_id,
            '-',
            msg_index
        ) AS message_id,
        block_timestamp,
        block_id,
        tx_id,
        tx_succeeded,
        chain_id,
        msg_index AS message_index,
        msg_type AS message_type,
        OBJECT_AGG(
            event_type,
            final_attrib_obj
        ) over (
            PARTITION BY tx_id,
            msg_index
        ) AS attributes,
        _ingested_at,
        _inserted_timestamp
    FROM
        final_table
    ORDER BY
        tx_id,
        message_index
)
SELECT
    *
FROM
    FINAL
