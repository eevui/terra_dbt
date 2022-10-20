{{ config(
    materialized = "incremental",
    cluster_by = ["_inserted_timestamp"],
    unique_key = "transfer_id",
) }}

WITH flattened_attributes AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        chain_id,
        message_value,
        message_type,
        message_index,
        REGEXP_SUBSTR(
            key,
            '[0-9]+'
        ) :: NUMBER AS key_index,
        CASE
            WHEN path LIKE 'amount%' THEN VALUE :: STRING
            ELSE NULL
        END AS amount,
        CASE
            WHEN path LIKE 'sender%' THEN VALUE :: STRING
            ELSE NULL
        END AS sender,
        CASE
            WHEN path LIKE 'currency%' THEN VALUE :: STRING
            ELSE NULL
        END AS currency,
        CASE
            WHEN path LIKE 'recipient%' THEN VALUE :: STRING
            ELSE NULL
        END AS receiver,
        _ingested_at,
        _inserted_timestamp
    FROM
        terra_dev.silver.messages,
        LATERAL FLATTEN(
            input => attributes :transfer,
            outer => TRUE
        )
    WHERE
        attributes :transfer IS NOT NULL
),
unpivoted_table AS (
    SELECT
        *
    FROM
        flattened_attributes unpivot(
            VALUE for key IN (
                sender,
                amount,
                currency,
                receiver
            )
        )
),
pivoted_table AS (
    SELECT
        *
    FROM
        unpivoted_table pivot(MAX(VALUE) for key IN ('AMOUNT', 'CURRENCY', 'SENDER', 'RECEIVER')) AS p (
            block_id,
            block_timestamp,
            tx_id,
            tx_succeeded,
            chain_id,
            message_value,
            message_type,
            message_index,
            key_index,
            _ingested_at,
            _inserted_timestamp,
            amount,
            currency,
            sender,
            receiver
        )
),
final_table AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        chain_id,
        message_value,
        message_type,
        message_index,
        amount,
        currency,
        sender,
        receiver,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                block_timestamp
        ) - 1 AS INDEX,
        CONCAT(
            tx_id,
            '_',
            INDEX
        ) AS transfer_id,
        REGEXP_SUBSTR(
            message_type,
            '(([^./]+)(/.\.|))',
            1,
            '1'
        ) AS blockchain,
        CASE
            WHEN message_type LIKE '/ibc%' THEN 'IBC_Transfer_In'
            ELSE 'IBC_Transfer_Off'
        END AS transfer_type,
        _ingested_at,
        _inserted_timestamp
    FROM
        pivoted_table
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    transfer_id,
    tx_succeeded,
    chain_id,
    message_value,
    message_type,
    message_index,
    amount,
    currency,
    sender,
    receiver,
    blockchain,
    transfer_type,
    _ingested_at,
    _inserted_timestamp
FROM
    final_table
