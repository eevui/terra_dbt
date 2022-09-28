{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    tx_id,
    block_id,
    block_timestamp,
    auth_type,
    tx_sender,
    gas_limit,
    fee_raw / pow(
        10,
        6
    ) AS fee,
    fee_denom,
    memo,
    codespace,
    tx_code,
    tx_succeeded,
    tx
FROM
    {{ ref('silver__transactions') }}
