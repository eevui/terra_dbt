{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    tx_id,
    block_id,
    block_timestamp,
    auth_type,
    authorizer_public_key,
    tx_sender,
    gas_limit,
    fee_raw / pow(
        10,
        6
    ) AS fee,
    fee_denom,
    memo,
    tx
FROM
    {{ ref('silver__transactions') }}
