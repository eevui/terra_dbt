{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id'
) }}

WITH bronze_txs AS (

    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_id
    ORDER BY
        _ingested_at DESC
) = 1
),
silver_txs AS (
    SELECT
        tx_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx :auth_info :signer_infos [0] :public_key :key :: STRING AS pub_key,
        TRY_BASE64_DECODE_STRING(
            tx :tx_result :events [0] :attributes [0] :key
        ) AS msg0_key,
        TRY_BASE64_DECODE_STRING(
            tx :tx_result :events [0] :attributes [0] :value
        ) AS msg0_value,
        tx :body :messages [0] :grantee :: STRING AS tx_grantee,
        tx :auth_info :fee :granter :: STRING AS tx_granter,
        tx :auth_info :fee :payer :: STRING AS tx_payer,
        CASE
            WHEN msg0_key = 'spender' THEN msg0_value
            WHEN msg0_key = 'granter' THEN tx_payer
            WHEN msg0_key = 'fee' THEN tx_grantee
        END AS tx_sender,
        tx :auth_info :fee :gas_limit :: NUMBER AS gas_limit,
        tx :auth_info :fee :amount [0] :amount :: NUMBER AS fee_raw,
        tx :auth_info :fee :amount [0] :denom :: STRING AS fee_denom,
        tx :body :memo :: STRING AS memo,
        tx :body AS tx_body,
        tx :tx_result AS tx_result,
        tx,
        _ingested_at,
        _inserted_timestamp
    FROM
        bronze_txs
)
SELECT
    tx_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    pub_key,
    tx_sender,
    gas_limit,
    fee_raw,
    fee_denom,
    memo,
    tx_body,
    tx_result,
    tx,
    _ingested_at,
    _inserted_timestamp
FROM
    silver_txs
