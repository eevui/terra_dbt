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
)
SELECT
    tx_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx :auth_info :signer_infos [0] :public_key :key :: STRING AS pub_key,
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
