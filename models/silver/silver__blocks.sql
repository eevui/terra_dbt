{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
) }}

WITH base_blocks AS (
    SELECT
        record_id,
        offset_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        header,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
        qualify ROW_NUMBER() over (
            PARTITION BY block_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
),
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_count,
        header :app_hash :: STRING AS block_hash,
        header :chain_id :: STRING AS chain_id,
        header :consensus_hash :: STRING AS consensus_hash,
        header :data_hash :: STRING AS data_hash,
        header :evidence AS evidence,
        header :evidence_hash :: STRING AS evidence_hash,
        header :height :: INTEGER AS block_height,
        header :last_block_id AS last_block_id,
        header :last_commit AS last_commit,
        header :last_commit_hash :: STRING AS last_commit_hash,
        header :last_results_hash :: STRING AS last_results_hash,
        header :next_validators_hash :: STRING AS next_validators_hash,
        header :proposer_address :: STRING AS proposer_address,
        header :validators_hash :: STRING AS validators_hash,
        _ingested_at,
        _inserted_timestamp
    FROM
        base_blocks
)
SELECT
    *
FROM
    FINAL
