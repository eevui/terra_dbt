{{ config(
    materialized = 'view',
    secure = true
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
)
SELECT
    block_id,
    block_timestamp,
    block_hash,
    tx_count,
    chain_id,
    consensus_hash,
    data_hash,
    evidence,
    evidence_hash,
    block_height,
    last_block_id,
    last_commit,
    last_commit_hash,
    last_results_hash,
    next_validators_hash,
    proposer_address,
    validators_hash
FROM
    blocks
