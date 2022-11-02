{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base AS (
    Select 
        block_id,
        block_timestamp,
        'terra' As blockchain,
        chain_id,
        TX_ID,
        TX_succeeded,
        MESSAGE_VALUE:proposer :: STRING AS proposer,
        ATTRIBUTES:submit_proposal :proposal_id :: INTEGER AS proposal_id,
        ATTRIBUTES:submit_proposal :proposal_type :: STRING AS proposal_type,
        _ingested_at,
        _inserted_timestamp
    FROM 
        {{ ref('silver__messages')}}
    where 
        message_type ilike '%MsgSubmitProposal%'
        and attributes:message:module::string = 'governance'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) 
        
    FROM
        {{this}}
)
{% endif %}
)

SELECT 
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_id,
    TX_succeeded,
    proposer,
    proposal_id,
    proposal_type,
    _ingested_at,
    _inserted_timestamp
FROM    
    base 