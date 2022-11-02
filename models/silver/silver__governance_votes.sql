{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert'
) }}

with base_votes as (
    select
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        chain_id,
        attributes,
        _inserted_timestamp
    from {{ ref('silver__messages') }}
    where message_type = '/cosmos.gov.v1beta1.MsgVote'
      and {{ incremental_load_filter('_inserted_timestamp') }}
),

parsed_votes as (
    select
        tx_id,
        block_id,
        block_timestamp,
        tx_succeeded,
        chain_id,
        attributes:message:sender::text as voter,
        attributes:proposal_vote:proposal_id::number as proposal_id,
        parse_json(attributes:proposal_vote:option) as parsed_vote_option,
        parsed_vote_option:option::number as vote_option,
        case vote_option
            when 1 then 'Yes' 
            when 2 then 'Abstain'
            when 3 then 'No'
            when 4 then 'NoWithVeto'
        end as vote_option_text,
        parsed_vote_option:weight::number as vote_weight,
        'terra' as blockchain,
        _inserted_timestamp
    from base_votes
),

final as (
    select
        tx_id,
        block_id,
        block_timestamp,
        blockchain,
        chain_id,
        voter,
        proposal_id,
        vote_option,
        vote_option_text,
        vote_weight,
        tx_succeeded,
        _inserted_timestamp
    from parsed_votes
)

select * from final
