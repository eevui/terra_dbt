{{ config(
    materialized = 'view'
) }}

with governance_votes as (
    select
        *
    from {{ ref('silver__governance_votes') }}
)

select * from governance_votes
