{{ config(
    materialized = 'view',
    secure = 'true'
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'validator_voting_power'
    ) }}
