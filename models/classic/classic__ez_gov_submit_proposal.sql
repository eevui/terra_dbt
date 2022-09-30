{{ config(
    materialized = 'view',
    secure = 'true'
) }}

SELECT
    *
FROM
    {{ source(
        'terra',
        'gov_submit_proposal'
    ) }}
