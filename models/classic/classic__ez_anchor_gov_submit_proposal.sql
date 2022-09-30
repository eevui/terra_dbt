{{ config(
    materialized = 'view',
    secure = 'true'
) }}

SELECT
    *
FROM
    {{ source(
        'anchor',
        'gov_submit_proposal'
    ) }}
