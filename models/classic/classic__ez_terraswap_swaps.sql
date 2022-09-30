{{ config(
    materialized = 'view',
    secure = 'true'
) }}

SELECT
    *
FROM
    {{ source(
        'terraswap',
        'swaps'
    ) }}
