{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    *
FROM
    {{ ref('silver__transactions') }}
