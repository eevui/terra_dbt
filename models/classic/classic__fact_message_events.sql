{{ config(
    materialized = 'view',
    secure = 'true'
) }}

SELECT
    *
FROM
    {{ source(
        'terra_sv',
        'msg_events'
    ) }}
