{{ config(
    materialized = 'view',
    secure = true
) }}

WITH messages AS (

    SELECT
        *
    FROM
        {{ ref('silver__msg') }}
)

SELECT
  block_id,
  block_timestamp,
  blockchain,
  chain_id,
  tx_id,
  tx_succeeded,
  msg_group,
  message_index,
  message_type,
  msg
FROM
  messages