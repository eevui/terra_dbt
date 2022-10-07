{{ config(
    materialized = 'view',
    secure = true
) }}

SELECT
    blockchain,
    address,
    creator,
    l1_label AS label_type,
    l2_label AS label_subtype,
    address_name AS label,
    project_name
FROM
    {{ source(
        'labels',
        'address_labels'
    ) }}
WHERE
    blockchain = 'terra'
