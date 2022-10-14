{{ config(materialized="view", secure=true) }}

with staking as (select * from {{ ref("silver__staking") }})

select *
from staking