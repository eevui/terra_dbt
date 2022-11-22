with pools as (
    select
        address
    from core.dim_address_labels
    where label_subtype = 'pool'
),

lp_txs as (
    select
        *
    from {{ ref('silver__mesages') }}
    where message_value:contract in (select address from pools)
),

withdraws as (
    select
        tx_id,
        'withdraw' as action,
        parse_json(try_base64_decode_string(message_value:msg:send:msg)) as decoded_message,
        attributes:wasm as logs
    from lp_txs
    where message_value:msg:send is not null
      and decoded_message:withdraw_liquidity is not null
),

withdraw_tokens as (
    select
        tx_id,
        regexp_substr(value, $$\d+$$)::number as amount,
        regexp_substr(value, $$\d+(\D.+)$$, 1, 1, 'e', 1) as token
    from withdraws,
    lateral split_to_table(logs:refund_assets::string, ', ')
),

provides as (
    select
        tx_id,
        attributes:wasm as logs
    from lp_txs
      where message_value:msg:provide_liquidity is not null
),

provide_tokens as (
    select
        tx_id,
        regexp_substr(value, $$\d+$$)::number as amount,
        regexp_substr(value, $$\d+(\D.+)$$, 1, 1, 'e', 1) as token
    from provides,
    lateral split_to_table(logs:assets::string, ', ')
),

combined as (
    select
        tx_id,
        amount,
        token
    from withdraw_tokens
    union
    select
        tx_id,
        amount,
        token
    from provide_tokens
),

actual as (
    select 
        tx_id,
        amount,
        token
    from {{ ref('silver__lp_actions') }}
),

expected as (
    select
        *
    from combined
),

test as (
    (select
        *
    from actual
    except
    select
        *
    from expected)
    union all
    (select
        *
    from expected
    except
    select
        *
    from actual)
)

select * from test
