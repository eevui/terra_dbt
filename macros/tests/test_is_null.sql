{% test is_null(model, column_name) %}

with validations as (
    select {{column_name}} as tx_sender

    from {{ model }}

),

validation_errors as ( 
    select 
        tx_sender
    from validations
    where tx_succeeded <> 'TRUE'

)

select *
from validation_errors

{% endtest %}