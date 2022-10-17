{% macro change_decimal(column_name,decimal_place=6)-%}

{{column_name}}/pow(10,{{decimal_place}}) :: INTEGER 

{%- endmacro %}