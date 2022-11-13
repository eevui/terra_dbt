{% macro create_json_merge() -%}
CREATE
OR REPLACE FUNCTION {{ target.database }}.{{ target.schema }}.json_merge(
  o1 OBJECT,
  o2 OBJECT
) returns object
language python
runtime_version = 3.8
handler = 'json_merge'
as $$
def json_merge(o1, o2):
    o1.update(o2)
    return o1
$$;
{%- endmacro %}
