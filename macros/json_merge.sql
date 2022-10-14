{% macro create_json_merge() -%}
CREATE OR REPLACE FUNCTION {{target.database}}.{{target.schema}}.json_merge(o1 VARIANT, o2 VARIANT)
  RETURNS VARIANT
  LANGUAGE JAVASCRIPT
AS
$$
  return Object.assign(O1, O2);
$$
;
{%- endmacro %}
