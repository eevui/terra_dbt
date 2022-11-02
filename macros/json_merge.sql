{% macro create_json_merge() -%}
CREATE OR REPLACE FUNCTION {{target.database}}.{{target.schema}}.json_merge(o1 OBJECT, o2 OBJECT)
  RETURNS OBJECT
  LANGUAGE JAVASCRIPT
AS
$$
  return Object.assign(O1, O2);
$$
;
{%- endmacro %}
