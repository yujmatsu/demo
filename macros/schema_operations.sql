{% macro create_schema_if_not_exists(schema_name) %}
  {% set sql %}
    create schema if not exists {{ target.database }}.{{ schema_name }}
  {% endset %}
  {% do run_query(sql) %}
  {% do log("Schema " ~ target.database ~ "." ~ schema_name ~ " created or already exists", info=True) %}
{% endmacro %}

{% macro drop_schema_if_exists(schema_name) %}
  {% set sql %}
    drop schema if exists {{ target.database }}.{{ schema_name }} cascade
  {% endset %}
  {% do run_query(sql) %}
  {% do log("Schema " ~ target.database ~ "." ~ schema_name ~ " dropped", info=True) %}
{% endmacro %}
