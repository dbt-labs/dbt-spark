{% macro spark_livy__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
