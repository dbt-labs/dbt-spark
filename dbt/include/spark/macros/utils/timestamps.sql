{% macro spark__current_timestamp() -%}
    CURRENT_TIMESTAMP()
{%- endmacro %}

{% macro spark__current_timestamp_in_utc() -%}
    {# spark current timestamp impl always returns in UTC #}
    CURRENT_TIMESTAMP()
{%- endmacro %}
