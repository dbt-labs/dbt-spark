{% macro spark__safe_cast(field, type) %}
{%- if cast_from_string_unsupported_for(type) and field is string -%}
    cast({{field.strip('"').strip("'")}} as {{type}})
{%- else -%}
    cast({{field}} as {{type}})
{%- endif -%}
{% endmacro %}

{% macro cast_from_string_unsupported_for(type) %}
    {{ return(type.lower().startswith('struct') or type.lower().startswith('array') or type.lower().startswith('map')) }}
{% endmacro %}
