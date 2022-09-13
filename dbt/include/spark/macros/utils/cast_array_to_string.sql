{% macro spark__cast_array_to_string(array) %}
    '['||concat_ws(',', {{ array }})||']'
{% endmacro %}
