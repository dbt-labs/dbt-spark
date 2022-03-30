{% macro source(source_name, identifier, start_dt = None, end_dt = None) %}
    {%- set relation = builtins.source(source_name, identifier) -%}
    
    {%- if execute and (relation.source_meta.python_module or relation.meta.python_module) -%}
        {%- do relation.load_python_module(start_dt, end_dt) -%}
        {# Return the view name only. Spark view do not support schema and catalog names #}
        {%- do return(relation.identifier) -%}
    {% else -%}
        {%- do return(relation) -%}
    {% endif -%}
{% endmacro %}
