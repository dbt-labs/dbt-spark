{% macro source(source_name, identifier, start_dt = None, end_dt = None) %}
    {%- set relation = builtins.source(source_name, identifier) -%}
    
    {%- if relation.source_meta.python_module or relation.meta.python_module -%}
        {% if start_dt == None %}
            {% set start_dt = modules.datetime.datetime.now() %}
        {% endif %}
        {% if end_dt == None %}
            {% set end_dt = (start_dt + modules.datetime.timedelta(3)) %}
        {% endif %}
        {%- set relation_for_view = relation.load_python_module(start_dt, end_dt, **kwargs) -%}
        {% do return(relation_for_view) %}
        {%- do return(relation) -%}
    {% else -%}
        {%- do return(relation) -%}
    {% endif -%}
{% endmacro %}