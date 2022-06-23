{% materialization view, adapter='spark_livy' -%}
    {{ return(create_or_replace_view()) }}
{%- endmaterialization %}
