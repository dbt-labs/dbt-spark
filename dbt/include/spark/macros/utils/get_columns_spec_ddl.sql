{% macro spark__get_columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
  {% if config.get('constraints_enabled', False) %}
    {%- set ns = namespace(at_least_one_check=False) -%}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns -%}
      {%- set col = user_provided_columns[i] -%}
      {% set constraints = col['constraints'] -%}
      {%- set ns.at_least_one_check = ns.at_least_one_check or col['constraints_check'] %}
      {{ col['name'] }} {{ col['data_type'] }} {% for x in constraints %} {{ x or "" }} {% endfor %} {{ "," if not loop.last }}
    {%- endfor %}
  )
    {%- if ns.at_least_one_check -%}
      {{exceptions.warn("We noticed you have `constraints_check` configs, these are NOT compatible with Snowflake and will be ignored")}}
    {%- endif %}
  {% endif %}
{% endmacro %}