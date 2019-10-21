
{#-- We can't use temporary tables with `create ... as ()` syntax #}
{% macro spark_create_temporary_view(relation, sql) -%}
  create temporary view {{ relation.include(database=false, schema=false) }} as
    {{ sql }}
{% endmacro %}

{% macro file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- if file_format is not none %}
    using {{ file_format }}
  {%- endif %}
{%- endmacro -%}

{% macro partition_cols(label, required=false) %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro spark__create_table_as(temporary, relation, sql) -%}
  {% if temporary -%}
    {{ spark_create_temporary_view(relation, sql) }}
  {%- else -%}
    create table {{ relation }}
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}

{% macro spark__create_view_as(relation, sql) -%}
  create view {{ relation }} as
    {{ sql }}
{% endmacro %}

{% macro spark__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      describe {{ relation }}
  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}

{% endmacro %}


{% macro spark__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    show tables in {{ schema }}
  {% endcall %}

  {% do return(load_result('list_relations_without_caching').table) %}
{% endmacro %}


{% macro spark__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro spark__current_timestamp() -%}
  current_timestamp()
{%- endmacro %}


{% macro spark_get_relation_type(relation) -%}
  {% call statement('get_relation_type', fetch_result=True) -%}
    SHOW TBLPROPERTIES {{ relation }} ('view.default.database')
  {%- endcall %}
  {% set res = load_result('get_relation_type').table %}
  {% if 'does not have property' in res[0][0] %}
    {{ return('table') }}
  {% else %}
    {{ return('view') }}
  {% endif %}
{%- endmacro %}


{% macro spark__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    {% if not from_relation.type %}
      {% do exceptions.raise_database_error("Cannot rename a relation with an unknown type: " ~ from_relation) %}
    {% elif from_relation.type == 'table' %}
        alter table {{ from_relation }} rename to {{ to_relation }}
    {% elif from_relation.type == 'view' %}
        alter view {{ from_relation }} rename to {{ to_relation }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_relation.type ~ "' for relation: " ~ from_relation) %}
    {% endif %}
  {%- endcall %}
{% endmacro %}


{% macro spark__drop_relation(relation) -%}
  {% set type = relation.type if relation.type is not none else spark_get_relation_type(relation) %}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}
