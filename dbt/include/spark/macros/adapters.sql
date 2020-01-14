{% macro file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- if file_format is not none %}
    using {{ file_format }}
  {%- endif %}
{%- endmacro -%}

{% macro location_clause(label, required=false) %}
  {%- set location = config.get('location', validator=validation.any[basestring]) -%}
  {%- if location is not none %}
    {{ label }} "{{ location }}"
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

{% macro clustered_cols(label, required=false) %}
  {%- set cols = config.get('cluster_by', validator=validation.any[list, basestring]) -%}
  {%- set num_buckets = config.get('num_buckets', validator=validation.any[int]) -%}
  {%- if (cols is not none) and (buckets is not none) %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    ) into {{ num_buckets }} buckets
  {%- endif %}
{%- endmacro -%}

{% macro spark__load_csv_rows(model, agate_table) %}
    {% set cols_sql = ", ".join(agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(10000) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% set _ = bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    %s
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% set _ = adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% set _ = statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{#-- We can't use temporary tables with `create ... as ()` syntax #}
{% macro create_temporary_view(relation, sql) -%}
  create temporary view {{ relation.include(schema=false) }} as
    {{ sql }}
{% endmacro %}

{% macro spark__create_table_as(temporary, relation, sql) -%}
  {% if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    create table {{ relation }}
      {{ file_format_clause() }}
      {{ location_clause(label="location") }}
      {{ partition_cols(label="partitioned by") }}
      {{ clustered_cols(label="clustered by") }}
    as
      {{ sql }}
  {%- endif %}
{%- endmacro -%}

{% macro spark__create_schema(database_name, schema_name) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{schema_name}}
  {% endcall %}
{% endmacro %}

{% macro list_extended_properties(schema, identifier) %}
  {% call statement('list_extended_properties', fetch_result=True) -%}
    describe extended {{ schema }}.{{ identifier }}
  {% endcall %}

  {% do return(load_result('list_extended_properties').table) %}
{% endmacro %}

{% macro spark__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    describe {{ relation }}
  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}

  {% set columns = [] %}
  {% for row in table %}
    {% do columns.append(api.Column(*row)) %}
  {% endfor %}
  {{ return(columns) }}

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

{% macro spark__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    {% if not from_relation.type %}
      {% do exceptions.raise_database_error("Cannot drop a relation with a blank type: " ~ from_relation.identifier) %}
    {% elif from_relation.type in ('table', 'external') %}
        alter table {{ from_relation }} rename to {{ to_relation }}
    {% elif from_relation.type == 'view' %}
        alter view {{ from_relation }} rename to {{ to_relation }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_relation.type ~ "' for relation: " ~ from_relation.identifier) %}
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro spark__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {% if not relation.type %}
      {% do exceptions.raise_database_error("Cannot drop a relation with a blank type: " ~ relation.identifier) %}
    {% elif relation.type in ('table', 'external') %}
        drop table if exists {{ relation }}
    {% elif relation.type == 'view' %}
        drop view if exists {{ relation }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ relation.type ~ "' for relation: " ~ relation.identifier) %}
    {% endif %}
  {%- endcall %}
{% endmacro %}