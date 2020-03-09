{% macro get_insert_overwrite_sql(source_relation, target_relation, partitions) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}

{% materialization incremental, adapter='spark' -%}

  {%- set partitions = config.get('partition_by') -%}
  {% if not partitions %}
    {% do exceptions.raise_compiler_error("Table partitions are required for incremental models on Spark") %}
  {% endif %}

  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['alias'] ~ "__dbt_tmp" -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier, schema=schema, database=database,  type='table') -%}
  {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,  type='table') -%}

  {%- set full_refresh = flags.FULL_REFRESH == True and old_relation is not none -%}
  {%- set old_relation_is_view = old_relation is not none and old_relation.type == 'view' -%}

  {%- if full_refresh or old_relation_is_view -%}
    {{ adapter.drop_relation(old_relation) }}
    {%- set old_relation = none -%}
  {%- endif %}

  {% call statement() %}
    set spark.sql.sources.partitionOverwriteMode = DYNAMIC
  {% endcall %}

  {% call statement() %}
    set spark.sql.hive.convertMetastoreParquet = false
  {% endcall %}

  {{ run_hooks(pre_hooks) }}

  {#-- This is required to make dbt's incremental scheme work #}
  {%- if old_relation is none -%}

    {%- call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}

  {%- else -%}

    {%- call statement('main') -%}
      {{ create_table_as(True, tmp_relation, sql) }}
    {%- endcall -%}

    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {%- call statement('main') -%}
      {{ get_insert_overwrite_sql(tmp_relation, target_relation, partitions) }}
    {%- endcall -%}

  {%- endif %}

  {{ run_hooks(post_hooks) }}

{%- endmaterialization %}
