{% materialization incremental, adapter='spark' -%}
  
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='append') -%}
  
  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}
  
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% if strategy == 'insert_overwrite' and partition_by %}
    {% call statement() %}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {% endcall %}
  {% endif %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% set build_sql = dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
