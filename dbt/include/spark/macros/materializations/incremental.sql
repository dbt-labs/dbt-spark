{% macro get_insert_overwrite_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation.include(database=false, schema=false) }}

{% endmacro %}

{% macro dbt_spark_validate_get_file_format() %}
  {#-- Find and validate the file format #}
  {%- set file_format = config.get("file_format", default="parquet") -%}

  {% set invalid_file_format_msg -%}
    Invalid file format provided: {{ file_format }}
    Expected one of: 'text', 'csv', 'json', 'jdbc', 'parquet', 'orc', 'hive', 'delta', 'libsvm'
  {%- endset %}

  {% if file_format not in ['text', 'csv', 'json', 'jdbc', 'parquet', 'orc', 'hive', 'delta', 'libsvm'] %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% do return(file_format) %}
{% endmacro %}

{% macro dbt_spark_validate_get_incremental_strategy(file_format) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="insert_overwrite") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'insert_overwrite'
  {%- endset %}

  {% set invalid_merge_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    You can only choose this strategy when file_format is set to 'delta'
  {%- endset %}

  {% if strategy not in ['merge', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {%-else %}
    {% if strategy == 'merge' and file_format != 'delta' %}
      {% do exceptions.raise_compiler_error(invalid_merge_msg) %}
    {% endif %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro dbt_spark_validate_merge(file_format) %}
  {% set invalid_file_format_msg -%}
    You can only choose the 'merge' incremental_strategy when file_format is set to 'delta'
  {%- endset %}

  {% if file_format != 'delta' %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

{% endmacro %}

{% macro get_merge_sql(source, target, unique_key) %}
    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source.include(schema=false) }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
    when matched then update set *
    when not matched then insert *
{% endmacro %}

{% macro dbt_spark_get_incremental_sql(strategy, source, target, unique_key) %}
  {%- if strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target) }}
  {%- else -%}
    {#-- merge all columns with databricks delta - schema changes are handled for us #}
    {{ get_merge_sql(source, target, unique_key) }}
  {%- endif -%}

{% endmacro %}


{% materialization incremental, adapter='spark' -%}
  {#-- Validate early so we don't run SQL if the file_format is invalid --#}
  {% set file_format = dbt_spark_validate_get_file_format() -%}
  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_spark_validate_get_incremental_strategy(file_format) -%}

  {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {% if strategy == 'merge' %}
    {%- set unique_key = config.require('unique_key') -%}
    {% do dbt_spark_validate_merge(file_format) %}
  {% endif %}

  {% call statement() %}
    set spark.sql.sources.partitionOverwriteMode = DYNAMIC
  {% endcall %}

  {% call statement() %}
    set spark.sql.hive.convertMetastoreParquet = false
  {% endcall %}

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
