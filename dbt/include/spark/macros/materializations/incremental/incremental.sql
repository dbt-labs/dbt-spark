{% materialization incremental, adapter='spark' -%}

  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='parquet') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='append') -%}
  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}  
  {%- set language = config.get('language') -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(this) -%}

  {%- if strategy == 'insert_overwrite' and partition_by -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {%- endcall -%}
  {%- endif -%}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Incremental run logic --#}




  {% if existing_relation is none %}
    {{ log("#-- Relation must be created --#") }}
    {% if language == 'sql'%}
      {%- call statement('main') -%}
        {{ create_table_as(False, target_relation, sql) }}
      {%- endcall -%}
    {% elif language == 'python' %}
      {%- set python_code = py_complete_script(python_code=sql, target_relation=target_relation) -%}
      {{ log("python code: " ~ python_code ) }}
      {% set result = adapter.submit_python_job(schema, model['alias'], python_code) %}
      {% call noop_statement('main', result, 'OK', 1) %}
        -- python model return run result --
      {% endcall %}
    {% endif %}
  {% elif existing_relation.is_view or should_full_refresh() %}
    {{ log("#-- Relation must be dropped & recreated --#") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% if language == 'sql'%}
      {%- call statement('main') -%}
        {{ create_table_as(False, target_relation, sql) }}
      {%- endcall -%}
    {% elif language == 'python' %}
      {%- set python_code = py_complete_script(python_code=sql, target_relation=target_relation) -%}
      {{ log("python code " ~ python_code ) }}
      {% set result = adapter.submit_python_job(schema, model['alias'], python_code) %}
      {% call noop_statement('main', result, 'OK', 1) %}
        -- python model return run result --
      {% endcall %}
    {% endif %}
  {% else %}
    {{ log("#-- Relation must be merged --#") }}
    {% if language == 'sql'%}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
      {%- call statement('main') -%}
        {{ dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) }}
      {%- endcall -%}
    {% elif language == 'python' %}
      {%- set python_code = py_complete_script(python_code=sql, target_relation=tmp_relation) -%}
      {% set result = adapter.submit_python_job(schema, model['alias'], python_code) %}
      {{ log("python code " ~ python_code ) }}
      {% call noop_statement('main', result, 'OK', 1) %}
        -- python model return run result --
      {% endcall %}
      {{ log("XXXXXX-" ~ result) }}
      {% do process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
      {%- call statement('main') -%}
        {{ dbt_spark_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key) }}
      {%- endcall -%}
    {% endif %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}
  
  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
