{% materialization table, adapter = 'spark' %}

  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- in case if the existing and future table is delta, we want to do a
  -- create or replace table instead of dropping, so we don't have the table unavailable
  {% if old_relation and not (old_relation.is_delta and config.get('file_format', validator=validation.any[basestring]) == 'delta') -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  -- build model
  {% if config.get('language', 'sql') == 'python' -%}}
    -- sql here is really just the compiled python code
    {%- set python_code = py_complete_script(python_code=sql, target_relation=target_relation) -%}
    {{ log("python code " ~ python_code ) }}
    {% set result = adapter.submit_python_job(schema, identifier, python_code) %}
    {% call noop_statement('main', result, 'OK', 1) %}
      -- python model return run result --
    {% endcall %}

  {%- else -%}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}
  {%- endif %}
  
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}


{% macro py_complete_script(python_code, target_relation) %}
{{ python_code }}

df = model(dbt)

# COMMAND ----------
# this is materialization code dbt generated, please do not modify

df.write.mode("overwrite").format("delta").saveAsTable("{{ target_relation }}")
{% endmacro %}
