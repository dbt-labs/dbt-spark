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
    {%- set python_code = py_complete_script(model=model, schema=schema, python_code=sql) -%}
    {{ log("python code " ~ python_code ) }}
    {{adapter.submit_python_job('chenyu.li@dbtlabs.com', identifier, python_code)}}
  {%- else -%}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {%- endcall %}
  {%- endif %}
  
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}

{% macro py_script_prefix( model) %}
# this part is dbt logic for get ref work
{{ build_ref_function(model ) }}
{{ build_source_function(model ) }}

def config(*args, **kwargs):
  pass

class dbt:
  config = config
  ref = ref
  source = source

# COMMAND ----------
# This part of the code is python model code

{% endmacro %}

{% macro py_complete_script(model, schema, python_code) %}
{#-- can we wrap in 'def model:' here? or will formatting screw us? --#}
{#-- Above was Drew's comment --#}
{{ python_code }}

# COMMAND ----------
# this is materialization code
df.write.mode("overwrite").format("delta").saveAsTable("{{schema}}.{{model['alias']}}")

{% endmacro %}
