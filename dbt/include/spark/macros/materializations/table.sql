{% materialization table, adapter = 'spark', supported_languages=['sql', 'python'] %}
  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set grant_config = config.get('grants') -%}

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

  {%- call statement('main', language=language) -%}
    {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall -%}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}


{% macro py_write_table(compiled_code, target_relation) %}
{{ compiled_code }}
dbt = dbtObj(spark.table)
df = model(dbt, spark)

# COMMAND ----------
# this is materialization code dbt generated, please do not modify

# make sure pandas exists
import importlib.util
package_name = 'pandas'
if importlib.util.find_spec(package_name):
    import pandas
    if isinstance(df, pandas.core.frame.DataFrame):
      # convert to pyspark.DataFrame
      df = spark.createDataFrame(df)

df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("{{ target_relation }}")
{%- endmacro -%}

{%macro py_script_comment()%}
# how to execute python model in notebook
# dbt = dbtObj(spark.table)
# df = model(dbt, spark)
{%endmacro%}
