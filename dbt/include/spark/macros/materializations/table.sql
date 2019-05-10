{% materialization table, adapter = 'spark' %}
  {%- set identifier = model['alias'] -%}

  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}
  {% if non_destructive_mode %}
    {{ exceptions.raise_compiler_error("--non-destructive mode is not supported on Spark") }}
  {% endif %}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  -- setup: if the target relation already exists, drop it
  {% if old_relation -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

{% endmaterialization %}
