{% materialization view, adapter = 'spark' %}
  
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='view') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  {% if old_relation -%}
    {{ adapter.drop_relation(old_relation) }}
  {%- endif %}

  -- build model
  {% call statement('main') -%}
    {{ create_view_as(target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

{%- endmaterialization -%}
