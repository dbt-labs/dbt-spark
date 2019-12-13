{% materialization table, adapter = 'spark' %}
  
  {%- set identifier = model['alias'] -%}

  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  -- setup: if the target relation already exists, drop it
  -- If the target relation already exists, drop it
  {{ adapter.drop_relation(target_relation) }}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

{% endmaterialization %}
