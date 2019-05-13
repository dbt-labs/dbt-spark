{% materialization table, adapter = 'spark' %}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

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
