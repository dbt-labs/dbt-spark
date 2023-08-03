{% macro spark__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}

{% macro spark__create_or_replace_clone(this_relation, defer_relation) %}
    create or replace table {{ this_relation }} shallow clone {{ defer_relation }}
{% endmacro %}

{%- materialization clone, adapter='spark' -%}

  {%- set relations = {'relations': []} -%}

  {%- if not defer_relation -%}
      -- nothing to do
      {{ log("No relation found in state manifest for " ~ model.unique_id, info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set existing_relation = load_cached_relation(this) -%}

  {%- if existing_relation and not flags.FULL_REFRESH -%}
      -- noop!
      {{ log("Relation " ~ existing_relation ~ " already exists", info=True) }}
      {{ return(relations) }}
  {%- endif -%}

  {%- set other_existing_relation = load_cached_relation(defer_relation) -%}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}

  -- If this is a database that can do zero-copy cloning of tables, and the other relation is a table, then this will be a table
  -- Otherwise, this will be a view

  {% set can_clone_table = can_clone_table() %}

  {%- if file_format != 'delta' -%}
    {% set invalid_format_msg -%}
      Invalid file format: {{ file_format }}
      shallow clone requires file_format be set to 'delta'
    {%- endset %}
    {% do exceptions.raise_compiler_error(invalid_format_msg) %}
  {%- elif other_existing_relation and other_existing_relation.type == 'table' and can_clone_table -%}

      {%- set target_relation = this.incorporate(type='table') -%}
      {% if existing_relation is not none and not existing_relation.is_table %}
        {{ log("Dropping relation " ~ existing_relation ~ " because it is of type " ~ existing_relation.type) }}
        {{ drop_relation_if_exists(existing_relation) }}
      {% endif %}

      -- as a general rule, data platforms that can clone tables can also do atomic 'create or replace'
      {% call statement('main') %}
          {{ create_or_replace_clone(target_relation, defer_relation) }}
      {% endcall %}

      {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
      {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
      {% do persist_docs(target_relation, model) %}

      {{ return({'relations': [target_relation]}) }}

  {%- else -%}

      {%- set target_relation = this.incorporate(type='view') -%}

      -- reuse the view materialization
      -- TODO: support actual dispatch for materialization macros
      -- Tracking ticket: https://github.com/dbt-labs/dbt-core/issues/7799
      {% set search_name = "materialization_view_" ~ adapter.type() %}
      {% if not search_name in context %}
          {% set search_name = "materialization_view_default" %}
      {% endif %}
      {% set materialization_macro = context[search_name] %}
      {% set relations = materialization_macro() %}
      {{ return(relations) }}
  {% endif %}

{%- endmaterialization -%}
