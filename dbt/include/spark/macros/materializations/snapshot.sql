{% macro spark__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        coalesce(cast({{ arg }} as string ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}

{% macro build_snapshot_table(strategy, sql) %}

    select *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.unique_key }} as dbt_unique_key,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}

{% macro snapshot_staging_table_inserts(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *

        from {{ target_relation }}

    ),

    source_data as (

        select *,
            {{ strategy.scd_id }} as dbt_scd_id,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to

        from snapshot_query
    ),

    insertions as (

        select
            source_data.*

        from source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and snapshotted_data.dbt_valid_to is null
            and (
                {{ strategy.row_changed }}
            )
        )

    )

    select * from insertions

{%- endmacro %}


{% macro snapshot_staging_table_updates(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *

        from {{ target_relation }}

    ),

    source_data as (

        select
            *,
            {{ strategy.scd_id }} as dbt_scd_id,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from

        from snapshot_query
    ),

    updates as (

        select
            'update' as dbt_change_type,
            snapshotted_data.dbt_scd_id,
            source_data.dbt_valid_from as dbt_valid_to

        from source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_valid_to is null
        and (
            {{ strategy.row_changed }}
        )

    )

    select * from updates

{%- endmacro %}

{% macro build_snapshot_staging_table_updates(strategy, sql, target_relation) %}
    {% set tmp_update_relation = make_temp_relation(target_relation, '__dbt_tmp_update') %}

    {% set update_select = snapshot_staging_table_updates(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation_updates') %}
        {{ create_table_as(True, tmp_update_relation, update_select) }}
    {% endcall %}

    {% do return(tmp_update_relation) %}
{% endmacro %}

{% macro build_snapshot_staging_table_insert(strategy, sql, target_relation) %}
    {% set tmp_insert_relation = make_temp_relation(target_relation, '__dbt_tmp_insert') %}

    {% set inserts_select = snapshot_staging_table_inserts(strategy, sql, target_relation) %}

    {% call statement('build_snapshot_staging_relation_inserts') %}
        {{ create_table_as(True, tmp_insert_relation, inserts_select) }}
    {% endcall %}


    {% do return(tmp_insert_relation) %}
{% endmacro %}

{% macro spark__snapshot_merge_update_sql(target, source) -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source.include(schema=false) }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
    when matched then update set DBT_INTERNAL_DEST.dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    ;
{% endmacro %}


{% macro spark__snapshot_merge_insert_sql(target, source) -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source.include(schema=false) }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id
    when not matched then insert *
    ;
{% endmacro %}


{% materialization snapshot, adapter='spark' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['injected_sql']) %}
      {% call statement('main') -%}
          {{ create_table_as(False, target_relation, build_sql) }}
      {% endcall %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_insert_table = build_snapshot_staging_table_insert(strategy, sql, target_relation) %}

      {% call statement('main') %}
          {{ spark__snapshot_merge_insert_sql(
                target = target_relation,
                source = staging_insert_table
             )
          }}
      {% endcall %}

      {% set staging_update_table = build_snapshot_staging_table_updates(strategy, sql, target_relation) %}

      {% call statement('main-2') %}
          {{ spark__snapshot_merge_update_sql(
                target = target_relation,
                source = staging_update_table
             )
          }}
      {% endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}