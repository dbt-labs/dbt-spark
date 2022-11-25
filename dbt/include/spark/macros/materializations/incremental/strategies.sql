{% macro get_insert_overwrite_sql(source_relation, target_relation, existing_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {% if existing_relation.is_iceberg %}
      {# removed table from statement for iceberg #}
      insert overwrite {{ target_relation }}
      {# removed partition_cols for iceberg as well #}
    {% else %}
      insert overwrite table {{ target_relation }}
      {{ partition_cols(label="partition") }}
    {% endif %}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}


{% macro spark__get_merge_sql(target, source, unique_key, dest_columns, predicates=none) %}
  {# need dest_columns for merge_exclude_columns, default to use "*" #}
  {%- set predicates = [] if predicates is none else [] + predicates -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {% set merge_condition %}
    {% if unique_key %}
        {# added support for multiple join condition, multiple unique_key #}
        on  {% if unique_key is string %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% else %}
              {%- for k in unique_key %}
                DBT_INTERNAL_SOURCE.{{ k }} = DBT_INTERNAL_DEST.{{ k }}
                {%- if not loop.last %} AND {%- endif %}
              {%- endfor %}
            {% endif %}
    {% else %}
        on false
    {% endif %}
  {% endset %}

  merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source.include(schema=false) }} as DBT_INTERNAL_SOURCE

    {{ merge_condition }}

      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}

      when not matched then insert *
{% endmacro %}


{% macro dbt_spark_get_incremental_sql(strategy, source, target, existing, unique_key) %}
  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target, existing) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns with databricks delta or iceberg - schema changes are handled for us #}
    {{ get_merge_sql(target, source, unique_key, dest_columns=none, predicates=none) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}
