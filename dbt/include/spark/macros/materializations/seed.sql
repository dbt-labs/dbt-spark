{% macro spark__load_csv_rows(model, agate_table) %}
    {% set batch_size = 1000 %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(batch_size) %}
        {% set bindings = [] %}

        {% for row in chunk %}
          {% do bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} values
            {% for row in chunk -%}
                ({%- for column in agate_table.columns -%}
                    {%- if 'ISODate' in (column.data_type | string) -%}
                      cast(%s as timestamp)
                    {%- else -%}
                    %s
                    {%- endif -%}
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% do adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% do statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}

{% macro spark__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% if old_relation %}
        {{ adapter.drop_relation(old_relation) }}
    {% endif %}
    {% set sql = create_csv_table(model, agate_table) %}
    {{ return(sql) }}
{% endmacro %}


{% macro spark__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ location_clause() }}
    {{ comment_clause() }}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}


{% materialization seed, adapter='spark' %}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier,
                                               type='table') -%}
  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% set status = 'CREATE' %}
  {% set num_rows = (agate_table.rows | length) %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call noop_statement('main', status ~ ' ' ~ num_rows) %}
    {{ create_table_sql }};
    -- dbt seed --
    {{ sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
