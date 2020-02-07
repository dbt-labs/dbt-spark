{% macro get_delta_tables() %}
  {% set delta_tables = [] %}
  {% for database in spark__list_schemas('not_used') %}
    {% for table in spark__list_relations_without_caching('not_used', database[0]) %}
      {% set db_tablename = database[0] ~ "." ~ table[1] %}
      {% call statement('table_detail', fetch_result=True) -%}
        describe extended {{ db_tablename }}
      {% endcall %}

      {% for row in load_result('table_detail').table %}
        {% if row[0] == 'Provider' and row[1] == 'DELTA' %}
          {{ delta_tables.append(db_tablename) }}
        {% endif %}
      {% endfor %}
    {% endfor %}
  {% endfor %}
  {{ return(delta_tables) }}
{% endmacro %}

{% macro get_tables() %}
  {% set tables = [] %}
  {% for database in spark__list_schemas('not_used') %}
    {% for table in spark__list_relations_without_caching('not_used', database[0]) %}
      {% set db_tablename = database[0] ~ "." ~ table[1] %}
      {% if spark_get_relation_type(db_tablename) == 'table' %}
          {{ tables.append(db_tablename) }}
      {% endif %}
    {% endfor %}
  {% endfor %}
  {{ return(tables) }}
{% endmacro %}

{% macro get_statistic_columns(table) %}
  {% set input_columns = spark__get_columns_in_relation(table) %}
  {% set output_columns = [] %}
  
  {% for column in input_columns %}
    {% if not column.dtype.startswith('struct') %}
      {{ output_columns.append(column.name) }}
    {% endif %}
  {% endfor %}
  {{ return(output_columns) }}
{% endmacro %}

{% macro spark_maintenance() %}

    {% for table in get_delta_tables() %}
        {% set start=modules.datetime.datetime.now() %}
        {% set message_prefix=loop.index ~ " of " ~ loop.length %}
        {{ dbt_utils.log_info(message_prefix ~ " Optimizing " ~ table) }}
        {% do run_query("optimize " ~ table) %}
        {% set end=modules.datetime.datetime.now() %}
        {% set total_seconds = (end - start).total_seconds() | round(2)  %}
        {{ dbt_utils.log_info(message_prefix ~ " Finished " ~ table ~ " in " ~ total_seconds ~ "s") }}
    {% endfor %}

{% endmacro %}

{% macro spark_statistics() %}

    {% for table in get_tables() %}
        {% set columns = get_statistic_columns(table) | join(',') %}
        {% set start=modules.datetime.datetime.now() %}
        {% set message_prefix=loop.index ~ " of " ~ loop.length %}
        {{ dbt_utils.log_info(message_prefix ~ " Analyzing " ~ table) }}
        {% do run_query("analyze table " ~ table ~ " compute statistics for columns " ~ columns) %}
        {% set end=modules.datetime.datetime.now() %}
        {% set total_seconds = (end - start).total_seconds() | round(2)  %}
        {{ dbt_utils.log_info(message_prefix ~ " Finished " ~ table ~ " in " ~ total_seconds ~ "s") }}
    {% endfor %}

{% endmacro %}