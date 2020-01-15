{% macro spark__load_csv_rows(model, agate_table) %}
    {% set cols_sql = ", ".join(agate_table.column_names) %}
    {% set bindings = [] %}

    {% set statements = [] %}

    {% for chunk in agate_table.rows | batch(10000) %}
        {% set bindings = [] %}

        {% for row in chunk %}
            {% set _ = bindings.extend(row) %}
        {% endfor %}

        {% set sql %}
            insert into {{ this.render() }} values
            {% for row in chunk -%}
                ({%- for column in agate_table.column_names -%}
                    %s
                    {%- if not loop.last%},{%- endif %}
                {%- endfor -%})
                {%- if not loop.last%},{%- endif %}
            {%- endfor %}
        {% endset %}

        {% set _ = adapter.add_query(sql, bindings=bindings, abridge_sql_log=True) %}

        {% if loop.index0 == 0 %}
            {% set _ = statements.append(sql) %}
        {% endif %}
    {% endfor %}

    {# Return SQL so we can render it out into the compiled files #}
    {{ return(statements[0]) }}
{% endmacro %}