_MACRO_TEST_IS_TYPE_SQL = """
{% macro simple_type_check_column(column, check) %}
    {% set checks = {
        'string': column.is_string,
        'float': column.is_float,
        'number': column.is_number,
        'numeric': column.is_numeric,
        'integer': column.is_integer,
    } %}
    {% if check not in checks %}
        {% do exceptions.raise_compiler_error('invalid type check value: ' ~ check) %}
    {% endif %}
    {{ return(checks[check]()) }}
{% endmacro %}

{% macro type_check_column(column, type_checks) %}
    {% set failures = [] %}
    {% for type_check in type_checks %}
        {% if type_check.startswith('not ') %}
            {% if simple_type_check_column(column, type_check[4:]) %}
                {% do log('simple_type_check_column got ', True) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% else %}
            {% if not simple_type_check_column(column, type_check) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% if (failures | length) > 0 %}
        {% do log('column ' ~ column.name ~ ' had failures: ' ~ failures, info=True) %}
    {% endif %}
    {% do return((failures | length) == 0) %}
{% endmacro %}

{% macro is_bad_column(column, column_map) %}
    {% set column_key = (column.name | lower) %}
    {% if column_key not in column_map %}
        {% do exceptions.raise_compiler_error('column key ' ~ column_key ~ ' not found in ' ~ (column_map | list | string)) %}
    {% endif %}

    {% set type_checks = column_map[column_key] %}
    {% if not type_checks %}
        {% do exceptions.raise_compiler_error('no type checks?') %}
    {% endif %}

    {{ return(not type_check_column(column, type_checks)) }}
{% endmacro %}

{% test is_type(seed, column_map) %}
    {% if not execute %}
        {{ return(None) }}
    {% endif %}
    {% if not column_map %}
        {% do exceptions.raise_compiler_error('test_is_type must have a column name') %}
    {% endif %}
    {% set columns = adapter.get_columns_in_relation(seed) %}
    {% if (column_map | length) != (columns | length) %}
        {% set column_map_keys = (column_map | list | string) %}
        {% set column_names = (columns | map(attribute='name') | list | string) %}
        {% do exceptions.raise_compiler_error('did not get all the columns/all columns not specified:\n' ~ column_map_keys ~ '\nvs\n' ~ column_names) %}
    {% endif %}
    {% set bad_columns = [] %}
    {% for column in columns %}
        {% if is_bad_column(column, column_map) %}
            {% do bad_columns.append(column.name) %}
        {% endif %}
    {% endfor %}
    {% do log('bad columns: ' ~ bad_columns, info=True) %}
    {% for bad_column in bad_columns %}
      select '{{ bad_column }}' as bad_column
      {{ 'union all' if not loop.last }}
    {% endfor %}
      select * from (select 1 limit 0) as nothing
{% endtest %}
"""

_SEED_CSV = """
ID,ORDERID,PAYMENTMETHOD,STATUS,AMOUNT,AMOUNT_USD,CREATED
1,1,credit_card,success,1000,10.00,2018-01-01
2,2,credit_card,success,2000,20.00,2018-01-02
3,3,coupon,success,100,1.00,2018-01-04
4,4,coupon,success,2500,25.00,2018-01-05
5,5,bank_transfer,fail,1700,17.00,2018-01-05
6,5,bank_transfer,success,1700,17.00,2018-01-05
7,6,credit_card,success,600,6.00,2018-01-07
8,7,credit_card,success,1600,16.00,2018-01-09
9,8,credit_card,success,2300,23.00,2018-01-11
10,9,gift_card,success,2300,23.00,2018-01-12
"""

_SEED_YML = """
version: 2

seeds:
  - name: payments
    config:
        column_types:
            id: string
            orderid: string
            paymentmethod: string
            status: string
            amount: integer
            amount_usd: decimal(20,2)
            created: timestamp
    tests:
        - is_type:
            column_map:
                id: ["string", "not number"]
                orderid: ["string", "not number"]
                paymentmethod: ["string", "not number"]
                status: ["string", "not number"]
                amount: ["integer", "number"]
                amount_usd: ["decimal", "number"]
                created: ["timestamp", "string"]
"""
