{% macro spark__copy_grants() %}

    {% if config.materialized == 'view' %}
        {#-- Spark views don't copy grants when they're replaced --#}
        {{ return(False) }}

    {% else %}
      {#-- This depends on how we're replacing the table, which depends on its file format
        -- Just play it safe by assuming that grants have been copied over, and need to be checked / possibly revoked
        -- We can make this more efficient in the future
      #}
        {{ return(True) }}

    {% endif %}
{% endmacro %}

{%- macro spark__get_grant_sql(relation, grant_config) -%}
    {%- set grant_statements = [] -%}
    {%- for privilege in grant_config.keys() %}
        {%- set grantees = grant_config[privilege] -%}
        {%- for grantee in grantees -%}
            {% set grant_sql -%}
                grant {{ privilege }} on {{ relation }} to {{ adapter.quote(grantee) }}
            {%- endset %}
            {%- do grant_statements.append(grant_sql) -%}
        {% endfor -%}
    {%- endfor -%}
    {{ return(grant_statements) }}
{%- endmacro %}

{%- macro spark__get_revoke_sql(relation, grant_config) -%}
    {%- set revoke_statements = [] -%}
    {%- for privilege in grant_config.keys() %}
        {%- set grantees = grant_config[privilege] -%}
        {%- for grantee in grantees -%}
            {% set grant_sql -%}
                revoke {{ privilege }} on {{ relation }} from {{ adapter.quote(grantee) }}
            {%- endset %}
            {%- do revoke_statements.append(grant_sql) -%}
        {% endfor -%}
    {%- endfor -%}
    {{ return(revoke_statements) }}
{%- endmacro %}


{% macro spark__call_grant_revoke_statement_list(grant_and_revoke_statement_list) %}
    {% for grant_or_revoke_statement in grant_and_revoke_statement_list %}
        {% call statement('grant_or_revoke') %}
            {{ grant_or_revoke_statement }}
        {% endcall %}
    {% endfor %}
{% endmacro %}
