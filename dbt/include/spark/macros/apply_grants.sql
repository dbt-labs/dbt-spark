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
    {%- for privilege in grant_config.keys() -%}
        {%- set grantees = grant_config[privilege] -%}
        {%- if grantees -%}
            {%- for grantee in grantees -%}
                grant {{ privilege }} on {{ relation }} to {{ adapter.quote(grantee) }};
            {% endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro %}

{% macro spark__get_revoke_sql(relation, grant_config) %}
    {%- for privilege in grant_config.keys() -%}
        {%- set grantees = grant_config[privilege] -%}
        {%- if grantees -%}
            {%- for grantee in grantees -%}
                revoke {{ privilege }} on {{ relation }} from {{ adapter.quote(grantee) }};
            {% endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}


{% macro spark__apply_grants(relation, grant_config, should_revoke=True) %}
    {% if grant_config %}
        {% if should_revoke %}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = diff_of_two_dicts(grant_config, current_grants_dict) %}
            {% set needs_revoking = diff_of_two_dicts(current_grants_dict, grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {% set needs_revoking = {} %}
            {% set needs_granting = grant_config %}
        {% endif %}
        {#-- On Spark, each semicolon-delimited statement needs to be submitted separately --#}
        {#-- This is pretty jank -- should get_grant_sql + get_revoke_sql return lists of statements instead? --#}
        {% if needs_granting or needs_revoking %}
            {% set grant_and_revoke_sql = get_revoke_sql(relation, needs_revoking)
                                          + get_grant_sql(relation, needs_granting) %}
            {% set split_statements = grant_and_revoke_sql.split(';')[:-1] %}
            {% for grant_or_revoke in split_statements %}
              {% call statement('manage_grants') %}
                  {{ grant_or_revoke }}
              {% endcall %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}
