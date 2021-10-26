SELECT 
  {% set cols = adapter.get_columns_in_relation(ref('child')) %}
  {% for col in cols %}
    {{ adapter.quote(col.column) }}{%- if not loop.last %},{{ '\n ' }}{% endif %}
  {% endfor %}
FROM {{ ref('child') }}
