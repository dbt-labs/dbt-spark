SELECT 
  {% set cols = adapter.get_columns_in_relation(ref('child')) %}
  {% for col in cols %}
    {{ adapter.quote(col) }}{%- if not loop.last %},{{ '\n ' }}{% endif %}
  {% for col in cols %}
FROM {{ ref('child') }}
