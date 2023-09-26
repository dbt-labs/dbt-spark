_MODELS__MY_FUN_DOCS = """
{% docs my_fun_doc %}
name Column description "with double quotes"
and with 'single  quotes' as welll as other;
'''abc123'''
reserved -- characters
--
/* comment */
Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting

{% enddocs %}
"""

_MODELS__INCREMENTAL_DELTA = """
{{ config(materialized='incremental', file_format='delta') }}
select 1 as id, 'Joe' as name
"""

_MODELS__TABLE_DELTA_MODEL = """
{{ config(materialized='table', file_format='delta') }}
select 1 as id, 'Joe' as name
"""

_MODELS__VIEW_DELTA_MODEL = """
{{ config(materialized='view') }}
select id, count(*) as count from {{ ref('table_delta_model') }} group by id
"""

_MODELS__TABLE_DELTA_MODEL_MISSING_COLUMN = """
{{ config(materialized='table', file_format='delta') }}
select 1 as id, 'Joe' as different_name
"""
_VIEW_PROPERTIES_MODELS = """
version: 2

models:
  - name: view_delta_model
    description: |
      View model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
"""
_PROPERTIES__MODELS = """
version: 2

models:
  - name: table_delta_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}

  - name: incremental_delta_model
    description: |
      Incremental model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
"""

_PROPERTIES__SEEDS = """
version: 2

seeds:
  - name: seed
    description: |
      Seed model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
"""

_SEEDS__BASIC = """id,name
1,Alice
2,Bob
"""
