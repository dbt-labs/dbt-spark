import json
import os
import pytest

from dbt.tests.util import run_dbt

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

_SEEDS__BASIC = """
id,name
1,Alice
2,Bob
"""

class TestPersistDocsDelta:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_delta_model.sql": _MODELS__INCREMENTAL_DELTA,
            "my_fun_docs.md": _MODELS__MY_FUN_DOCS,
            "table_delta_model.sql": _MODELS__TABLE_DELTA_MODEL,
            "schema.yml": _PROPERTIES__MODELS
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seeds.csv": _SEEDS__BASIC,
            "seeds.yml": _PROPERTIES__SEEDS
        }


    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'models': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
            'seeds': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                    '+file_format': 'delta',
                    '+quote_columns': True
                }
            },
        }

    def test_delta_comments(self, project):
        run_dbt(['seed'])
        run_dbt(['run'])

        for table, whatis in [
            ('table_delta_model', 'Table'),
            ('seed', 'Seed'),
            ('incremental_delta_model', 'Incremental')
        ]:
            results = run_sql(
                'describe extended {schema}.{table}'.format(schema=project.test_schema, table=table),
                fetch='all'
            )

            for result in results:
                if result[0] == 'Comment':
                    assert result[1].startswith(f'{whatis} model description')
                if result[0] == 'id':
                    assert result[2].startswith('id Column description')
                if result[0] == 'name':
                    assert result[2].startswith('Some stuff here and then a call to')

    # runs on Spark v3.0
    # @use_profile("databricks_cluster")
    # runs on Spark v3.0
    # @use_profile("databricks_sql_endpoint")

