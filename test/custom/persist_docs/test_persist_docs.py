from cProfile import run
from test.custom.base import DBTSparkIntegrationTest, use_profile
import dbt.exceptions

import json


class TestPersistDocsDelta(DBTSparkIntegrationTest):
    @property
    def schema(self):
        return "persist_docs_columns"
        
    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
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

    def test_delta_comments(self):
        self.run_dbt(['seed'])
        self.run_dbt(['run'])
        
        for table in ['table_delta_model', 'seed']:
            results = self.run_sql(
                'describe extended {schema}.{table}'.format(schema=self.unique_schema(), table=table),
                fetch='all'
            )
            
            for result in results:
                if result[0] == 'Comment':
                    whatis = 'Seed' if table == 'seed' else 'Table'
                    assert result[1].startswith(f'{whatis} model description')
                if result[0] == 'id':
                    assert result[2].startswith('id Column description')
                if result[0] == 'name':
                    assert result[2].startswith('Some stuff here and then a call to')

    # runs on Spark v3.0
    @use_profile("databricks_cluster")
    def test_delta_comments_databricks_cluster(self):
        self.test_delta_comments()

    # runs on Spark v3.0
    @use_profile("databricks_sql_endpoint")
    def test_delta_comments_databricks_sql_endpoint(self):
        self.test_delta_comments()
