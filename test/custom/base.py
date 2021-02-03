from dbt_adapter_tests import DBTIntegrationTestBase, use_profile

class DBTSparkIntegrationTest(DBTIntegrationTestBase):
    
    def apache_spark_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'spark',
                        'host': 'localhost',
                        'user': 'dbt',
                        'method': 'thrift',
                        'port': '10000',
                        'connect_retries': '5',
                        'connect_timeout': '60',
                        'schema': self.unique_schema()
                    },
                'target': 'default2'
                }
            }
        }

    def databricks_cluster_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'odbc': {
                        'type': 'spark',
                        'method': 'odbc',
                        'host': os.getenv('DBT_DATABRICKS_HOST_NAME'),
                        'cluster': os.getenv('DBT_DATABRICKS_CLUSTER_NAME'),
                        'token': os.getenv('DBT_DATABRICKS_TOKEN'),
                        'port': 443,
                        'schema': self.unique_schema()
                    },
                'target': 'odbc'
                }
            }
        }
        
    def databricks_sql_endpoint_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'default2': {
                        'type': 'spark',
                        'method': 'odbc',
                        'host': os.getenv('DBT_DATABRICKS_HOST_NAME'),
                        'endpoint': os.getenv('DBT_DATABRICKS_ENDPOINT'),
                        'token': os.getenv('DBT_DATABRICKS_TOKEN'),
                        'port': 443,
                        'schema': self.unique_schema()
                    },
                'target': 'default2'
                }
            }
        }
