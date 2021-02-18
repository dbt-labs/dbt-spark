import pytest
from functools import wraps
import os
from dbt_adapter_tests import DBTIntegrationTestBase
import pyodbc


class DBTSparkIntegrationTest(DBTIntegrationTestBase):

    def get_profile(self, adapter_type):
        if adapter_type == 'apache_spark':
            return self.apache_spark_profile()
        elif adapter_type == 'databricks_cluster':
            return self.databricks_cluster_profile()
        elif adapter_type == 'databricks_sql_endpoint':
            return self.databricks_sql_endpoint_profile()
        else:
            raise ValueError('invalid adapter type {}'.format(adapter_type))

    @staticmethod
    def _profile_from_test_name(test_name):
        adapter_names = ('apache_spark', 'databricks_cluster',
                         'databricks_sql_endpoint')
        adapters_in_name = sum(x in test_name for x in adapter_names)
        if adapters_in_name != 1:
            raise ValueError(
                'test names must have exactly 1 profile choice embedded, {} has {}'
                .format(test_name, adapters_in_name)
            )

        for adapter_name in adapter_names:
            if adapter_name in test_name:
                return adapter_name

        raise ValueError(
            'could not find adapter name in test name {}'.format(test_name)
        )

    def run_sql(self, query, fetch='None', kwargs=None, connection_name=None):
        if connection_name is None:
            connection_name = '__test'

        if query.strip() == "":
            return

        sql = self.transform_sql(query, kwargs=kwargs)

        with self.get_connection(connection_name) as conn:
            cursor = conn.handle.cursor()
            try:
                cursor.execute(sql)
                if fetch == 'one':
                    return cursor.fetchall()[0]
                elif fetch == 'all':
                    return cursor.fetchall()
                else:
                    # we have to fetch.
                    cursor.fetchall()
            except pyodbc.ProgrammingError as e:
                # hacks for dropping schema
                if "No results.  Previous SQL was not a query." not in str(e):
                    raise e
            except Exception as e:
                conn.handle.rollback()
                conn.transaction_open = False
                print(sql)
                print(e)
                raise
            else:
                conn.transaction_open = False

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
                        'port': 10000,
                        'connect_retries': 5,
                        'connect_timeout': 60,
                        'schema': self.unique_schema()
                    },
                },
                'target': 'default2'
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
                        'driver': os.getenv('ODBC_DRIVER'),
                        'port': 443,
                        'schema': self.unique_schema()
                    },
                },
                'target': 'odbc'
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
                        'driver': os.getenv('ODBC_DRIVER'),
                        'port': 443,
                        'schema': self.unique_schema()
                    },
                },
                'target': 'default2'
            }
        }


def use_profile(profile_name):
    """A decorator to declare a test method as using a particular profile.
    Handles both setting the nose attr and calling self.use_profile.

    Use like this:

    class TestSomething(DBIntegrationTest):
        @use_profile('postgres')
        def test_postgres_thing(self):
            self.assertEqual(self.adapter_type, 'postgres')

        @use_profile('snowflake')
        def test_snowflake_thing(self):
            self.assertEqual(self.adapter_type, 'snowflake')
    """
    def outer(wrapped):
        @getattr(pytest.mark, 'profile_'+profile_name)
        @wraps(wrapped)
        def func(self, *args, **kwargs):
            return wrapped(self, *args, **kwargs)
        # sanity check at import time
        assert DBTSparkIntegrationTest._profile_from_test_name(
            wrapped.__name__) == profile_name
        return func
    return outer
