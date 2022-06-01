import json
import os
import io
import random
import shutil
import sys
import tempfile
import traceback
import unittest
from contextlib import contextmanager
from datetime import datetime
from functools import wraps

import pyodbc
import pytest
import yaml
from unittest.mock import patch

import dbt.main as dbt
from dbt import flags
from dbt.deprecations import reset_deprecations
from dbt.adapters.factory import get_adapter, reset_adapters, register_adapter
from dbt.clients.jinja import template_cache
from dbt.config import RuntimeConfig
from dbt.context import providers
from dbt.logger import log_manager
from dbt.events.functions import (
    capture_stdout_logs, fire_event, setup_event_logger, stop_capture_stdout_logs
)
from dbt.events import AdapterLogger
from dbt.contracts.graph.manifest import Manifest

logger = AdapterLogger("Spark")

INITIAL_ROOT = os.getcwd()


def normalize(path):
    """On windows, neither is enough on its own:

    >>> normcase('C:\\documents/ALL CAPS/subdir\\..')
    'c:\\documents\\all caps\\subdir\\..'
    >>> normpath('C:\\documents/ALL CAPS/subdir\\..')
    'C:\\documents\\ALL CAPS'
    >>> normpath(normcase('C:\\documents/ALL CAPS/subdir\\..'))
    'c:\\documents\\all caps'
    """
    return os.path.normcase(os.path.normpath(path))


class Normalized:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return f'Normalized({self.value!r})'

    def __str__(self):
        return f'Normalized({self.value!s})'

    def __eq__(self, other):
        return normalize(self.value) == normalize(other)


class FakeArgs:
    def __init__(self):
        self.threads = 1
        self.defer = False
        self.full_refresh = False
        self.models = None
        self.select = None
        self.exclude = None
        self.single_threaded = False
        self.selector_name = None
        self.state = None
        self.defer = None


class TestArgs:
    __test__ = False

    def __init__(self, kwargs):
        self.which = 'run'
        self.single_threaded = False
        self.profiles_dir = None
        self.project_dir = None
        self.__dict__.update(kwargs)


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


def _pytest_test_name():
    return os.environ['PYTEST_CURRENT_TEST'].split()[0]


def _pytest_get_test_root():
    test_path = _pytest_test_name().split('::')[0]
    relative_to = INITIAL_ROOT
    head = os.path.relpath(test_path, relative_to)

    path_parts = []
    while head:
        head, tail = os.path.split(head)
        path_parts.append(tail)
    path_parts.reverse()
    # dbt tests are all of the form 'tests/integration/suite_name'
    target = os.path.join(*path_parts[:3])  # TODO: try to not hard code this
    return os.path.join(relative_to, target)


def _really_makedirs(path):
    while not os.path.exists(path):
        try:
            os.makedirs(path)
        except EnvironmentError:
            raise


class DBTIntegrationTest(unittest.TestCase):
    CREATE_SCHEMA_STATEMENT = 'CREATE SCHEMA {}'
    DROP_SCHEMA_STATEMENT = 'DROP SCHEMA IF EXISTS {} CASCADE'

    _randint = random.randint(0, 9999)
    _runtime_timedelta = (datetime.utcnow() - datetime(1970, 1, 1, 0, 0, 0))
    _runtime = (
        (int(_runtime_timedelta.total_seconds() * 1e6)) +
        _runtime_timedelta.microseconds
    )

    prefix = f'test{_runtime}{_randint:04}'
    setup_alternate_db = False

    def apache_spark_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'thrift': {
                        'type': 'spark',
                        'host': 'localhost',
                        'user': 'dbt',
                        'method': 'thrift',
                        'port': 10000,
                        'connect_retries': 3,
                        'connect_timeout': 5,
                        'retry_all': True,
                        'schema': self.unique_schema()
                    },
                },
                'target': 'thrift'
            }
        }

    def databricks_cluster_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'cluster': {
                        'type': 'spark',
                        'method': 'odbc',
                        'host': os.getenv('DBT_DATABRICKS_HOST_NAME'),
                        'cluster': os.getenv('DBT_DATABRICKS_CLUSTER_NAME'),
                        'token': os.getenv('DBT_DATABRICKS_TOKEN'),
                        'driver': os.getenv('ODBC_DRIVER'),
                        'port': 443,
                        'connect_retries': 3,
                        'connect_timeout': 5,
                        'retry_all': True,
                        'schema': self.unique_schema()
                    },
                },
                'target': 'cluster'
            }
        }

    def databricks_sql_endpoint_profile(self):
        return {
            'config': {
                'send_anonymous_usage_stats': False
            },
            'test': {
                'outputs': {
                    'endpoint': {
                        'type': 'spark',
                        'method': 'odbc',
                        'host': os.getenv('DBT_DATABRICKS_HOST_NAME'),
                        'endpoint': os.getenv('DBT_DATABRICKS_ENDPOINT'),
                        'token': os.getenv('DBT_DATABRICKS_TOKEN'),
                        'driver': os.getenv('ODBC_DRIVER'),
                        'port': 443,
                        'connect_retries': 3,
                        'connect_timeout': 5,
                        'retry_all': True,
                        'schema': self.unique_schema()
                    },
                },
                'target': 'endpoint'
            }
        }

    @property
    def packages_config(self):
        return None

    @property
    def selectors_config(self):
        return None

    def unique_schema(self):
        schema = self.schema

        to_return = "{}_{}".format(self.prefix, schema)

        return to_return.lower()

    @property
    def default_database(self):
        database = self.config.credentials.database
        return database

    @property
    def alternative_database(self):
        return None

    def get_profile(self, adapter_type):
        if adapter_type == 'apache_spark':
            return self.apache_spark_profile()
        elif adapter_type == 'databricks_cluster':
            return self.databricks_cluster_profile()
        elif adapter_type == 'databricks_sql_endpoint':
            return self.databricks_sql_endpoint_profile()
        else:
            raise ValueError('invalid adapter type {}'.format(adapter_type))

    def _pick_profile(self):
        test_name = self.id().split('.')[-1]
        return _profile_from_test_name(test_name)

    def _symlink_test_folders(self):
        for entry in os.listdir(self.test_original_source_path):
            src = os.path.join(self.test_original_source_path, entry)
            tst = os.path.join(self.test_root_dir, entry)
            if os.path.isdir(src) or src.endswith('.sql'):
                # symlink all sql files and all directories.
                os.symlink(src, tst)
        os.symlink(self._logs_dir, os.path.join(self.test_root_dir, 'logs'))

    @property
    def test_root_realpath(self):
        if sys.platform == 'darwin':
            return os.path.realpath(self.test_root_dir)
        else:
            return self.test_root_dir

    def _generate_test_root_dir(self):
        return normalize(tempfile.mkdtemp(prefix='dbt-int-test-'))

    def setUp(self):
        self.dbt_core_install_root = os.path.dirname(dbt.__file__)
        log_manager.reset_handlers()
        self.initial_dir = INITIAL_ROOT
        os.chdir(self.initial_dir)
        # before we go anywhere, collect the initial path info
        self._logs_dir = os.path.join(self.initial_dir, 'logs', self.prefix)
        setup_event_logger(self._logs_dir)
        _really_makedirs(self._logs_dir)
        self.test_original_source_path = _pytest_get_test_root()
        self.test_root_dir = self._generate_test_root_dir()

        os.chdir(self.test_root_dir)
        try:
            self._symlink_test_folders()
        except Exception as exc:
            msg = '\n\t'.join((
                'Failed to symlink test folders!',
                'initial_dir={0.initial_dir}',
                'test_original_source_path={0.test_original_source_path}',
                'test_root_dir={0.test_root_dir}'
            )).format(self)
            logger.exception(msg)

            # if logging isn't set up, I still really want this message.
            print(msg)
            traceback.print_exc()

            raise

        self._created_schemas = set()
        reset_deprecations()
        template_cache.clear()

        self.use_profile(self._pick_profile())
        self.use_default_project()
        self.set_packages()
        self.set_selectors()
        self.load_config()

    def use_default_project(self, overrides=None):
        # create a dbt_project.yml
        base_project_config = {
            'name': 'test',
            'version': '1.0',
            'config-version': 2,
            'test-paths': [],
            'source-paths': [self.models],
            'profile': 'test',
        }

        project_config = {}
        project_config.update(base_project_config)
        project_config.update(self.project_config)
        project_config.update(overrides or {})

        with open("dbt_project.yml", 'w') as f:
            yaml.safe_dump(project_config, f, default_flow_style=True)

    def use_profile(self, adapter_type):
        self.adapter_type = adapter_type

        profile_config = {}
        default_profile_config = self.get_profile(adapter_type)

        profile_config.update(default_profile_config)
        profile_config.update(self.profile_config)

        if not os.path.exists(self.test_root_dir):
            os.makedirs(self.test_root_dir)

        flags.PROFILES_DIR = self.test_root_dir
        profiles_path = os.path.join(self.test_root_dir, 'profiles.yml')
        with open(profiles_path, 'w') as f:
            yaml.safe_dump(profile_config, f, default_flow_style=True)
        self._profile_config = profile_config

    def set_packages(self):
        if self.packages_config is not None:
            with open('packages.yml', 'w') as f:
                yaml.safe_dump(self.packages_config, f, default_flow_style=True)

    def set_selectors(self):
        if self.selectors_config is not None:
            with open('selectors.yml', 'w') as f:
                yaml.safe_dump(self.selectors_config, f, default_flow_style=True)

    def load_config(self):
        # we've written our profile and project. Now we want to instantiate a
        # fresh adapter for the tests.
        # it's important to use a different connection handle here so
        # we don't look into an incomplete transaction
        kwargs = {
            'profile': None,
            'profiles_dir': self.test_root_dir,
            'target': None,
        }

        config = RuntimeConfig.from_args(TestArgs(kwargs))

        register_adapter(config)
        adapter = get_adapter(config)
        adapter.cleanup_connections()
        self.adapter_type = adapter.type()
        self.adapter = adapter
        self.config = config

        self._drop_schemas()
        self._create_schemas()

    def quote_as_configured(self, value, quote_key):
        return self.adapter.quote_as_configured(value, quote_key)

    def tearDown(self):
        # get any current run adapter and clean up its connections before we
        # reset them. It'll probably be different from ours because
        # handle_and_check() calls reset_adapters().
        register_adapter(self.config)
        adapter = get_adapter(self.config)
        if adapter is not self.adapter:
            adapter.cleanup_connections()
        if not hasattr(self, 'adapter'):
            self.adapter = adapter

        self._drop_schemas()

        self.adapter.cleanup_connections()
        reset_adapters()
        os.chdir(INITIAL_ROOT)
        try:
            shutil.rmtree(self.test_root_dir)
        except EnvironmentError:
            logger.exception('Could not clean up after test - {} not removable'
                             .format(self.test_root_dir))

    def _get_schema_fqn(self, database, schema):
        schema_fqn = self.quote_as_configured(schema, 'schema')
        return schema_fqn

    def _create_schema_named(self, database, schema):
        self.run_sql('CREATE SCHEMA {schema}')

    def _drop_schema_named(self, database, schema):
        self.run_sql('DROP SCHEMA IF EXISTS {schema} CASCADE')

    def _create_schemas(self):
        schema = self.unique_schema()
        with self.adapter.connection_named('__test'):
            self._create_schema_named(self.default_database, schema)

    def _drop_schemas(self):
        with self.adapter.connection_named('__test'):
            schema = self.unique_schema()
            self._drop_schema_named(self.default_database, schema)
            if self.setup_alternate_db and self.alternative_database:
                self._drop_schema_named(self.alternative_database, schema)

    @property
    def project_config(self):
        return {
            'config-version': 2,
        }

    @property
    def profile_config(self):
        return {}

    def run_dbt(self, args=None, expect_pass=True, profiles_dir=True):
        res, success = self.run_dbt_and_check(args=args, profiles_dir=profiles_dir)
        self.assertEqual(
            success, expect_pass,
            "dbt exit state did not match expected")

        return res


    def run_dbt_and_capture(self, *args, **kwargs):
        try:
            stringbuf = capture_stdout_logs()
            res = self.run_dbt(*args, **kwargs)
            stdout = stringbuf.getvalue()

        finally:
            stop_capture_stdout_logs()

        return res, stdout

    def run_dbt_and_check(self, args=None, profiles_dir=True):
        log_manager.reset_handlers()
        if args is None:
            args = ["run"]

        final_args = []

        if os.getenv('DBT_TEST_SINGLE_THREADED') in ('y', 'Y', '1'):
            final_args.append('--single-threaded')

        final_args.extend(args)

        if profiles_dir:
            final_args.extend(['--profiles-dir', self.test_root_dir])
        final_args.append('--log-cache-events')

        logger.info("Invoking dbt with {}".format(final_args))
        return dbt.handle_and_check(final_args)

    def run_sql_file(self, path, kwargs=None):
        with open(path, 'r') as f:
            statements = f.read().split(";")
            for statement in statements:
                self.run_sql(statement, kwargs=kwargs)

    def transform_sql(self, query, kwargs=None):
        to_return = query

        base_kwargs = {
            'schema': self.unique_schema(),
            'database': self.adapter.quote(self.default_database),
        }
        if kwargs is None:
            kwargs = {}
        base_kwargs.update(kwargs)

        to_return = to_return.format(**base_kwargs)

        return to_return

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

    def _ilike(self, target, value):
        return "{} ilike '{}'".format(target, value)

    def get_many_table_columns_bigquery(self, tables, schema, database=None):
        result = []
        for table in tables:
            relation = self._make_relation(table, schema, database)
            columns = self.adapter.get_columns_in_relation(relation)
            for col in columns:
                result.append((table, col.column, col.dtype, col.char_size))
        return result

    def get_many_table_columns(self, tables, schema, database=None):
        result = self.get_many_table_columns_bigquery(tables, schema, database)
        result.sort(key=lambda x: '{}.{}'.format(x[0], x[1]))
        return result

    def filter_many_columns(self, column):
        if len(column) == 3:
            table_name, column_name, data_type = column
            char_size = None
        else:
            table_name, column_name, data_type, char_size = column
        return (table_name, column_name, data_type, char_size)

    @contextmanager
    def get_connection(self, name=None):
        """Create a test connection context where all executed macros, etc will
        get self.adapter as the adapter.

        This allows tests to run normal adapter macros as if reset_adapters()
        were not called by handle_and_check (for asserts, etc)
        """
        if name is None:
            name = '__test'
        with patch.object(providers, 'get_adapter', return_value=self.adapter):
            with self.adapter.connection_named(name):
                conn = self.adapter.connections.get_thread_connection()
                yield conn

    def get_relation_columns(self, relation):
        with self.get_connection():
            columns = self.adapter.get_columns_in_relation(relation)

        return sorted(((c.name, c.dtype, c.char_size) for c in columns),
                      key=lambda x: x[0])

    def get_table_columns(self, table, schema=None, database=None):
        schema = self.unique_schema() if schema is None else schema
        database = self.default_database if database is None else database
        relation = self.adapter.Relation.create(
            database=database,
            schema=schema,
            identifier=table,
            type='table',
            quote_policy=self.config.quoting
        )
        return self.get_relation_columns(relation)

    def get_table_columns_as_dict(self, tables, schema=None):
        col_matrix = self.get_many_table_columns(tables, schema)
        res = {}
        for row in col_matrix:
            table_name = row[0]
            col_def = row[1:]
            if table_name not in res:
                res[table_name] = []
            res[table_name].append(col_def)
        return res

    def get_models_in_schema(self, schema=None):
        schema = self.unique_schema() if schema is None else schema
        sql = """
                select table_name,
                        case when table_type = 'BASE TABLE' then 'table'
                             when table_type = 'VIEW' then 'view'
                             else table_type
                        end as materialization
                from information_schema.tables
                where {}
                order by table_name
                """

        sql = sql.format(self._ilike('table_schema', schema))
        result = self.run_sql(sql, fetch='all')

        return {model_name: materialization for (model_name, materialization) in result}

    def _assertTablesEqualSql(self, relation_a, relation_b, columns=None):
        if columns is None:
            columns = self.get_relation_columns(relation_a)
        column_names = [c[0] for c in columns]

        sql = self.adapter.get_rows_different_sql(
            relation_a, relation_b, column_names
        )

        return sql

    def assertTablesEqual(self, table_a, table_b,
                          table_a_schema=None, table_b_schema=None,
                          table_a_db=None, table_b_db=None):
        if table_a_schema is None:
            table_a_schema = self.unique_schema()

        if table_b_schema is None:
            table_b_schema = self.unique_schema()

        if table_a_db is None:
            table_a_db = self.default_database

        if table_b_db is None:
            table_b_db = self.default_database

        relation_a = self._make_relation(table_a, table_a_schema, table_a_db)
        relation_b = self._make_relation(table_b, table_b_schema, table_b_db)

        self._assertTableColumnsEqual(relation_a, relation_b)

        sql = self._assertTablesEqualSql(relation_a, relation_b)
        result = self.run_sql(sql, fetch='one')

        self.assertEqual(
            result[0],
            0,
            'row_count_difference nonzero: ' + sql
        )
        self.assertEqual(
            result[1],
            0,
            'num_mismatched nonzero: ' + sql
        )

    def _make_relation(self, identifier, schema=None, database=None):
        if schema is None:
            schema = self.unique_schema()
        if database is None:
            database = self.default_database
        return self.adapter.Relation.create(
            database=database,
            schema=schema,
            identifier=identifier,
            quote_policy=self.config.quoting
        )

    def get_many_relation_columns(self, relations):
        """Returns a dict of (datbase, schema) -> (dict of (table_name -> list of columns))
        """
        schema_fqns = {}
        for rel in relations:
            this_schema = schema_fqns.setdefault((rel.database, rel.schema), [])
            this_schema.append(rel.identifier)

        column_specs = {}
        for key, tables in schema_fqns.items():
            database, schema = key
            columns = self.get_many_table_columns(tables, schema, database=database)
            table_columns = {}
            for col in columns:
                table_columns.setdefault(col[0], []).append(col[1:])
            for rel_name, columns in table_columns.items():
                key = (database, schema, rel_name)
                column_specs[key] = columns

        return column_specs

    def assertManyRelationsEqual(self, relations, default_schema=None, default_database=None):
        if default_schema is None:
            default_schema = self.unique_schema()
        if default_database is None:
            default_database = self.default_database

        specs = []
        for relation in relations:
            if not isinstance(relation, (tuple, list)):
                relation = [relation]

            assert len(relation) <= 3

            if len(relation) == 3:
                relation = self._make_relation(*relation)
            elif len(relation) == 2:
                relation = self._make_relation(relation[0], relation[1], default_database)
            elif len(relation) == 1:
                relation = self._make_relation(relation[0], default_schema, default_database)
            else:
                raise ValueError('relation must be a sequence of 1, 2, or 3 values')

            specs.append(relation)

        with self.get_connection():
            column_specs = self.get_many_relation_columns(specs)

        # make sure everyone has equal column definitions
        first_columns = None
        for relation in specs:
            key = (relation.database, relation.schema, relation.identifier)
            # get a good error here instead of a hard-to-diagnose KeyError
            self.assertIn(key, column_specs, f'No columns found for {key}')
            columns = column_specs[key]
            if first_columns is None:
                first_columns = columns
            else:
                self.assertEqual(
                    first_columns, columns,
                    '{} did not match {}'.format(str(specs[0]), str(relation))
                )

        # make sure everyone has the same data. if we got here, everyone had
        # the same column specs!
        first_relation = None
        for relation in specs:
            if first_relation is None:
                first_relation = relation
            else:
                sql = self._assertTablesEqualSql(first_relation, relation,
                                                 columns=first_columns)
                result = self.run_sql(sql, fetch='one')

                self.assertEqual(
                    result[0],
                    0,
                    'row_count_difference nonzero: ' + sql
                )
                self.assertEqual(
                    result[1],
                    0,
                    'num_mismatched nonzero: ' + sql
                )

    def assertManyTablesEqual(self, *args):
        schema = self.unique_schema()

        all_tables = []
        for table_equivalencies in args:
            all_tables += list(table_equivalencies)

        all_cols = self.get_table_columns_as_dict(all_tables, schema)

        for table_equivalencies in args:
            first_table = table_equivalencies[0]
            first_relation = self._make_relation(first_table)

            # assert that all tables have the same columns
            base_result = all_cols[first_table]
            self.assertTrue(len(base_result) > 0)

            for other_table in table_equivalencies[1:]:
                other_result = all_cols[other_table]
                self.assertTrue(len(other_result) > 0)
                self.assertEqual(base_result, other_result)

                other_relation = self._make_relation(other_table)
                sql = self._assertTablesEqualSql(first_relation,
                                                 other_relation,
                                                 columns=base_result)
                result = self.run_sql(sql, fetch='one')

                self.assertEqual(
                    result[0],
                    0,
                    'row_count_difference nonzero: ' + sql
                )
                self.assertEqual(
                    result[1],
                    0,
                    'num_mismatched nonzero: ' + sql
                )


    def _assertTableRowCountsEqual(self, relation_a, relation_b):
        cmp_query = """
            with table_a as (

                select count(*) as num_rows from {}

            ), table_b as (

                select count(*) as num_rows from {}

            )

            select table_a.num_rows - table_b.num_rows as difference
            from table_a, table_b

        """.format(str(relation_a), str(relation_b))

        res = self.run_sql(cmp_query, fetch='one')

        self.assertEqual(int(res[0]), 0, "Row count of table {} doesn't match row count of table {}. ({} rows different)".format(
                relation_a.identifier,
                relation_b.identifier,
                res[0]
            )
        )

    def assertTableDoesNotExist(self, table, schema=None, database=None):
        columns = self.get_table_columns(table, schema, database)

        self.assertEqual(
            len(columns),
            0
        )

    def assertTableDoesExist(self, table, schema=None, database=None):
        columns = self.get_table_columns(table, schema, database)

        self.assertGreater(
            len(columns),
            0
        )

    def _assertTableColumnsEqual(self, relation_a, relation_b):
        table_a_result = self.get_relation_columns(relation_a)
        table_b_result = self.get_relation_columns(relation_b)

        text_types = {'text', 'character varying', 'character', 'varchar'}

        self.assertEqual(len(table_a_result), len(table_b_result))
        for a_column, b_column in zip(table_a_result, table_b_result):
            a_name, a_type, a_size = a_column
            b_name, b_type, b_size = b_column
            self.assertEqual(a_name, b_name,
                '{} vs {}: column "{}" != "{}"'.format(
                    relation_a, relation_b, a_name, b_name
                ))

            self.assertEqual(a_type, b_type,
                '{} vs {}: column "{}" has type "{}" != "{}"'.format(
                    relation_a, relation_b, a_name, a_type, b_type
                ))

            self.assertEqual(a_size, b_size,
                '{} vs {}: column "{}" has size "{}" != "{}"'.format(
                    relation_a, relation_b, a_name, a_size, b_size
                ))

    def assertEquals(self, *args, **kwargs):
        # assertEquals is deprecated. This makes the warnings less chatty
        self.assertEqual(*args, **kwargs)

    def assertBetween(self, timestr, start, end=None):
        datefmt = '%Y-%m-%dT%H:%M:%S.%fZ'
        if end is None:
            end = datetime.utcnow()

        parsed = datetime.strptime(timestr, datefmt)

        self.assertLessEqual(start, parsed,
            'parsed date {} happened before {}'.format(
                parsed,
                start.strftime(datefmt))
        )
        self.assertGreaterEqual(end, parsed,
            'parsed date {} happened after {}'.format(
                parsed,
                end.strftime(datefmt))
        )


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
        assert _profile_from_test_name(wrapped.__name__) == profile_name
        return func
    return outer


class AnyFloat:
    """Any float. Use this in assertEqual() calls to assert that it is a float.
    """
    def __eq__(self, other):
        return isinstance(other, float)


class AnyString:
    """Any string. Use this in assertEqual() calls to assert that it is a string.
    """
    def __eq__(self, other):
        return isinstance(other, str)


class AnyStringWith:
    def __init__(self, contains=None):
        self.contains = contains

    def __eq__(self, other):
        if not isinstance(other, str):
            return False

        if self.contains is None:
            return True

        return self.contains in other

    def __repr__(self):
        return 'AnyStringWith<{!r}>'.format(self.contains)


def get_manifest():
    path = './target/partial_parse.msgpack'
    if os.path.exists(path):
        with open(path, 'rb') as fp:
            manifest_mp = fp.read()
        manifest: Manifest = Manifest.from_msgpack(manifest_mp)
        return manifest
    else:
        return None
