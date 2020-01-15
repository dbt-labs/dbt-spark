import unittest
from unittest import mock

import dbt.flags as flags
from pyhive import hive
from agate import Column, MappedSequence
from dbt.adapters.spark import SparkAdapter, SparkRelation
from .utils import config_from_parts_or_dicts


# from spark import connector as spark_connector


class TestSparkAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = False

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': False,
            }
        }

    def _get_target_http(self, project):
        return config_from_parts_or_dicts(project, {
            'outputs': {
                'test': {
                    'type': 'spark',
                    'method': 'http',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'organization': '0123456789',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        })

    def _get_target_thrift(self, project):
        return config_from_parts_or_dicts(project, {
            'outputs': {
                'test': {
                    'type': 'spark',
                    'method': 'thrift',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 10001,
                    'user': 'dbt'
                }
            },
            'target': 'test'
        })

    def test_http_connection(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config)

        def hive_http_connect(thrift_transport):
            self.assertEqual(thrift_transport.scheme, 'https')
            self.assertEqual(thrift_transport.port, 443)
            self.assertEqual(thrift_transport.host, 'myorg.sparkhost.com')
            self.assertEqual(thrift_transport.path, '/sql/protocolv1/o/0123456789/01234-23423-coffeetime')

        with mock.patch.object(hive, 'connect', new=hive_http_connect):
            connection = adapter.acquire_connection('dummy')

            self.assertEqual(connection.state, 'open')
            self.assertNotEqual(connection.handle, None)
            self.assertEqual(connection.credentials.cluster, '01234-23423-coffeetime')
            self.assertEqual(connection.credentials.token, 'abc123')
            self.assertEqual(connection.credentials.schema, 'analytics')
            self.assertEqual(connection.credentials.database, 'analytics')

    def test_thrift_connection(self):
        config = self._get_target_thrift(self.project_cfg)
        adapter = SparkAdapter(config)

        def hive_thrift_connect(host, port, username):
            self.assertEqual(host, 'myorg.sparkhost.com')
            self.assertEqual(port, 10001)
            self.assertEqual(username, 'dbt')

        with mock.patch.object(hive, 'connect', new=hive_thrift_connect):
            connection = adapter.acquire_connection('dummy')

            self.assertEqual(connection.state, 'open')
            self.assertNotEqual(connection.handle, None)
            self.assertEqual(connection.credentials.schema, 'analytics')
            self.assertEqual(connection.credentials.database, 'analytics')

    def test_parse_relation(self):
        rel_type = SparkRelation.RelationType.Table

        relation = SparkRelation.create(
            database='default_database',
            schema='default_schema',
            identifier='mytable',
            type=rel_type
        )

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ('col1', 'decimal(22,0)'),
            ('col2', 'string',),
            ('# Partition Information', 'data_type'),
            ('# col_name', 'data_type'),
            ('dt', 'date'),
            ('', ''),
            ('# Detailed Table Information', ''),
            ('Database', relation.database),
            ('Owner', 'root'),
            ('Created Time', 'Wed Feb 04 18:15:00 UTC 1815'),
            ('Last Access', 'Wed May 20 19:25:00 UTC 1925'),
            ('Type', 'MANAGED'),
            ('Provider', 'delta'),
            ('Location', '/mnt/vo'),
            ('Serde Library', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'),
            ('InputFormat', 'org.apache.hadoop.mapred.SequenceFileInputFormat'),
            ('OutputFormat', 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'),
            ('Partition Provider', 'Catalog')
        ]

        input_cols = [Column(index=None, name=r[0], data_type=r[1], rows=MappedSequence(
            keys=['col_name', 'data_type'],
            values=r
        )) for r in plain_rows]

        rows = SparkAdapter._parse_relation(relation, input_cols, rel_type)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], {
            'table_database': relation.database,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': relation.type,
            'stats:bytes:description': 'The size of the table in bytes',
            'stats:bytes:include': False,
            'stats:bytes:label': 'Table size',
            'stats:bytes:value': None,
            'stats:rows:description': 'The number of rows in the table',
            'stats:rows:include': False,
            'stats:rows:label': 'Number of rows',
            'stats:rows:value': None,
            'table_comment': None,
            'table_owner': 'root',
            'column_name': 'col1',
            'column_index': 0,
            'column_type': 'decimal(22,0)',
            'column_comment': None
        })

        self.assertEqual(rows[1], {
            'table_database': relation.database,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': relation.type,
            'stats:bytes:description': 'The size of the table in bytes',
            'stats:bytes:include': False,
            'stats:bytes:label': 'Table size',
            'stats:bytes:value': None,
            'stats:rows:description': 'The number of rows in the table',
            'stats:rows:include': False,
            'stats:rows:label': 'Number of rows',
            'stats:rows:value': None,
            'table_comment': None,
            'table_owner': 'root',
            'column_name': 'col2',
            'column_index': 1,
            'column_type': 'string',
            'column_comment': None
        })

        self.assertEqual(rows[2], {
            'table_database': relation.database,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': relation.type,
            'stats:bytes:description': 'The size of the table in bytes',
            'stats:bytes:include': False,
            'stats:bytes:label': 'Table size',
            'stats:bytes:value': None,
            'stats:rows:description': 'The number of rows in the table',
            'stats:rows:include': False,
            'stats:rows:label': 'Number of rows',
            'stats:rows:value': None,
            'table_comment': None,
            'table_owner': 'root',
            'column_name': 'dt',
            'column_index': 4,
            'column_type': 'date',
            'column_comment': None
        })

    def test_parse_relation_with_properties(self):
        rel_type = SparkRelation.RelationType.Table

        relation = SparkRelation.create(
            database='default_database',
            schema='default_schema',
            identifier='mytable',
            type=rel_type
        )

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ('col1', 'decimal(19,25)'),
            ('', ''),
            ('# Detailed Table Information', ''),
            ('Database', relation.database),
            ('Owner', 'root'),
            ('Created Time', 'Wed Feb 04 18:15:00 UTC 1815'),
            ('Last Access', 'Wed May 20 19:25:00 UTC 1925'),
            ('Type', 'MANAGED'),
            ('Provider', 'delta'),
            ('Location', '/mnt/vo'),
            ('Serde Library', 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'),
            ('InputFormat', 'org.apache.hadoop.mapred.SequenceFileInputFormat'),
            ('OutputFormat', 'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'),
            ('Partition Provider', 'Catalog')
        ]

        input_cols = [Column(index=None, name=r[0], data_type=r[1], rows=MappedSequence(
            keys=['col_name', 'data_type'],
            values=r
        )) for r in plain_rows]

        rows = SparkAdapter._parse_relation(relation, input_cols, rel_type, {'Owner': 'Fokko'})
        self.assertEqual(rows[0], {
            'table_database': relation.database,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
            'stats:bytes:description': 'The size of the table in bytes',
            'stats:bytes:include': False,
            'stats:bytes:label': 'Table size',
            'stats:bytes:value': None,
            'stats:rows:description': 'The number of rows in the table',
            'stats:rows:include': False,
            'stats:rows:label': 'Number of rows',
            'stats:rows:value': None,
            'table_comment': None,
            'table_owner': 'Fokko',
            'column_name': 'col1',
            'column_index': 0,
            'column_type': 'decimal(19,25)',
            'column_comment': None
        })
