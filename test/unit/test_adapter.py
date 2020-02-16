import unittest

import dbt.flags as flags
import mock
from agate import Column, MappedSequence
from dbt.adapters.base import BaseRelation
from pyhive import hive

from dbt.adapters.spark import SparkAdapter
from .utils import config_from_parts_or_dicts


class TestSparkAdapter(unittest.TestCase):

    def setUp(self):
        flags.STRICT_MODE = True

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

    def get_target_http(self, project):
        return config_from_parts_or_dicts(project, {
            'outputs': {
                'test': {
                    'type': 'spark',
                    'method': 'http',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        })

    def get_target_thrift(self, project):
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
        config = self.get_target_http(self.project_cfg)
        adapter = SparkAdapter(config)

        def hive_http_connect(thrift_transport):
            self.assertEqual(thrift_transport.scheme, 'https')
            self.assertEqual(thrift_transport.port, 443)
            self.assertEqual(thrift_transport.host, 'myorg.sparkhost.com')
            self.assertEqual(thrift_transport.path, '/sql/protocolv1/o/0/01234-23423-coffeetime')

        with mock.patch.object(hive, 'connect', new=hive_http_connect):
            connection = adapter.acquire_connection('dummy')

            self.assertEqual(connection.state, 'open')
            self.assertNotEqual(connection.handle, None)

    def test_thrift_connection(self):
        config = self.get_target_thrift(self.project_cfg)
        adapter = SparkAdapter(config)

        def hive_thrift_connect(host, port, username):
            self.assertEqual(host, 'myorg.sparkhost.com')
            self.assertEqual(port, 10001)
            self.assertEqual(username, 'dbt')

        with mock.patch.object(hive, 'connect', new=hive_thrift_connect):
            connection = adapter.acquire_connection('dummy')

            self.assertEqual(connection.state, 'open')
            self.assertNotEqual(connection.handle, None)

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = 'table'

        relation = BaseRelation.create(
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
            (None, None),
            ('# Detailed Table Information', None),
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

        rows = SparkAdapter.parse_describe_extended(relation, input_cols)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], {
            'table_database': relation.database,
            'table_schema': relation.schema,
            'table_name': relation.name,
            'table_type': rel_type,
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
            'table_type': rel_type,
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
            'table_type': rel_type,
            'table_comment': None,
            'table_owner': 'root',
            'column_name': 'dt',
            'column_index': 2,
            'column_type': 'date',
            'column_comment': None
        })
