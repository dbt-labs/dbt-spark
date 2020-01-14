import unittest
from unittest import mock

import dbt.flags as flags
from pyhive import hive

from dbt.adapters.spark import SparkAdapter
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

