import mock
import unittest
import dbt.adapters
import dbt.flags as flags
from pyhive import hive
from dbt.adapters.spark import SparkAdapter
import agate

from .utils import config_from_parts_or_dicts, inject_adapter


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
