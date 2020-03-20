import unittest
from unittest import mock
import re
from jinja2 import Environment, FileSystemLoader


class TestSparkMacros(unittest.TestCase):

    def setUp(self):
        self.jinja_env = Environment(loader=FileSystemLoader('dbt/include/spark/macros'),
                                     extensions=['jinja2.ext.do',])

        self.config = {}

        self.default_context = {}
        self.default_context['validation'] = mock.Mock()
        self.default_context['model'] = mock.Mock()
        self.default_context['exceptions'] = mock.Mock()
        self.default_context['config'] = mock.Mock()
        self.default_context['config'].get = lambda key, default=None, **kwargs: self.config.get(key, default)


    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)


    def __run_macro(self, template, name, temporary, relation, sql):
        self.default_context['model'].alias = relation
        value = getattr(template.module, name)(temporary, relation, sql)
        return re.sub(r'\s\s+', ' ', value)


    def test_macros_load(self):
        self.jinja_env.get_template('adapters.sql')


    def test_macros_create_table_as(self):
        template = self.__get_template('adapters.sql')

        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table as select 1")


    def test_macros_create_table_as_file_format(self):
        template = self.__get_template('adapters.sql')


        self.config['file_format'] = 'delta'
        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table using delta as select 1")


    def test_macros_create_table_as_partition(self):
        template = self.__get_template('adapters.sql')


        self.config['partition_by'] = 'partition_1'
        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table partitioned by (partition_1) as select 1")


    def test_macros_create_table_as_partitions(self):
        template = self.__get_template('adapters.sql')


        self.config['partition_by'] = ['partition_1', 'partition_2']
        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table partitioned by (partition_1,partition_2) as select 1")


    def test_macros_create_table_as_cluster(self):
        template = self.__get_template('adapters.sql')


        self.config['clustered_by'] = 'cluster_1'
        self.config['buckets'] = '1'
        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table clustered by (cluster_1) into 1 buckets as select 1")


    def test_macros_create_table_as_clusters(self):
        template = self.__get_template('adapters.sql')


        self.config['clustered_by'] = ['cluster_1', 'cluster_2']
        self.config['buckets'] = '1'
        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table clustered by (cluster_1,cluster_2) into 1 buckets as select 1")


    def test_macros_create_table_as_location(self):
        template = self.__get_template('adapters.sql')


        self.config['location_root'] = '/mnt/root'
        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table location '/mnt/root/my_table' as select 1")


    def test_macros_create_table_as_comment(self):
        template = self.__get_template('adapters.sql')


        self.config['persist_docs'] = {'relation': True}
        self.default_context['model'].description = 'Description Test'
        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table comment 'Description Test' as select 1")


    def test_macros_create_table_as_all(self):
        template = self.__get_template('adapters.sql')

        self.config['file_format'] = 'delta'
        self.config['location_root'] = '/mnt/root'
        self.config['partition_by'] = ['partition_1', 'partition_2']
        self.config['clustered_by'] = ['cluster_1', 'cluster_2']
        self.config['buckets'] = '1'
        self.config['persist_docs'] = {'relation': True}
        self.default_context['model'].description = 'Description Test'

        self.assertEqual(self.__run_macro(template, 'spark__create_table_as', False, 'my_table', 'select 1'),
                         "create table my_table using delta partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1")
