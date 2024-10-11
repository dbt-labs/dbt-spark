import unittest
import pytest
from multiprocessing import get_context
from unittest import mock

from dbt.exceptions import DbtRuntimeError
from agate import Row
from pyhive import hive
from dbt.adapters.spark import SparkAdapter, SparkRelation
from .utils import config_from_parts_or_dicts


class TestSparkAdapter(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def set_up_fixtures(
        self,
        target_http,
        target_odbc_with_extra_conn,
        target_thrift,
        target_thrift_kerberos,
        target_odbc_sql_endpoint,
        target_odbc_cluster,
        target_use_ssl_thrift,
        base_project_cfg,
    ):
        self.base_project_cfg = base_project_cfg
        self.target_http = target_http
        self.target_odbc_with_extra_conn = target_odbc_with_extra_conn
        self.target_odbc_sql_endpoint = target_odbc_sql_endpoint
        self.target_odbc_cluster = target_odbc_cluster
        self.target_thrift = target_thrift
        self.target_thrift_kerberos = target_thrift_kerberos
        self.target_use_ssl_thrift = target_use_ssl_thrift

    def test_http_connection(self):
        adapter = SparkAdapter(self.target_http, get_context("spawn"))

        def hive_http_connect(thrift_transport, configuration):
            self.assertEqual(thrift_transport.scheme, "https")
            self.assertEqual(thrift_transport.port, 443)
            self.assertEqual(thrift_transport.host, "myorg.sparkhost.com")
            self.assertEqual(
                thrift_transport.path, "/sql/protocolv1/o/0123456789/01234-23423-coffeetime"
            )
            self.assertEqual(configuration["spark.driver.memory"], "4g")

        # with mock.patch.object(hive, 'connect', new=hive_http_connect):
        with mock.patch("dbt.adapters.spark.connections.hive.connect", new=hive_http_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.cluster, "01234-23423-coffeetime")
            self.assertEqual(connection.credentials.token, "abc123")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_thrift_connection(self):
        config = self.target_thrift
        adapter = SparkAdapter(config, get_context("spawn"))

        def hive_thrift_connect(
            host, port, username, auth, kerberos_service_name, password, configuration
        ):
            self.assertEqual(host, "myorg.sparkhost.com")
            self.assertEqual(port, 10001)
            self.assertEqual(username, "dbt")
            self.assertIsNone(auth)
            self.assertIsNone(kerberos_service_name)
            self.assertIsNone(password)
            self.assertDictEqual(configuration, {})

        with mock.patch.object(hive, "connect", new=hive_thrift_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_thrift_ssl_connection(self):
        adapter = SparkAdapter(self.target_use_ssl_thrift, get_context("spawn"))

        def hive_thrift_connect(thrift_transport, configuration):
            self.assertIsNotNone(thrift_transport)
            transport = thrift_transport._trans
            self.assertEqual(transport.host, "myorg.sparkhost.com")
            self.assertEqual(transport.port, 10001)
            self.assertDictEqual(configuration, {})

        with mock.patch.object(hive, "connect", new=hive_thrift_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_thrift_connection_kerberos(self):
        adapter = SparkAdapter(self.target_thrift_kerberos, get_context("spawn"))

        def hive_thrift_connect(
            host, port, username, auth, kerberos_service_name, password, configuration
        ):
            self.assertEqual(host, "myorg.sparkhost.com")
            self.assertEqual(port, 10001)
            self.assertEqual(username, "dbt")
            self.assertEqual(auth, "KERBEROS")
            self.assertEqual(kerberos_service_name, "hive")
            self.assertIsNone(password)
            self.assertDictEqual(configuration, {})

        with mock.patch.object(hive, "connect", new=hive_thrift_connect):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_odbc_cluster_connection(self):
        adapter = SparkAdapter(self.target_odbc_cluster, get_context("spawn"))

        def pyodbc_connect(connection_str, autocommit):
            self.assertTrue(autocommit)
            self.assertIn("driver=simba;", connection_str.lower())
            self.assertIn("port=443;", connection_str.lower())
            self.assertIn("host=myorg.sparkhost.com;", connection_str.lower())
            self.assertIn(
                "httppath=/sql/protocolv1/o/0123456789/01234-23423-coffeetime;",
                connection_str.lower(),
            )  # noqa

        with mock.patch(
            "dbt.adapters.spark.connections.pyodbc.connect", new=pyodbc_connect
        ):  # noqa
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.cluster, "01234-23423-coffeetime")
            self.assertEqual(connection.credentials.token, "abc123")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_odbc_endpoint_connection(self):
        adapter = SparkAdapter(self.target_odbc_sql_endpoint, get_context("spawn"))

        def pyodbc_connect(connection_str, autocommit):
            self.assertTrue(autocommit)
            self.assertIn("driver=simba;", connection_str.lower())
            self.assertIn("port=443;", connection_str.lower())
            self.assertIn("host=myorg.sparkhost.com;", connection_str.lower())
            self.assertIn(
                "httppath=/sql/1.0/endpoints/012342342393920a;", connection_str.lower()
            )  # noqa

        with mock.patch(
            "dbt.adapters.spark.connections.pyodbc.connect", new=pyodbc_connect
        ):  # noqa
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.endpoint, "012342342393920a")
            self.assertEqual(connection.credentials.token, "abc123")
            self.assertEqual(connection.credentials.schema, "analytics")
            self.assertIsNone(connection.credentials.database)

    def test_odbc_with_extra_connection_string(self):
        adapter = SparkAdapter(self.target_odbc_with_extra_conn, get_context("spawn"))

        def pyodbc_connect(connection_str, autocommit):
            self.assertTrue(autocommit)
            self.assertIn("driver=simba;", connection_str.lower())
            self.assertIn("port=443;", connection_str.lower())
            self.assertIn("host=myorg.sparkhost.com;", connection_str.lower())
            self.assertIn("someExtraValues", connection_str)

        with mock.patch(
            "dbt.adapters.spark.connections.pyodbc.connect", new=pyodbc_connect
        ):  # noqa
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertIsNone(connection.credentials.database)

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            (
                "col2",
                "string",
            ),
            ("dt", "date"),
            ("struct_col", "struct<struct_inner_col:string>"),
            ("# Partition Information", "data_type"),
            ("# col_name", "data_type"),
            ("dt", "date"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        rows = SparkAdapter(self.target_http, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct<struct_inner_col:string>",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_relation_with_integer_owner(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Detailed Table Information", None),
            ("Owner", 1234),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        rows = SparkAdapter(self.target_http, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )

        self.assertEqual(rows[0].to_column_dict().get("table_owner"), "1234")

    def test_parse_relation_with_statistics(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Partition Information", "data_type"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Statistics", "1109049927 bytes, 14093476 rows"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        rows = SparkAdapter(self.target_http, get_context("spawn")).parse_describe_extended(
            relation, input_cols
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1109049927,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 14093476,
            },
        )

    def test_relation_with_database(self):
        adapter = SparkAdapter(self.target_http, get_context("spawn"))
        # fine
        adapter.Relation.create(schema="different", identifier="table")
        with self.assertRaises(DbtRuntimeError):
            # not fine - database set
            adapter.Relation.create(database="something", schema="different", identifier="table")

    def test_profile_with_database(self):
        profile = {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "http",
                    # not allowed
                    "database": "analytics2",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 443,
                    "token": "abc123",
                    "organization": "0123456789",
                    "cluster": "01234-23423-coffeetime",
                }
            },
            "target": "test",
        }
        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.base_project_cfg, profile)

    def test_profile_with_cluster_and_sql_endpoint(self):
        profile = {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "odbc",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 443,
                    "token": "abc123",
                    "organization": "0123456789",
                    "cluster": "01234-23423-coffeetime",
                    "endpoint": "0123412341234e",
                }
            },
            "target": "test",
        }
        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.base_project_cfg, profile)

    def test_parse_columns_from_information_with_table_type_and_delta_provider(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        # Mimics the output of Spark in the information column
        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: delta\n"
            "Statistics: 123456789 bytes\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Partition Provider: Catalog\n"
            "Partition Columns: [`dt`]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        columns = SparkAdapter(
            self.target_http, get_context("spawn")
        ).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

    def test_parse_columns_from_information_with_view_type(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.View
        information = (
            "Database: default_schema\n"
            "Table: myview\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: UNKNOWN\n"
            "Created By: Spark 3.0.1\n"
            "Type: VIEW\n"
            "View Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Original Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Catalog and Namespace: spark_catalog.default\n"
            "View Query Output Columns: [col1, col2, dt]\n"
            "Table Properties: [view.query.out.col.1=col1, view.query.out.col.2=col2, "
            "transient_lastDdlTime=1618324324, view.query.out.col.3=dt, "
            "view.catalogAndNamespace.part.0=spark_catalog, "
            "view.catalogAndNamespace.part.1=default]\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Storage Properties: [serialization.format=1]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = SparkRelation.create(
            schema="default_schema", identifier="myview", type=rel_type, information=information
        )

        columns = SparkAdapter(
            self.target_http, get_context("spawn")
        ).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_columns_from_information_with_table_type_and_parquet_provider(self):
        self.maxDiff = None
        rel_type = SparkRelation.get_relation_type.Table

        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: parquet\n"
            "Statistics: 1234567890 bytes, 12345678 rows\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n"
            "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = SparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        columns = SparkAdapter(
            self.target_http, get_context("spawn")
        ).parse_columns_from_information(relation)
        self.assertEqual(len(columns), 4)

        self.assertEqual(
            columns[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )
