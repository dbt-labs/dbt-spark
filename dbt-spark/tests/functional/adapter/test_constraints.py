import pytest
from dbt.tests.adapter.constraints.test_constraints import (
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseConstraintQuotedColumn,
)
from dbt.tests.adapter.constraints.fixtures import (
    constrained_model_schema_yml,
    my_model_sql,
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    model_schema_yml,
    my_model_view_wrong_order_sql,
    my_model_view_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_incremental_wrong_name_sql,
    my_incremental_model_sql,
    model_fk_constraint_schema_yml,
    my_model_wrong_order_depends_on_fk_sql,
    foreign_key_model_sql,
    my_model_incremental_wrong_order_depends_on_fk_sql,
    my_model_with_quoted_column_name_sql,
    model_quoted_column_schema_yml,
)

# constraints are enforced via 'alter' statements that run after table creation
_expected_sql_spark = """
create or replace table <model_identifier>
    using delta
    as
select
  id,
  color,
  date_day
from

(
    -- depends_on: <foreign_key_model_identifier>
    select
    'blue' as color,
    1 as id,
    '2019-01-01' as date_day ) as model_subq
"""

_expected_sql_spark_model_constraints = """
create or replace table <model_identifier>
    using delta
    as
select
  id,
  color,
  date_day
from

(
    -- depends_on: <foreign_key_model_identifier>
    select
    'blue' as color,
    1 as id,
    '2019-01-01' as date_day ) as model_subq
"""

# Different on Spark:
# - does not support a data type named 'text' (TODO handle this in the base test classes using string_type
constraints_yml = model_schema_yml.replace("text", "string").replace("primary key", "")
model_fk_constraint_schema_yml = model_fk_constraint_schema_yml.replace("text", "string").replace(
    "primary key", ""
)
model_constraints_yml = constrained_model_schema_yml.replace("text", "string")


class PyodbcSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            }
        }

    @pytest.fixture
    def string_type(self):
        return "STR"

    @pytest.fixture
    def int_type(self):
        return "INT"

    @pytest.fixture
    def schema_string_type(self):
        return "STRING"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type, schema_string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ['"1"', schema_string_type, string_type],
            ["true", "boolean", "BOOL"],
            ['array("1","2","3")', "string", string_type],
            ["array(1,2,3)", "string", string_type],
            ["6.45", "decimal", "DECIMAL"],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["cast('2019-01-01' as timestamp)", "timestamp", "DATETIME"],
        ]


class DatabricksHTTPSetup:
    @pytest.fixture
    def string_type(self):
        return "STRING_TYPE"

    @pytest.fixture
    def int_type(self):
        return "INT_TYPE"

    @pytest.fixture
    def schema_string_type(self):
        return "STRING"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type, schema_string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ['"1"', schema_string_type, string_type],
            ["true", "boolean", "BOOLEAN_TYPE"],
            ['array("1","2","3")', "array<string>", "ARRAY_TYPE"],
            ["array(1,2,3)", "array<int>", "ARRAY_TYPE"],
            ["cast('2019-01-01' as date)", "date", "DATE_TYPE"],
            ["cast('2019-01-01' as timestamp)", "timestamp", "TIMESTAMP_TYPE"],
            ["cast(1.0 AS DECIMAL(4, 2))", "decimal", "DECIMAL_TYPE"],
        ]


@pytest.mark.skip_profile("spark_session", "apache_spark", "databricks_http_cluster")
class TestSparkTableConstraintsColumnsEqualPyodbc(PyodbcSetup, BaseTableConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


@pytest.mark.skip_profile("spark_session", "apache_spark", "databricks_http_cluster")
class TestSparkViewConstraintsColumnsEqualPyodbc(PyodbcSetup, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


@pytest.mark.skip_profile("spark_session", "apache_spark", "databricks_http_cluster")
class TestSparkIncrementalConstraintsColumnsEqualPyodbc(
    PyodbcSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


@pytest.mark.skip_profile(
    "spark_session",
    "apache_spark",
    "databricks_sql_endpoint",
    "databricks_cluster",
    "spark_http_odbc",
)
class TestSparkTableConstraintsColumnsEqualDatabricksHTTP(
    DatabricksHTTPSetup, BaseTableConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


@pytest.mark.skip_profile(
    "spark_session",
    "apache_spark",
    "databricks_sql_endpoint",
    "databricks_cluster",
    "spark_http_odbc",
)
class TestSparkViewConstraintsColumnsEqualDatabricksHTTP(
    DatabricksHTTPSetup, BaseViewConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


@pytest.mark.skip_profile(
    "spark_session",
    "apache_spark",
    "databricks_sql_endpoint",
    "databricks_cluster",
    "spark_http_odbc",
)
class TestSparkIncrementalConstraintsColumnsEqualDatabricksHTTP(
    DatabricksHTTPSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


class BaseSparkConstraintsDdlEnforcementSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            }
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_spark


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestSparkTableConstraintsDdlEnforcement(
    BaseSparkConstraintsDdlEnforcementSetup, BaseConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestSparkIncrementalConstraintsDdlEnforcement(
    BaseSparkConstraintsDdlEnforcementSetup, BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }


@pytest.mark.skip_profile("spark_session", "apache_spark", "databricks_http_cluster")
class TestSparkConstraintQuotedColumn(PyodbcSetup, BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": model_quoted_column_schema_yml.replace(
                "text", "string"
            ).replace('"from"', "`from`"),
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create or replace table <model_identifier>
    using delta
    as
select
  id,
  `from`,
  date_day
from

(
    select
    'blue' as `from`,
    1 as id,
    '2019-01-01' as date_day ) as model_subq
"""


class BaseSparkConstraintsRollbackSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            }
        }

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return [
            "violate the new CHECK constraint",
            "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
            "DELTA_NEW_NOT_NULL_VIOLATION",
            "violate the new NOT NULL constraint",
            "(id > 0) violated by row with values:",  # incremental mats
            "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",  # incremental mats
            "NOT NULL constraint violated for col",
        ]

    def assert_expected_error_messages(self, error_message, expected_error_messages):
        # This needs to be ANY instead of ALL
        # The CHECK constraint is added before the NOT NULL constraint
        # and different connection types display/truncate the error message in different ways...
        assert any(msg in error_message for msg in expected_error_messages)


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestSparkTableConstraintsRollback(
    BaseSparkConstraintsRollbackSetup, BaseConstraintsRollback
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": constraints_yml,
        }

    # On Spark/Databricks, constraints are applied *after* the table is replaced.
    # We don't have any way to "rollback" the table to its previous happy state.
    # So the 'color' column will be updated to 'red', instead of 'blue'.
    @pytest.fixture(scope="class")
    def expected_color(self):
        return "red"


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestSparkIncrementalConstraintsRollback(
    BaseSparkConstraintsRollbackSetup, BaseIncrementalConstraintsRollback
):
    # color stays blue for incremental models since it's a new row that just
    # doesn't get inserted
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": constraints_yml,
        }


# TODO: Like the tests above, this does test that model-level constraints don't
# result in errors, but it does not verify that they are actually present in
# Spark and that the ALTER TABLE statement actually ran.
@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestSparkModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_spark_model_constraints
