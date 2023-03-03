import pytest
from dbt.tests.util import relation_from_name
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsColumnsEqual,
    BaseConstraintsRuntimeEnforcement
)

# constraints are enforced via 'alter' statements that run after table creation
_expected_sql_spark = """
create or replace table {0}  
    using delta
    as

select
    1 as id,
    'blue' as color,
    cast('2019-01-01' as date) as date_day
""".lstrip()

@pytest.mark.skip_profile('spark_session', 'apache_spark')
class TestSparkConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture
    def int_type(self):
        return "INT_TYPE"

    @pytest.fixture
    def string_type(self):
        return "STRING"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ['1', schema_int_type, int_type],
        ]

@pytest.mark.skip_profile('spark_session', 'apache_spark')
class TestSparkConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            }
        }
    
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        relation = relation_from_name(project.adapter, "my_model")
        return _expected_sql_spark.format(relation)

    # On Spark/Databricks, constraints are applied *after* the table is replaced.
    # We don't have any way to "rollback" the table to its previous happy state.
    # So the 'color' column will be updated to 'red', instead of 'blue'.
    @pytest.fixture(scope="class")
    def expected_color(self):
        return "red"

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return [
            "violate the new CHECK constraint",
            "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
            "violate the new NOT NULL constraint",
        ]

    def assert_expected_error_messages(self, error_message, expected_error_messages):
        # This needs to be ANY instead of ALL
        # The CHECK constraint is added before the NOT NULL constraint
        # and different connection types display/truncate the error message in different ways...
        assert any(msg in error_message for msg in expected_error_messages)
