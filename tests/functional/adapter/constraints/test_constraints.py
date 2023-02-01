import pytest
import re
import json
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture
)
from dbt.tests.adapter.constraints.fixtures import (
    my_model_sql,
    model_schema_yml,
)
from dbt.tests.adapter.constraints.test_constraints import (
  BaseConstraintsColumnsEqual,
  BaseConstraintsRuntimeEnforcement
)

model_schema_yml = model_schema_yml.replace(
  "      constraints_enabled: true",
  "      constraints_enabled: true\n      file_format: delta"
)

_expected_sql_spark = """
create table {0}.my_model as
  select
    1 as id,
    'blue' as color,
    cast('2019-01-01' as date) as date_day
"""

@pytest.mark.skip_profile('spark_session', 'apache_spark')
class TestSparkConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    pass

@pytest.mark.skip_profile('spark_session', 'apache_spark')
class TestSparkConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        return _expected_sql_spark.format(project.test_schema)

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['violate the new NOT NULL constraint']