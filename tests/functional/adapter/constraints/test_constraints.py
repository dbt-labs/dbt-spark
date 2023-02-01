import pytest
import re
import json
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture
)
from dbt.tests.adapter.constraints.test_constraints import (
  BaseConstraintsColumnsEqual,
  BaseConstraintsRuntimeEnforcement
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
    def expected_sql(self, project):
        return _expected_sql_spark.format(project.test_schema)

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ['violate the new NOT NULL constraint']