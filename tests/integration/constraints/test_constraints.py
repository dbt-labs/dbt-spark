import pytest
import json
# from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.integration.base import use_profile, DBTIntegrationTest
import logging

models__constraints_column_types_sql = """
select
    1000 as int_column,
    99.99 as float_column,
    true as bool_column,
    to_date('2022-01-01') as date_column
"""

models__constraints_incorrect_constraints_check_sql = """
select
    1000 as int_column,
    99.99 as float_column,
    true as bool_column,
    to_date('2022-01-01') as date_column
"""

models__constraints_not_null_sql = """
select
    1000 as int_column,
    99.99 as float_column,
    true as bool_column,
    to_date('2022-01-01') as date_column

union all

select
    NULL as int_column,
    99.99 as float_column,
    true as bool_column,
    to_date('2022-01-01') as date_column
"""

models__models_config_yml = """
version: 2
models:
  - name: constraints_column_types
    description: "Model to test column data type constraints"
    config:
      constraints_enabled: true
    columns:
      - name: int_column
        description: "Test for int type"
        data_type: int
        constraints: 
          - not null
        constraints_check: "int_column > 0"
      - name: float_column
        description: "Test for int type"
        data_type: float
        constraints_check: "float_column > 0"
      - name: bool_column
        description: "Test for int type"
        data_type: boolean
      - name: date_column
        description: "Test for int type"
        data_type: date

  - name: constraints_incorrect_constraints_check
    description: "Model to test failing column data type constraints"
    config:
      constraints_enabled: true
    columns:
      - name: int_column
        description: "Test for int type"
        data_type: int
        constraints: 
          - not null
        cosntraints_check:
          - "< 10"
        constraints_check: "int_column > 0"
      - name: float_column
        description: "Test for int type"
        data_type: float
        constraints_check: "float_column > 0"
      - name: bool_column
        description: "Test for int type"
        data_type: boolean
      - name: date_column
        description: "Test for int type"
        data_type: date

  - name: constraints_not_null
    description: "Model to test failing materialization when a column is NULL"
    config:
      constraints_enabled: true
    columns:
      - name: int_column
        description: "Test for int type with some constraints"
        data_type: int
        constraints: 
          - not null
      - name: float_column
        description: "Test for int type"
        data_type: float
      - name: bool_column
        description: "Test for int type"
        data_type: boolean
      - name: date_column
        description: "Test for int type"
        data_type: date
"""


class TestMaterializedWithConstraints(DBTIntegrationTest):
    @property
    def schema(self):
        return "constraints"
    
    @property
    def models(self):
        return {
            "constraints_column_types.sql": models__constraints_column_types_sql,
            "constraints_incorrect_constraints_check.sql": models__constraints_incorrect_constraints_check_sql,
            "constraints_not_null.sql": models__constraints_not_null_sql,
            "models_config.yml": models__models_config_yml,
        }

    @property
    def project_config(self, prefix):
        return {
            "config-version": 2,
            "models": {
                "materialized": "table",
                "file_format": "delta",
            },
        }

    def materialized_with_constraints(self):
        self.run_dbt(["run", "--select", "constraints_column_types"])

    def failing_materialized_with_not_null_constraint(self):
        result = self.run_dbt(
            ["run", "--select", "constraints_not_null"], expect_pass=False
        )
        assert "violate the new NOT NULL constraint" in result.results[0].message

    def failing_constraint_check(self):
        result = self.run_dbt(["run", "--select", "constraints_incorrect_constraints_check"], expect_pass=False)
        assert "violate the new CHECK constraint" in result.results[0].message

    @use_profile("databricks_cluster")
    def test__databricks_cluster__materialized_with_constraints(self, project):
        self.materialized_with_constraints()

    @use_profile("databricks_cluster")
    def test__databricks_cluster__failing_materialized_with_constraints(self, project):
        self.failing_materialized_with_not_null_constraint()

    @use_profile("databricks_cluster")
    def test__databricks_cluster__failing_constraint_check(self, project):
        self.failing_constraint_check()

    @use_profile("databricks_sql_endpoint")
    def test__databricks_sql_endpoint__materialized_with_constraints(self, project):
        self.materialized_with_constraints()

    @use_profile("databricks_sql_endpoint")
    def test__databricks_sql_endpoint__failing_materialized_with_constraints(self, project):
        self.failing_materialized_with_not_null_constraint()

    @use_profile("databricks_sql_endpoint")
    def test__databricks_sql_endpoint__failing_constraint_check(self, project):
        self.failing_constraint_check()
