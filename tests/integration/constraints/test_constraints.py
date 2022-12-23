import pytest
import json
from dbt.tests.util import run_dbt, run_dbt_and_capture
from tests.integration.base import use_profile
import logging

models__constraints_column_types_sql = """
select
    1000 as int_column,
    99.99 as float_column,
    true as bool_column,
    '2022-01-01'::date as date_column
"""

models__constraints_incorrect_column_types_sql = """
select
    1000 as int_column,
    99.99 as float_column,
    true as bool_column,
    '2022-01-01'::date as date_column
"""

models__constraints_not_null_sql = """
select
    1000 as int_column,
    99.99 as float_column,
    true as bool_column,
    '2022-01-01'::date as date_column

union all

select
    NULL as int_column,
    99.99 as float_column,
    true as bool_column,
    '2022-01-01'::date as date_column
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
        check: "int_column > 0"
      - name: float_column
        description: "Test for int type"
        data_type: float
        check: "float_column > 0"
      - name: bool_column
        description: "Test for int type"
        data_type: boolean
      - name: date_column
        description: "Test for int type"
        data_type: date

  - name: constraints_incorrect_column_types
    description: "Model to test failing column data type constraints"
    config:
      constraints_enabled: true
    columns:
      - name: int_column
        description: "Test for int type"
        data_type: boolean
      - name: float_column
        description: "Test for int type"
        data_type: date
      - name: bool_column
        description: "Test for int type"
        data_type: int
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
          - unique
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


class TestMaterializedWithConstraints:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "constraints_column_types.sql": models__constraints_column_types_sql,
            "constraints_incorrect_column_types.sql": models__constraints_incorrect_column_types_sql,
            "constraints_not_null.sql": models__constraints_not_null_sql,
            "models_config.yml": models__models_config_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, prefix):
        return {
            "config-version": 2,
            "models": {
                "materialized": "table",
            },
        }

    @use_profile('apache_spark')
    def test__apache_spark__materialized_with_constraints(self, project):
        _, stdout = run_dbt_and_capture(["run", "--select", "constraints_column_types"])
        found_check_config_str = "We noticed you have `check` configs"
        number_times_print_found_check_config = stdout.count(found_check_config_str)
        assert number_times_print_found_check_config == 1

    @use_profile('apache_spark')
    def test__apache_spark__failing_materialized_with_constraints(self, project):
        result = run_dbt(
            ["run", "--select", "constraints_incorrect_column_types"], expect_pass=False
        )
        assert "incompatible types" in result.results[0].message

    @use_profile('apache_spark')
    def test__apache_spark__failing_not_null_constraint(self, project):
        result = run_dbt(["run", "--select", "constraints_not_null"], expect_pass=False)
        assert "NULL result in a non-nullable column" in result.results[0].message

    @use_profile('apache_spark')
    def test__apache_spark__rollback(self, project):

        # run the correct model and modify it to fail
        run_dbt(["run", "--select", "constraints_column_types"])

        with open("./models/constraints_column_types.sql", "r") as fp:
            my_model_sql_original = fp.read()

        my_model_sql_error = my_model_sql_original.replace(
            "1000 as int_column", "'a' as int_column"
        )

        with open("./models/constraints_column_types.sql", "w") as fp:
            fp.write(my_model_sql_error)

        # run the failed model
        results = run_dbt(["run", "--select", "constraints_column_types"], expect_pass=False)

        with open("./target/manifest.json", "r") as fp:
            generated_manifest = json.load(fp)

        model_unique_id = "model.test.constraints_column_types"
        schema_name_generated = generated_manifest["nodes"][model_unique_id]["schema"]
        database_name_generated = generated_manifest["nodes"][model_unique_id]["database"]

        # verify the previous table exists
        sql = f"""
            select int_column from {database_name_generated}.{schema_name_generated}.constraints_column_types where int_column = 1000
        """
        results = project.run_sql(sql, fetch="all")
        assert len(results) == 1
        assert results[0][0] == 1000
