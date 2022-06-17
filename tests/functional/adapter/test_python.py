import pytest
from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import BasePythonModelTests

class TestPythonModelSpark(BasePythonModelTests):
    pass

models__simple_python_model = """
import pandas

def model(dbt):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""
models__simple_python_model_v2 = """
import pandas

def model(dbt):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test1', 'test3'])
"""

class TestChangingSchemaSnowflake:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_python_model.py": models__simple_python_model
            }
    def test_changing_schema(self,project):
        run_dbt(["run"])
        write_file(models__simple_python_model_v2, project.project_root + '/models', "simple_python_model.py")
        run_dbt(["run"])