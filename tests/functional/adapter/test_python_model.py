import os
import pytest
from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


@pytest.mark.skip_profile("apache_spark", "spark_session", "databricks_sql_endpoint")
class TestPythonModelSpark(BasePythonModelTests):
    pass


@pytest.mark.skip_profile("apache_spark", "spark_session", "databricks_sql_endpoint")
class TestPySpark(BasePySparkTests):
    pass


@pytest.mark.skip_profile("apache_spark", "spark_session", "databricks_sql_endpoint")
class TestPythonIncrementalModelSpark(BasePythonIncrementalTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {}


models__simple_python_model = """
import pandas
import torch
import spacy

def model(dbt, spark):
    dbt.config(
        materialized='table',
        submission_method='job_cluster',
        job_cluster_config={
            "spark_version": "7.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 0,
            "spark_conf": {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*, 4]"
            },
            "custom_tags": {
                "ResourceClass": "SingleNode"
            }
        },
        packages=['spacy', 'torch', 'pydantic<1.10.3']
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""
models__simple_python_model_v2 = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test1', 'test3'])
"""


@pytest.mark.skip_profile("apache_spark", "spark_session", "databricks_sql_endpoint")
class TestChangingSchemaSpark:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_python_model.py": models__simple_python_model}

    def test_changing_schema_with_log_validation(self, project, logs_dir):
        run_dbt(["run"])
        write_file(
            models__simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
        run_dbt(["run"])
        log_file = os.path.join(logs_dir, "dbt.log")
        with open(log_file, "r") as f:
            log = f.read()
            # validate #5510 log_code_execution works
            assert "On model.test.simple_python_model:" in log
            assert "spark.createDataFrame(data, schema=['test1', 'test3'])" in log
            assert "Execution status: OK in" in log
