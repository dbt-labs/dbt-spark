import os
import pytest
from dbt.tests.util import run_dbt, write_file
from dbt.tests.adapter.python_model.test_python_model import (
    BasePythonModelTests,
    BasePythonIncrementalTests,
)
from dbt.tests.adapter.python_model.test_spark import BasePySparkTests


@pytest.mark.skip_profile(
    "apache_spark", "spark_session", "databricks_sql_endpoint", "spark_http_odbc"
)
class TestPythonModelSpark(BasePythonModelTests):
    pass


@pytest.mark.skip_profile(
    "apache_spark", "spark_session", "databricks_sql_endpoint", "spark_http_odbc"
)
class TestPySpark(BasePySparkTests):
    def test_different_dataframes(self, project):
        """
        Test that python models are supported using dataframes from:
        - pandas
        - pyspark
        - pyspark.pandas (formerly dataspark.koalas)

        Note:
            The CI environment is on Apache Spark >3.1, which includes koalas as pyspark.pandas.
            The only Databricks runtime that supports Apache Spark <=3.1 is 9.1 LTS, which is EOL 2024-09-23.
            For more information, see:
            - https://github.com/databricks/koalas
            - https://docs.databricks.com/en/release-notes/runtime/index.html
        """
        results = run_dbt(["run", "--exclude", "koalas_df"])
        assert len(results) == 3


@pytest.mark.skip_profile(
    "apache_spark", "spark_session", "databricks_sql_endpoint", "spark_http_odbc"
)
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
            "spark_version": "12.2.x-scala2.12",
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
        packages=['spacy', 'torch', 'pydantic>=1.10.8', 'numpy<2']
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


@pytest.mark.skip_profile(
    "apache_spark",
    "spark_session",
    "databricks_sql_endpoint",
    "spark_http_odbc",
    "databricks_http_cluster",
)
class TestChangingSchemaSpark:
    """
    Confirm that we can setup a spot instance and parse required packages into the Databricks job.

    Notes:
        - This test generates a spot instance on demand using the settings from `job_cluster_config`
        in `models__simple_python_model` above. It takes several minutes to run due to creating the cluster.
        The job can be monitored via "Data Engineering > Job Runs" or "Workflows > Job Runs"
        in the Databricks UI (instead of via the normal cluster).
        - The `spark_version` argument will need to periodically be updated. It will eventually become
        unsupported and start experiencing issues.
        - See https://github.com/explosion/spaCy/issues/12659 for why we're pinning pydantic
    """

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
