import pytest

from dbt.tests.adapter.persist_test_results.basic import PersistTestResults


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestPersistTestResultsDatabricks(PersistTestResults):
    pass


@pytest.mark.skip_profile("spark_session", "databricks_cluster", "databricks_sql_endpoint")
class TestPersistTestResultsSpark(PersistTestResults):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {"quote_columns": True},
            "tests": {"+schema": self.audit_schema_suffix},
        }
