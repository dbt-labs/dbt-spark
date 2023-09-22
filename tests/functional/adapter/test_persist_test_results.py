import pytest

from dbt.tests.adapter.persist_test_results.basic import PersistTestResults


@pytest.mark.skip_profile("spark_session", "databricks_cluster", "databricks_sql_endpoint")
class TestPersistTestResultsSpark(PersistTestResults):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": True,
            },
            "tests": {
                "+schema": self.audit_schema_suffix,
            },
        }


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestPersistTestResultsDatabricks(PersistTestResults):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "+file_format": "delta",
            },
            "tests": {"+schema": self.audit_schema_suffix, "+file_format": "delta"},
        }
