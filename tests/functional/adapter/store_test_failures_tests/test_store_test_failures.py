import pytest

from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    StoreTestFailuresBase,
    TEST_AUDIT_SCHEMA_SUFFIX,
)


@pytest.mark.skip_profile("spark_session", "databricks_cluster", "databricks_sql_endpoint")
class TestSparkStoreTestFailures(StoreTestFailuresBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": True,
            },
            "tests": {"+schema": TEST_AUDIT_SCHEMA_SUFFIX, "+store_failures": True},
        }

    def test_store_and_assert(self, project):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSparkStoreTestFailuresWithDelta(StoreTestFailuresBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
                "test": self.column_type_overrides(),
                "+file_format": "delta",
            },
            "tests": {
                "+schema": TEST_AUDIT_SCHEMA_SUFFIX,
                "+store_failures": True,
                "+file_format": "delta",
            },
        }

    def test_store_and_assert_failure_with_delta(self, project):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)
