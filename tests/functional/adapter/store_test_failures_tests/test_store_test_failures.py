import pytest

from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import StoreTestFailuresBase

@pytest.mark.skip_profile('spark_session')
class TestSparkStoreTestFailures(StoreTestFailuresBase):
    def test_store_and_assert(self, project):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)

@pytest.mark.skip_profile('apache_spark', 'spark_session')
class TestSparkStoreTestFailuresWithDelta(StoreTestFailuresBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'tests': {
                '+store_failures': True,
                '+severity': 'warn',
                '+file_format': 'delta',
            }
        }

    def test_store_and_assert_failure_with_delta(self, project):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)
