from collections import namedtuple
from typing import Dict

import pytest

from dbt.contracts.results import TestStatus
from dbt.tests.adapter.store_test_failures_tests.basic import StoreTestFailures
from dbt.tests.util import run_dbt, check_relation_types


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestStoreTestFailuresDatabricks(StoreTestFailures):
    """
    Databricks works as expected. This tests all Databricks profiles as they are not skipped above.
    """

    pass


@pytest.mark.skip_profile("spark_session", "databricks_cluster", "databricks_sql_endpoint")
class TestStoreTestFailuresSpark(StoreTestFailures):
    """
    This is the same set of test cases as the test class above; it's the same subclass.

    Using "DELETE FROM" with Spark throws the following error:
    dbt.exceptions.DbtDatabaseError: Database Error
        org.apache.hive.service.cli.HiveSQLException:
            Error running query: org.apache.spark.sql.AnalysisException:
                DELETE is only supported with v2 tables.

    As a result, this class overrides `self.delete_record` to do nothing and then overrides the test
    only to skip updating the expected changes to reflect the absence of a delete.

    This should be updated in the future:
    - `self.delete_record` should be updated to properly delete the record by replacing the data frame
    with a filtered dataframe
    - the test case should be removed from here; it should not need to be altered once `self.delete_record`
    is updated correctly
    """

    def delete_record(self, project, record: Dict[str, str]):
        pass

    def test_tests_run_successfully_and_are_persisted_correctly(self, project):
        """
        This test case is overridden to back out the deletion check for whether the results are persisted as views.
        `self.delete_record` should be updated to delete correctly, and then this should be removed to run the
        default test case that's in `dbt-core`.
        """
        # set up the expected results
        TestResult = namedtuple("TestResult", ["name", "status", "type", "row_count"])
        expected_results = {
            TestResult("pass_with_view_strategy", TestStatus.Pass, "view", 0),
            TestResult("fail_with_view_strategy", TestStatus.Fail, "view", 1),
            TestResult("pass_with_table_strategy", TestStatus.Pass, "table", 0),
            TestResult("fail_with_table_strategy", TestStatus.Fail, "table", 1),
        }

        # run the tests once
        results = run_dbt(["test"], expect_pass=False)

        # show that the statuses are what we expect
        actual = {(result.node.name, result.status) for result in results}
        expected = {(result.name, result.status) for result in expected_results}
        assert actual == expected

        # show that the results are persisted in the correct database objects
        check_relation_types(
            project.adapter, {result.name: result.type for result in expected_results}
        )

        # show that only the failed records show up
        actual = {
            (result.name, self.row_count(project, result.name)) for result in expected_results
        }
        expected = {(result.name, result.row_count) for result in expected_results}
        assert actual == expected

        # insert a new record in the model that fails the "pass" tests
        # show that the view updates, but not the table
        self.insert_record(project, {"name": "dave", "shirt": "grape"})
        expected_results.remove(TestResult("pass_with_view_strategy", TestStatus.Pass, "view", 0))
        expected_results.add(TestResult("pass_with_view_strategy", TestStatus.Pass, "view", 1))

        # delete the original record from the model that failed the "fail" tests
        # show that the view updates, but not the table
        self.delete_record(project, {"name": "theodore", "shirt": "green"})
        # `delete_record` doesn't do anything right now, so the expected results should not be updated.
        # expected_results.remove(TestResult("fail_with_view_strategy", TestStatus.Fail, "view", 1))
        # expected_results.add(TestResult("fail_with_view_strategy", TestStatus.Fail, "view", 0))

        # show that the views update without needing to run dbt, but the tables do not update
        actual = {
            (result.name, self.row_count(project, result.name)) for result in expected_results
        }
        expected = {(result.name, result.row_count) for result in expected_results}
        assert actual == expected
