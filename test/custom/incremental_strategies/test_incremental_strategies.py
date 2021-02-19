from cProfile import run
from test.custom.base import DBTSparkIntegrationTest, use_profile
import dbt.exceptions


class TestIncrementalStrategies(DBTSparkIntegrationTest):
    @property
    def schema(self):
        return "incremental_strategies"

    @property
    def project_config(self):
        return {
            'seeds': {
                'quote_columns': False,
            },
        }

    def seed_and_run_twice(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.run_dbt(["run"])


class TestDefaultAppend(TestIncrementalStrategies):
    @property
    def models(self):
        return "models"
        
    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("default_append", "expected_append")

    @use_profile("apache_spark")
    def test_default_append_apache_spark(self):
        self.run_and_test()

    @use_profile("databricks_cluster")
    def test_default_append_databricks_cluster(self):
        self.run_and_test()


class TestInsertOverwrite(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_insert_overwrite"

    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual(
            "insert_overwrite_no_partitions", "expected_overwrite")
        self.assertTablesEqual(
            "insert_overwrite_partitions", "expected_upsert")

    @use_profile("apache_spark")
    def test_insert_overwrite_apache_spark(self):
        self.run_and_test()

    @use_profile("databricks_cluster")
    def test_insert_overwrite_databricks_cluster(self):
        self.run_and_test()


class TestDeltaStrategies(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_delta"

    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("append_delta", "expected_append")
        self.assertTablesEqual("merge_no_key", "expected_append")
        self.assertTablesEqual("merge_unique_key", "expected_upsert")

    @use_profile("databricks_cluster")
    def test_delta_strategies_databricks_cluster(self):
        self.run_and_test()


class TestBadStrategies(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_bad"

    def run_and_test(self):
        results = self.run_dbt(["run"], expect_pass=False)
        # assert all models fail with compilation errors
        for result in results:
            self.assertEqual("error", result.status)
            self.assertIn("Compilation Error in model", result.message)

    @use_profile("apache_spark")
    def test_bad_strategies_apache_spark(self):
        self.run_and_test()

    @use_profile("databricks_cluster")
    def test_bad_strategies_databricks_cluster(self):
        self.run_and_test()

    @use_profile("databricks_sql_endpoint")
    def test_bad_strategies_databricks_sql_endpoint(self):
        self.run_and_test()
