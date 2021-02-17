from test.custom.base import DBTSparkIntegrationTest, use_profile
import dbt.exceptions


class TestIncrementalStrategies(DBTSparkIntegrationTest):
    @property
    def schema(self):
        return "incremental_strategies"

    @property
    def models(self):
        return "models"

    def run_and_test(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.assertTablesEqual("default_append", "expected_append")


class TestDefaultAppend(TestIncrementalStrategies):
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
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
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
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.assertTablesEqual("append_delta", "expected_append")
        self.assertTablesEqual("merge_no_key", "expected_append")
        self.assertTablesEqual("merge_unique_key", "expected_upsert")

    @use_profile("databricks_cluster")
    def test_delta_strategies_databricks_cluster(self):
        self.run_and_test()


class TestBadStrategies(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_insert_overwrite"

    def run_and_test(self):
        with self.assertRaises(dbt.exceptions.Exception) as exc:
            self.run_dbt(["compile"])
        message = str(exc.exception)
        self.assertIn("Invalid file format provided", message)
        self.assertIn("Invalid incremental strategy provided", message)

    @use_profile("apache_spark")
    def test_bad_strategies_apache_spark(self):
        self.run_and_test()

    @use_profile("databricks_cluster")
    def test_bad_strategies_databricks_cluster(self):
        self.run_and_test()


class TestBadStrategyWithEndpoint(TestInsertOverwrite):
    @use_profile("databricks_sql_endpoint")
    def test_bad_strategies_databricks_sql_endpoint(self):
        with self.assertRaises(dbt.exceptions.Exception) as exc:
            self.run_dbt(["compile"], "--target", "odbc-sql-endpoint")
        message = str(exc.exception)
        self.assertIn("Invalid incremental strategy provided", message)
