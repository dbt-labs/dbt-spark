from tests.integration.base import DBTIntegrationTest, use_profile


class TestGetColumnInRelationInSameRun(DBTIntegrationTest):
    @property
    def schema(self):
        return "get_columns_in_relation"

    @property
    def models(self):
        return "models"

    def run_and_test(self):
        self.run_dbt(["run"])
        self.assertTablesEqual("child", "get_columns_from_child")

    @use_profile("apache_spark")
    def test_get_columns_in_relation_in_same_run_apache_spark(self):
        self.run_and_test()

    @use_profile("databricks_cluster")
    def test_get_columns_in_relation_in_same_run_databricks_cluster(self):
        self.run_and_test()

    @use_profile("databricks_sql_endpoint")
    def test_get_columns_in_relation_in_same_run_databricks_sql_endpoint(self):
        self.run_and_test()
