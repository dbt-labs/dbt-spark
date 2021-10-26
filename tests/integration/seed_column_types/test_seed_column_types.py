from cProfile import run
from tests.integration.base import DBTIntegrationTest, use_profile
import dbt.exceptions


class TestSeedColumnTypeCast(DBTIntegrationTest):
    @property
    def schema(self):
        return "seed_column_types"
        
    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'seeds': {
                'quote_columns': False,
            },
        }

    # runs on Spark v2.0
    @use_profile("apache_spark")
    def test_seed_column_types_apache_spark(self):
        self.run_dbt(["seed"])

    # runs on Spark v3.0
    @use_profile("databricks_cluster")
    def test_seed_column_types_databricks_cluster(self):
        self.run_dbt(["seed"])

    # runs on Spark v3.0
    @use_profile("databricks_sql_endpoint")
    def test_seed_column_types_databricks_sql_endpoint(self):
        self.run_dbt(["seed"])
