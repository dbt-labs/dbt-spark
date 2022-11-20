from cProfile import run
from tests.integration.base import DBTIntegrationTest, use_profile
import dbt.exceptions

class TestRegisterPythonUDF(DBTIntegrationTest):
    @property
    def schema(self):
        return "python_udf"

    @property
    def models(self):
        return "models"

    def run_and_test(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.assertTablesEqual("python_udf", "expected_python_udf")

    @use_profile("python_hooks")
    def test_udf_python_hooks(self):
        self.run_and_test()