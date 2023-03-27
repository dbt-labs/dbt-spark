import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from tests.functional.adapter.incremental_strategies.seeds import (
    expected_append_csv,
    expected_overwrite_csv,
    expected_upsert_csv,
    expected_partial_upsert_csv,
)
from tests.functional.adapter.incremental_strategies.fixtures import (
    bad_file_format_sql,
    bad_insert_overwrite_delta_sql,
    bad_merge_not_delta_sql,
    bad_strategy_sql,
    default_append_sql,
    insert_overwrite_no_partitions_sql,
    insert_overwrite_partitions_sql,
    append_delta_sql,
    delta_merge_no_key_sql,
    delta_merge_unique_key_sql,
    delta_merge_update_columns_sql,
)


class BaseIncrementalStrategies(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_append.csv": expected_append_csv,
            "expected_overwrite.csv": expected_overwrite_csv,
            "expected_upsert.csv": expected_upsert_csv,
            "expected_partial_upsert.csv": expected_partial_upsert_csv,
        }

    @staticmethod
    def seed_and_run_once():
        run_dbt(["seed"])
        run_dbt(["run"])

    @staticmethod
    def seed_and_run_twice():
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["run"])


class TestDefaultAppend(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {"default_append.sql": default_append_sql}

    def run_and_test(self, project):
        self.seed_and_run_twice()
        check_relations_equal(project.adapter, ["default_append", "expected_append"])

    @pytest.mark.skip_profile(
        "databricks_http_cluster", "databricks_sql_endpoint", "spark_session"
    )
    def test_default_append(self, project):
        self.run_and_test(project)


class TestInsertOverwrite(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "insert_overwrite_no_partitions.sql": insert_overwrite_no_partitions_sql,
            "insert_overwrite_partitions.sql": insert_overwrite_partitions_sql,
        }

    def run_and_test(self, project):
        self.seed_and_run_twice()
        check_relations_equal(
            project.adapter, ["insert_overwrite_no_partitions", "expected_overwrite"]
        )
        check_relations_equal(project.adapter, ["insert_overwrite_partitions", "expected_upsert"])

    @pytest.mark.skip_profile(
        "databricks_http_cluster", "databricks_sql_endpoint", "spark_session"
    )
    def test_insert_overwrite(self, project):
        self.run_and_test(project)


class TestDeltaStrategies(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "append_delta.sql": append_delta_sql,
            "merge_no_key.sql": delta_merge_no_key_sql,
            "merge_unique_key.sql": delta_merge_unique_key_sql,
            "merge_update_columns.sql": delta_merge_update_columns_sql,
        }

    def run_and_test(self, project):
        self.seed_and_run_twice()
        check_relations_equal(project.adapter, ["append_delta", "expected_append"])
        check_relations_equal(project.adapter, ["merge_no_key", "expected_append"])
        check_relations_equal(project.adapter, ["merge_unique_key", "expected_upsert"])
        check_relations_equal(project.adapter, ["merge_update_columns", "expected_partial_upsert"])

    @pytest.mark.skip_profile(
        "apache_spark", "databricks_http_cluster", "databricks_sql_endpoint", "spark_session"
    )
    def test_delta_strategies(self, project):
        self.run_and_test(project)


class TestBadStrategies(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_file_format.sql": bad_file_format_sql,
            "bad_insert_overwrite_delta.sql": bad_insert_overwrite_delta_sql,
            "bad_merge_not_delta.sql": bad_merge_not_delta_sql,
            "bad_strategy.sql": bad_strategy_sql,
        }

    @staticmethod
    def run_and_test():
        run_results = run_dbt(["run"], expect_pass=False)
        # assert all models fail with compilation errors
        for result in run_results:
            assert result.status == "error"
            assert "Compilation Error in model" in result.message

    @pytest.mark.skip_profile("databricks_http_cluster", "spark_session")
    def test_bad_strategies(self, project):
        self.run_and_test()
