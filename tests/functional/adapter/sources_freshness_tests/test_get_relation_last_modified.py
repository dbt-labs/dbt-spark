import os
import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.sources_freshness_tests import files


class TestGetLastRelationModified:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "test_source_no_last_modified.csv": files.SEED_TEST_SOURCE_NO_LAST_MODIFIED_CSV,
            "test_source_last_modified.csv": files.SEED_TEST_SOURCE_LAST_MODIFIED_CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": files.SCHEMA_YML}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        # we need the schema name for the sources section
        os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"] = project.test_schema
        run_dbt(["seed"])
        yield
        del os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]

    @pytest.mark.parametrize(
        "source,status",
        [
            ("test_source.test_source_last_modified", "error"),  # stale
        ],
    )
    def test_get_last_relation_modified(self, project, source, status):
        statuses = {"pass": True, "error": False}
        results = run_dbt(
            ["source", "freshness", "--select", f"source:{source}"], expect_pass=statuses[status]
        )
        assert len(results) == 1
        result = results[0]
        assert result.status == status
