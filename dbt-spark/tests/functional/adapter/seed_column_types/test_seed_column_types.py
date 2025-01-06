import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.seed_column_types.fixtures import (
    _MACRO_TEST_IS_TYPE_SQL,
    _SEED_CSV,
    _SEED_YML,
)


@pytest.mark.skip_profile("spark_session")
class TestSeedColumnTypesCast:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_is_type.sql": _MACRO_TEST_IS_TYPE_SQL}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"payments.csv": _SEED_CSV, "schema.yml": _SEED_YML}

    #  We want to test seed types because hive would cause all fields to be strings.
    # setting column_types in project.yml should change them and pass.
    def test_column_seed_type(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        run_dbt(["test"], expect_pass=False)
