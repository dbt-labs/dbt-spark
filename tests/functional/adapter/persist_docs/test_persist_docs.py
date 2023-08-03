import pytest

from dbt.tests.util import run_dbt

from fixtures import (
    _MODELS__MY_FUN_DOCS,
    _MODELS__INCREMENTAL_DELTA,
    _MODELS__TABLE_DELTA_MODEL,
    _MODELS__TABLE_DELTA_MODEL_MISSING_COLUMN,
    _PROPERTIES__MODELS,
    _PROPERTIES__SEEDS,
    _SEEDS__BASIC,
)


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestPersistDocsDeltaTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_delta_model.sql": _MODELS__INCREMENTAL_DELTA,
            "my_fun_docs.md": _MODELS__MY_FUN_DOCS,
            "table_delta_model.sql": _MODELS__TABLE_DELTA_MODEL,
            "schema.yml": _PROPERTIES__MODELS,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__BASIC, "seed.yml": _PROPERTIES__SEEDS}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
            "seeds": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                    "+file_format": "delta",
                    "+quote_columns": True,
                }
            },
        }

    def test_delta_comments(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        for table, whatis in [
            ("table_delta_model", "Table"),
            ("seed", "Seed"),
            ("incremental_delta_model", "Incremental"),
        ]:
            results = project.run_sql(
                "describe extended {schema}.{table}".format(
                    schema=project.test_schema, table=table
                ),
                fetch="all",
            )

            for result in results:
                if result[0] == "Comment":
                    assert result[1].startswith(f"{whatis} model description")
                if result[0] == "id":
                    assert result[2].startswith("id Column description")
                if result[0] == "name":
                    assert result[2].startswith("Some stuff here and then a call to")


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestPersistDocsMissingColumn:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__BASIC, "seed.yml": _PROPERTIES__SEEDS}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_delta_model.sql": _MODELS__TABLE_DELTA_MODEL_MISSING_COLUMN,
            "my_fun_docs.md": _MODELS__MY_FUN_DOCS,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": _PROPERTIES__MODELS}

    def test_missing_column(self, project):
        """spark will use our schema to verify all columns exist rather than fail silently"""
        run_dbt(["seed"])
        res = run_dbt(["run"], expect_pass=False)
        assert "Missing field name in table" in res[0].message
