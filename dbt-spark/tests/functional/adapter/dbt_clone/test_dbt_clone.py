import pytest
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClonePossible
from tests.functional.adapter.dbt_clone.fixtures import (
    seed_csv,
    table_model_sql,
    view_model_sql,
    ephemeral_model_sql,
    exposures_yml,
    schema_yml,
    snapshot_sql,
    get_schema_name_sql,
    macros_sql,
    infinite_macros_sql,
)


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSparkClonePossible(BaseClonePossible):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model_sql,
            "view_model.sql": view_model_sql,
            "ephemeral_model.sql": ephemeral_model_sql,
            "schema.yml": schema_yml,
            "exposures.yml": exposures_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            },
            "seeds": {
                "test": {
                    "quote_columns": False,
                },
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            },
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass
