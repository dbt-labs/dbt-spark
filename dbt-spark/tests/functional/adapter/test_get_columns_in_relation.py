import pytest

from dbt.tests.util import run_dbt, relation_from_name, check_relations_equal_with_relations


_MODEL_CHILD = "select 1"


_MODEL_PARENT = """
{% set cols = adapter.get_columns_in_relation(ref('child')) %}

select
    {% for col in cols %}
        {{ adapter.quote(col.column) }}{%- if not loop.last %},{{ '\n ' }}{% endif %}
    {% endfor %}
from {{ ref('child') }}
"""


class TestColumnsInRelation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "child.sql": _MODEL_CHILD,
            "parent.sql": _MODEL_PARENT,
        }

    @pytest.mark.skip_profile("databricks_http_cluster", "spark_session")
    def test_get_columns_in_relation(self, project):
        run_dbt(["run"])
        child = relation_from_name(project.adapter, "child")
        parent = relation_from_name(project.adapter, "parent")
        check_relations_equal_with_relations(project.adapter, [child, parent])
