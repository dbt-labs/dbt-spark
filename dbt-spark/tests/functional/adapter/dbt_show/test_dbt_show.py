import pytest

from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowSqlHeader,
    BaseShowLimit,
    BaseShowDoesNotHandleDoubleLimit,
)


class TestSparkShowLimit(BaseShowLimit):
    pass


class TestSparkShowSqlHeader(BaseShowSqlHeader):
    pass


@pytest.mark.skip_profile("apache_spark", "spark_session", "databricks_http_cluster")
class TestSparkShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    """The syntax message is quite variable across clusters, but this hits two at once."""

    DATABASE_ERROR_MESSAGE = "limit"
