from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowSqlHeader,
    BaseShowLimit,
    BaseShowDoesNotHandleDoubleLimit,
)


class TestSparkShowLimit(BaseShowLimit):
    pass


class TestSparkShowSqlHeader(BaseShowSqlHeader):
    pass


@pytest.mark.skip_profile("spark_session")
class TestSparkShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    pass
