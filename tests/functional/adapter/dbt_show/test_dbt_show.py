from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowSqlHeader,
    BaseShowLimit,
    BaseShowDoesNotHandleDoubleLimit,
)


class TestSparkShowLimit(BaseShowLimit):
    pass


class TestSparkShowSqlHeader(BaseShowSqlHeader):
    pass


class TestSparkShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    pass
