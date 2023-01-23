import pytest

from dbt.tests.util import run_dbt
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import BaseMergeExcludeColumns

@pytest.mark.skip_profile('spark_session', 'apache_spark')
class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    pass
