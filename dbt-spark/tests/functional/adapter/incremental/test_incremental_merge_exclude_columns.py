import pytest

from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import (
    BaseMergeExcludeColumns,
)


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+file_format": "delta"}}
