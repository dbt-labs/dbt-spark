import pytest

from dbt.tests.util import run_dbt
from dbt.tests.adapter.incremental.test_incremental_merge_exclude_columns import BaseMergeExcludeColumns

class TestMergeExcludeColumns(BaseMergeExcludeColumns):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"file_format": "delta"}}

