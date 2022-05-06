import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey


class TestUniqueKeySpark(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
            }
        }