import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey


@pytest.mark.skip_profile("spark_session", "apache_spark", "spark_connect")
class TestUniqueKeySpark(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
            }
        }
