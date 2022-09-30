from dbt.tests.adapter.utils import test_timestamps
import pytest


class TestCurrentTimestampSnowflake(test_timestamps.TestCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "timestamp",
            "current_timestamp_in_utc_backcompat": "timestamp",
            "current_timestamp_backcompat": "timestamp",
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """select CURRENT_TIMESTAMP() as current_timestamp,
                current_timestamp::timestamp as current_timestamp_in_utc_backcompat,
                current_timestamp::timestamp as current_timestamp_backcompat"""
