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
