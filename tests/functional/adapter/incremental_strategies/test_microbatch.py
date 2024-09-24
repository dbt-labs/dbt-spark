import pytest

from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)

# No requirement for a unique_id for spark microbatch!
_microbatch_model_no_unique_id_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0), partition_by=['date_day'], file_format='parquet') }}
select *, cast(event_time as date) as date_day
from {{ ref('input_model') }}
"""


@pytest.mark.skip_profile(
    "databricks_http_cluster", "databricks_sql_endpoint", "spark_session", "spark_http_odbc"
)
class TestMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_model_no_unique_id_sql
