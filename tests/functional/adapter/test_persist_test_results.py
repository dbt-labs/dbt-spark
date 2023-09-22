import pytest

from dbt.tests.adapter.persist_test_results.basic import PersistTestResults


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestPersistTestResults(PersistTestResults):
    pass
