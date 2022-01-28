import pytest
from pyspark.sql import SparkSession


@pytest.fixture(autouse=True)
def spark(spark_session: SparkSession) -> SparkSession:
    """Force use Spark session."""
    return spark_session


def pytest_configure(config):
    config.addinivalue_line("markers", "profile_databricks_cluster")
    config.addinivalue_line("markers", "profile_databricks_sql_endpoint")
    config.addinivalue_line("markers", "profile_apache_spark")
