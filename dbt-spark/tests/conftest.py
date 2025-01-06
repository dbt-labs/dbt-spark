import pytest
import os

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="apache_spark", type=str)


# Using @pytest.mark.skip_profile('apache_spark') uses the 'skip_by_profile_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "databricks_cluster":
        target = databricks_cluster_target()
    elif profile_type == "databricks_sql_endpoint":
        target = databricks_sql_endpoint_target()
    elif profile_type == "apache_spark":
        target = apache_spark_target()
    elif profile_type == "databricks_http_cluster":
        target = databricks_http_cluster_target()
    elif profile_type == "spark_session":
        target = spark_session_target()
    elif profile_type == "spark_http_odbc":
        target = spark_http_odbc_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def apache_spark_target():
    return {
        "type": "spark",
        "host": "spark_db",
        "user": "dbt",
        "method": "thrift",
        "port": 10000,
        "connect_retries": 2,
        "connect_timeout": 3,
        "retry_all": False,
    }


def databricks_cluster_target():
    return {
        "type": "spark",
        "method": "odbc",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "cluster": os.getenv("DBT_DATABRICKS_CLUSTER_NAME"),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "driver": os.getenv("ODBC_DRIVER"),
        "port": 443,
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": False,
        "user": os.getenv("DBT_DATABRICKS_USER"),
    }


def databricks_sql_endpoint_target():
    return {
        "type": "spark",
        "method": "odbc",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "endpoint": os.getenv("DBT_DATABRICKS_ENDPOINT"),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "driver": os.getenv("ODBC_DRIVER"),
        "port": 443,
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": True,
    }


def databricks_http_cluster_target():
    return {
        "type": "spark",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "cluster": os.getenv("DBT_DATABRICKS_CLUSTER_NAME"),
        "token": os.getenv("DBT_DATABRICKS_TOKEN"),
        "method": "http",
        "port": 443,
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": False,
        "user": os.getenv("DBT_DATABRICKS_USER"),
    }


def spark_session_target():
    return {
        "type": "spark",
        "host": "localhost",
        "method": "session",
    }


def spark_http_odbc_target():
    return {
        "type": "spark",
        "method": "odbc",
        "host": os.getenv("DBT_DATABRICKS_HOST_NAME"),
        "port": 443,
        "driver": os.getenv("ODBC_DRIVER"),
        "connection_string_suffix": f'UID=token;PWD={os.getenv("DBT_DATABRICKS_TOKEN")};HTTPPath=/sql/1.0/endpoints/{os.getenv("DBT_DATABRICKS_ENDPOINT")};AuthMech=3;SparkServerType=3',
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": True,
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")
