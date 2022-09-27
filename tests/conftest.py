import pytest
import os
from xdist import is_xdist_controller
from xdist.scheduler.loadscope import LoadScopeScheduling

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="apache_spark", type=str)


# Using @pytest.mark.skip_profile('apache_spark') uses the 'skip_by_profile_type'
# autouse fixture below
def pytest_configure(config):
    config.pluginmanager.register(XDistSerialPlugin())
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )
    config.addinivalue_line(
        "markers",
        "serial: run all tests with mark in single process",
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
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def apache_spark_target():
    return {
        "type": "spark",
        "host": "localhost",
        "user": "dbt",
        "method": "thrift",
        "port": 10000,
        "connect_retries": 3,
        "connect_timeout": 5,
        "retry_all": True,
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
        "retry_all": True,
        "user": os.getenv('DBT_DATABRICKS_USER'),
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
        "host": os.getenv('DBT_DATABRICKS_HOST_NAME'),
        "cluster": os.getenv('DBT_DATABRICKS_CLUSTER_NAME'),
        "token": os.getenv('DBT_DATABRICKS_TOKEN'),
        "method": "http",
        "port": 443,
        # more retries + longer timout to handle unavailability while cluster is restarting
        # return failures quickly in dev, retry all failures in CI (up to 5 min)
        "connect_retries": 5,
        "connect_timeout": 60, 
        "retry_all": bool(os.getenv('DBT_DATABRICKS_RETRY_ALL', False)),
        "user": os.getenv('DBT_DATABRICKS_USER'),
    }


def spark_session_target():
    return {
        "type": "spark",
        "host": "localhost",
        "method": "session",
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")


# solution from https://github.com/pytest-dev/pytest-xdist/issues/385#issuecomment-1176976154
class XDistSerialPlugin:
    def __init__(self):
        self._nodes = None

    @pytest.hookimpl(tryfirst=True)
    def pytest_collection(self, session):
        if is_xdist_controller(session):
            self._nodes = {
                item.nodeid: item
                for item in session.perform_collect(None)
            }
            return True

    def pytest_xdist_make_scheduler(self, config, log):
        return SerialScheduling(config, log, nodes=self._nodes)


class SerialScheduling(LoadScopeScheduling):
    def __init__(self, config, log, *, nodes):
        super().__init__(config, log)
        self._nodes = nodes

    def _split_scope(self, nodeid):
        node = self._nodes[nodeid]
        if node.get_closest_marker("serial"):
            # put all `@pytest.mark.serial` tests in same scope, to
            # ensure they're all run in the same worker
            return "__serial__"

        # otherwise, each test is in its own scope
        return nodeid