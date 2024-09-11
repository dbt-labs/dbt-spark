import pytest

from tests.unit.utils import config_from_parts_or_dicts


@pytest.fixture(scope="session", autouse=True)
def base_project_cfg():
    return {
        "name": "X",
        "version": "0.1",
        "profile": "test",
        "project-root": "/tmp/dbt/does-not-exist",
        "quoting": {
            "identifier": False,
            "schema": False,
        },
        "config-version": 2,
    }


@pytest.fixture(scope="session", autouse=True)
def target_http(base_project_cfg):
    config = config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "http",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 443,
                    "token": "abc123",
                    "organization": "0123456789",
                    "cluster": "01234-23423-coffeetime",
                    "server_side_parameters": {"spark.driver.memory": "4g"},
                }
            },
            "target": "test",
        },
    )
    return config


@pytest.fixture(scope="session", autouse=True)
def target_thrift(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "thrift",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 10001,
                    "user": "dbt",
                }
            },
            "target": "test",
        },
    )


@pytest.fixture(scope="session", autouse=True)
def target_thrift_kerberos(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "thrift",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 10001,
                    "user": "dbt",
                    "auth": "KERBEROS",
                    "kerberos_service_name": "hive",
                }
            },
            "target": "test",
        },
    )


@pytest.fixture(scope="session", autouse=True)
def target_use_ssl_thrift(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "thrift",
                    "use_ssl": True,
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 10001,
                    "user": "dbt",
                }
            },
            "target": "test",
        },
    )


@pytest.fixture(scope="session", autouse=True)
def target_odbc_cluster(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "odbc",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 443,
                    "token": "abc123",
                    "organization": "0123456789",
                    "cluster": "01234-23423-coffeetime",
                    "driver": "Simba",
                }
            },
            "target": "test",
        },
    )


@pytest.fixture(scope="session", autouse=True)
def target_odbc_sql_endpoint(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "odbc",
                    "schema": "analytics",
                    "host": "myorg.sparkhost.com",
                    "port": 443,
                    "token": "abc123",
                    "endpoint": "012342342393920a",
                    "driver": "Simba",
                }
            },
            "target": "test",
        },
    )


@pytest.fixture(scope="session", autouse=True)
def target_odbc_with_extra_conn(base_project_cfg):
    return config_from_parts_or_dicts(
        base_project_cfg,
        {
            "outputs": {
                "test": {
                    "type": "spark",
                    "method": "odbc",
                    "host": "myorg.sparkhost.com",
                    "schema": "analytics",
                    "port": 443,
                    "driver": "Simba",
                    "connection_string_suffix": "someExtraValues",
                    "connect_retries": 3,
                    "connect_timeout": 5,
                    "retry_all": True,
                }
            },
            "target": "test",
        },
    )
