from contextlib import contextmanager

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import ConnectionState, AdapterResponse
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS
from dbt.adapters.spark import __version__

try:
    from TCLIService.ttypes import TOperationState as ThriftState
    from thrift.transport import THttpClient
    from pyhive import hive
except ImportError:
    ThriftState = None
    THttpClient = None
    hive = None
try:
    import pyodbc
except ImportError:
    pyodbc = None
from datetime import datetime
import sqlparams
from dbt.contracts.connection import Connection
from dbt.dataclass_schema import StrEnum
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union, Tuple, List, Generator, Iterable, Sequence

from abc import ABC, abstractmethod

try:
    from thrift.transport.TSSLSocket import TSSLSocket
    import thrift
    import ssl
    import thrift_sasl
    from puresasl.client import SASLClient
except ImportError:
    pass  # done deliberately: setting modules to None explicitly violates MyPy contracts by degrading type semantics

import base64
import time

logger = AdapterLogger("Spark")

NUMBERS = DECIMALS + (int, float)


def _build_odbc_connnection_string(**kwargs: Any) -> str:
    return ";".join([f"{k}={v}" for k, v in kwargs.items()])


class SparkConnectionMethod(StrEnum):
    THRIFT = "thrift"
    HTTP = "http"
    ODBC = "odbc"
    SESSION = "session"


@dataclass
class SparkCredentials(Credentials):
    host: Optional[str] = None
    schema: Optional[str] = None  # type: ignore
    method: SparkConnectionMethod = None  # type: ignore
    database: Optional[str] = None  # type: ignore
    driver: Optional[str] = None
    cluster: Optional[str] = None
    endpoint: Optional[str] = None
    token: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: int = 443
    auth: Optional[str] = None
    kerberos_service_name: Optional[str] = None
    organization: str = "0"
    connect_retries: int = 0
    connect_timeout: int = 10
    use_ssl: bool = False
    server_side_parameters: Dict[str, str] = field(default_factory=dict)
    retry_all: bool = False

    @classmethod
    def __pre_deserialize__(cls, data: Any) -> Any:
        data = super().__pre_deserialize__(data)
        if "database" not in data:
            data["database"] = None
        return data

    @property
    def cluster_id(self) -> Optional[str]:
        return self.cluster

    def __post_init__(self) -> None:
        if self.method is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `method` in profile")
        if self.host is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `host` in profile")
        if self.schema is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `schema` in profile")

        # spark classifies database and schema as the same thing
        if self.database is not None and self.database != self.schema:
            raise dbt.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On Spark, database must be omitted or have the same value as"
                f" schema."
            )
        self.database = None

        if self.method == SparkConnectionMethod.ODBC:
            try:
                import pyodbc  # noqa: F401
            except ImportError as e:
                raise dbt.exceptions.DbtRuntimeError(
                    f"{self.method} connection method requires "
                    "additional dependencies. \n"
                    "Install the additional required dependencies with "
                    "`pip install dbt-spark[ODBC]`\n\n"
                    f"ImportError({e.msg})"
                ) from e

        if self.method == SparkConnectionMethod.ODBC and self.cluster and self.endpoint:
            raise dbt.exceptions.DbtRuntimeError(
                "`cluster` and `endpoint` cannot both be set when"
                f" using {self.method} method to connect to Spark"
            )

        if (
            self.method == SparkConnectionMethod.HTTP
            or self.method == SparkConnectionMethod.THRIFT
        ) and not (ThriftState and THttpClient and hive):
            raise dbt.exceptions.DbtRuntimeError(
                f"{self.method} connection method requires "
                "additional dependencies. \n"
                "Install the additional required dependencies with "
                "`pip install dbt-spark[PyHive]`"
            )

        if self.method == SparkConnectionMethod.SESSION:
            try:
                import pyspark  # noqa: F401
            except ImportError as e:
                raise dbt.exceptions.DbtRuntimeError(
                    f"{self.method} connection method requires "
                    "additional dependencies. \n"
                    "Install the additional required dependencies with "
                    "`pip install dbt-spark[session]`\n\n"
                    f"ImportError({e.msg})"
                ) from e

        if self.method != SparkConnectionMethod.SESSION:
            self.host = self.host.rstrip("/")

        self.server_side_parameters = {
            str(key): str(value) for key, value in self.server_side_parameters.items()
        }

    @property
    def type(self) -> str:
        return "spark"

    @property
    def unique_field(self) -> str:
        return self.host  # type: ignore

    def _connection_keys(self) -> Tuple[str, ...]:
        return "host", "port", "cluster", "endpoint", "schema", "organization"


class SparkConnectionWrapper(ABC):
    @abstractmethod
    def cursor(self) -> "SparkConnectionWrapper":
        pass

    @abstractmethod
    def cancel(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    @abstractmethod
    def fetchall(self) -> Optional[List]:
        pass

    @abstractmethod
    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        pass

    @property
    @abstractmethod
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        pass


class PyhiveConnectionWrapper(SparkConnectionWrapper):
    """Wrap a Spark connection in a way that no-ops transactions"""

    # https://forums.databricks.com/questions/2157/in-apache-spark-sql-can-we-roll-back-the-transacti.html  # noqa

    handle: "pyodbc.Connection"
    _cursor: "Optional[pyodbc.Cursor]"

    def __init__(self, handle: "pyodbc.Connection") -> None:
        self.handle = handle
        self._cursor = None

    def cursor(self) -> "PyhiveConnectionWrapper":
        self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.cancel()
            except EnvironmentError as exc:
                logger.debug("Exception while cancelling query: {}".format(exc))

    def close(self) -> None:
        if self._cursor:
            # Handle bad response in the pyhive lib when
            # the connection is cancelled
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug("Exception while closing cursor: {}".format(exc))
        self.handle.close()

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> List["pyodbc.Row"]:
        assert self._cursor, "Cursor not available"
        return self._cursor.fetchall()

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        # Reaching into the private enumeration here is bad form,
        # but there doesn't appear to be any way to determine that
        # a query has completed executing from the pyhive public API.
        # We need to use an async query + poll here, otherwise our
        # request may be dropped after ~5 minutes by the thrift server
        STATE_PENDING = [
            ThriftState.INITIALIZED_STATE,
            ThriftState.RUNNING_STATE,
            ThriftState.PENDING_STATE,
        ]

        STATE_SUCCESS = [
            ThriftState.FINISHED_STATE,
        ]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]

        assert self._cursor, "Cursor not available"

        self._cursor.execute(sql, bindings, async_=True)
        poll_state = self._cursor.poll()
        state = poll_state.operationState

        while state in STATE_PENDING:
            logger.debug("Poll status: {}, sleeping".format(state))

            poll_state = self._cursor.poll()
            state = poll_state.operationState

        # If an errorMessage is present, then raise a database exception
        # with that exact message. If no errorMessage is present, the
        # query did not necessarily succeed: check the state against the
        # known successful states, raising an error if the query did not
        # complete in a known good state. This can happen when queries are
        # cancelled, for instance. The errorMessage will be None, but the
        # state of the query will be "cancelled". By raising an exception
        # here, we prevent dbt from showing a status of OK when the query
        # has in fact failed.
        if poll_state.errorMessage:
            logger.debug("Poll response: {}".format(poll_state))
            logger.debug("Poll status: {}".format(state))
            raise dbt.exceptions.DbtDatabaseError(poll_state.errorMessage)

        elif state not in STATE_SUCCESS:
            status_type = ThriftState._VALUES_TO_NAMES.get(state, "Unknown<{!r}>".format(state))
            raise dbt.exceptions.DbtDatabaseError(
                "Query failed with status: {}".format(status_type)
            )

        logger.debug("Poll status: {}, query complete".format(state))

    @classmethod
    def _fix_binding(cls, value: Any) -> Union[float, str]:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return value

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        assert self._cursor, "Cursor not available"
        return self._cursor.description


class PyodbcConnectionWrapper(PyhiveConnectionWrapper):
    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        assert self._cursor, "Cursor not available"
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]
        # pyodbc does not handle a None type binding!
        if bindings is None:
            self._cursor.execute(sql)
        else:
            # pyodbc only supports `qmark` sql params!
            query = sqlparams.SQLParams("format", "qmark")
            sql, bindings = query.format(sql, bindings)
            self._cursor.execute(sql, *bindings)


class SparkConnectionManager(SQLConnectionManager):
    TYPE = "spark"

    SPARK_CLUSTER_HTTP_PATH = "/sql/protocolv1/o/{organization}/{cluster}"
    SPARK_SQL_ENDPOINT_HTTP_PATH = "/sql/1.0/endpoints/{endpoint}"
    SPARK_CONNECTION_URL = "{host}:{port}" + SPARK_CLUSTER_HTTP_PATH

    @contextmanager
    def exception_handler(self, sql: str) -> Generator[None, None, None]:
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.DbtRuntimeError(msg)
            else:
                raise dbt.exceptions.DbtRuntimeError(str(exc))

    def cancel(self, connection: Connection) -> None:
        connection.handle.cancel()

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = "OK"
        return AdapterResponse(_message=message)

    # No transactions on Spark....
    def add_begin_query(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: commit")

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    @classmethod
    def validate_creds(cls, creds: Any, required: Iterable[str]) -> None:
        method = creds.method

        for key in required:
            if not hasattr(creds, key):
                raise dbt.exceptions.DbtProfileError(
                    "The config '{}' is required when using the {} method"
                    " to connect to Spark".format(key, method)
                )

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        exc = None
        handle: SparkConnectionWrapper

        for i in range(1 + creds.connect_retries):
            try:
                if creds.method == SparkConnectionMethod.HTTP:
                    cls.validate_creds(creds, ["token", "host", "port", "cluster", "organization"])

                    # Prepend https:// if it is missing
                    host = creds.host
                    if not host.startswith("https://"):
                        host = "https://" + creds.host

                    conn_url = cls.SPARK_CONNECTION_URL.format(
                        host=host,
                        port=creds.port,
                        organization=creds.organization,
                        cluster=creds.cluster,
                    )

                    logger.debug("connection url: {}".format(conn_url))

                    transport = THttpClient.THttpClient(conn_url)

                    raw_token = "token:{}".format(creds.token).encode()
                    token = base64.standard_b64encode(raw_token).decode()
                    transport.setCustomHeaders({"Authorization": "Basic {}".format(token)})

                    conn = hive.connect(
                        thrift_transport=transport,
                        configuration=creds.server_side_parameters,
                    )
                    handle = PyhiveConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.THRIFT:
                    cls.validate_creds(creds, ["host", "port", "user", "schema"])

                    if creds.use_ssl:
                        transport = build_ssl_transport(
                            host=creds.host,
                            port=creds.port,
                            username=creds.user,
                            auth=creds.auth,
                            kerberos_service_name=creds.kerberos_service_name,
                            password=creds.password,
                        )
                        conn = hive.connect(
                            thrift_transport=transport,
                            configuration=creds.server_side_parameters,
                        )
                    else:
                        conn = hive.connect(
                            host=creds.host,
                            port=creds.port,
                            username=creds.user,
                            auth=creds.auth,
                            kerberos_service_name=creds.kerberos_service_name,
                            password=creds.password,
                            configuration=creds.server_side_parameters,
                        )  # noqa
                    handle = PyhiveConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.ODBC:
                    if creds.cluster is not None:
                        required_fields = [
                            "driver",
                            "host",
                            "port",
                            "token",
                            "organization",
                            "cluster",
                        ]
                        http_path = cls.SPARK_CLUSTER_HTTP_PATH.format(
                            organization=creds.organization, cluster=creds.cluster
                        )
                    elif creds.endpoint is not None:
                        required_fields = ["driver", "host", "port", "token", "endpoint"]
                        http_path = cls.SPARK_SQL_ENDPOINT_HTTP_PATH.format(
                            endpoint=creds.endpoint
                        )
                    else:
                        raise dbt.exceptions.DbtProfileError(
                            "Either `cluster` or `endpoint` must set when"
                            " using the odbc method to connect to Spark"
                        )

                    cls.validate_creds(creds, required_fields)

                    dbt_spark_version = __version__.version
                    user_agent_entry = (
                        f"dbt-labs-dbt-spark/{dbt_spark_version} (Databricks)"  # noqa
                    )

                    # http://simba.wpengine.com/products/Spark/doc/ODBC_InstallGuide/unix/content/odbc/hi/configuring/serverside.htm
                    ssp = {f"SSP_{k}": f"{{{v}}}" for k, v in creds.server_side_parameters.items()}

                    # https://www.simba.com/products/Spark/doc/v2/ODBC_InstallGuide/unix/content/odbc/options/driver.htm
                    connection_str = _build_odbc_connnection_string(
                        DRIVER=creds.driver,
                        HOST=creds.host,
                        PORT=creds.port,
                        UID="token",
                        PWD=creds.token,
                        HTTPPath=http_path,
                        AuthMech=3,
                        SparkServerType=3,
                        ThriftTransport=2,
                        SSL=1,
                        UserAgentEntry=user_agent_entry,
                        LCaseSspKeyName=0 if ssp else 1,
                        **ssp,
                    )

                    conn = pyodbc.connect(connection_str, autocommit=True)
                    handle = PyodbcConnectionWrapper(conn)
                elif creds.method == SparkConnectionMethod.SESSION:
                    from .session import (  # noqa: F401
                        Connection,
                        SessionConnectionWrapper,
                    )

                    handle = SessionConnectionWrapper(
                        Connection(server_side_parameters=creds.server_side_parameters)
                    )
                else:
                    raise dbt.exceptions.DbtProfileError(
                        f"invalid credential method: {creds.method}"
                    )
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a token expired, or something
                    msg = "Failed to connect"
                    if creds.token is not None:
                        msg += ", is your token valid?"
                    raise dbt.exceptions.FailedToConnectError(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {getattr(exc, 'message', 'No message')}, "
                        f"retrying due to 'retry_all' configuration "
                        f"set to true.\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise dbt.exceptions.FailedToConnectError("failed to connect") from e
        else:
            raise exc  # type: ignore

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[type, str]) -> str:  # type: ignore
        """
        :param Union[type, str] type_code: The sql to execute.
            * type_code is a python type (!) in pyodbc https://github.com/mkleehammer/pyodbc/wiki/Cursor#description, and a string for other spark runtimes.
            * ignoring the type annotation on the signature for this adapter instead of updating the base class because this feels like a really special case.
        :return: stringified the cursor type_code
        :rtype: str
        """
        if isinstance(type_code, str):
            return type_code
        return type_code.__name__.upper()


def build_ssl_transport(
    host: str,
    port: int,
    username: str,
    auth: str,
    kerberos_service_name: str,
    password: Optional[str] = None,
) -> "thrift_sasl.TSaslClientTransport":
    transport = None
    if port is None:
        port = 10000
    if auth is None:
        auth = "NONE"
    socket = TSSLSocket(host, port, cert_reqs=ssl.CERT_NONE)
    if auth == "NOSASL":
        # NOSASL corresponds to hive.server2.authentication=NOSASL
        # in hive-site.xml
        transport = thrift.transport.TTransport.TBufferedTransport(socket)
    elif auth in ("LDAP", "KERBEROS", "NONE", "CUSTOM"):
        # Defer import so package dependency is optional
        if auth == "KERBEROS":
            # KERBEROS mode in hive.server2.authentication is GSSAPI
            # in sasl library
            sasl_auth = "GSSAPI"
        else:
            sasl_auth = "PLAIN"
            if password is None:
                # Password doesn't matter in NONE mode, just needs
                # to be nonempty.
                password = "x"

        def sasl_factory() -> SASLClient:
            if sasl_auth == "GSSAPI":
                sasl_client = SASLClient(host, kerberos_service_name, mechanism=sasl_auth)
            elif sasl_auth == "PLAIN":
                sasl_client = SASLClient(
                    host, mechanism=sasl_auth, username=username, password=password
                )
            else:
                raise AssertionError
            return sasl_client

        transport = thrift_sasl.TSaslClientTransport(sasl_factory, sasl_auth, socket)
    return transport


def _is_retryable_error(exc: Exception) -> str:
    message = str(exc).lower()
    if "pending" in message or "temporarily_unavailable" in message:
        return str(exc)
    else:
        return ""
