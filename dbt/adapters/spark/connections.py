from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions

from pyhive import hive
from thrift.transport import THttpClient
import base64


SPARK_CONNECTION_URL = "https://{host}:{port}/sql/protocolv1/o/0/{cluster}"

SPARK_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'host': {
            'type': 'string'
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'cluster': {
            'type': 'string'
        },
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'token': {
            'type': 'string',
        },
    },
    'required': ['host', 'database', 'schema', 'cluster'],
}


class SparkCredentials(Credentials):
    SCHEMA = SPARK_CREDENTIALS_CONTRACT

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('database', kwargs.get('schema'))

        super(SparkCredentials, self).__init__(*args, **kwargs)

    @property
    def type(self):
        return 'spark'

    def _connection_keys(self):
        return ('host', 'port', 'cluster', 'schema')


class ConnectionWrapper(object):
    "Wrap a Spark connection in a way that no-ops transactions"
    # https://forums.databricks.com/questions/2157/in-apache-spark-sql-can-we-roll-back-the-transacti.html
    def __init__(self, handle):
        self.handle = handle
        self._cursor = None
        self._fetch_result = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor is not None:
            self._cursor.cancel()

    def close(self):
        self.handle.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        return self._cursor.execute(sql, bindings)

    @property
    def description(self):
        return self._cursor.description


class SparkConnectionManager(SQLConnectionManager):
    TYPE = 'spark'

    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, 'status'):
                msg = thrift_resp.status.errorMessage
                raise dbt.exceptions.RuntimeException(msg)
            else:
                raise dbt.exceptions.RuntimeException(str(exc))

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args, **kwargs):
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        conn_url = SPARK_CONNECTION_URL.format(**connection.credentials)
        transport = THttpClient.THttpClient(conn_url)

        creds = "token:{}".format(connection.credentials['token']).encode()
        token = base64.standard_b64encode(creds).decode()
        transport.setCustomHeaders({
            'Authorization': 'Basic {}'.format(token)
        })

        conn = hive.connect(thrift_transport=transport)
        wrapped = ConnectionWrapper(conn)

        connection.state = 'open'
        connection.handle = wrapped
        return connection

    @classmethod
    def get_status(cls, cursor):
        #status = cursor._cursor.poll()
        return 'OK'

    def cancel(self, connection):
        import ipdb; ipdb.set_trace()
        connection.handle.cancel()
