from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
import dbt.exceptions

import jaydebeapi

JDBC_CONN_STRING = 'jdbc:spark://{creds.host}:{creds.port}/{creds.schema};{jdbc_conf}'  # noqa

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
        'database': {
            'type': ['string'],
        },
        'schema': {
            'type': 'string',
        },
        'user': {
            'type': 'string'
        },
        'password': {
            'type': 'string'
        },
        'jdbc_driver': {
            'type': 'object',
            'properties': {
                'class': {
                    'type': 'string'
                },
                'path': {
                    'type': 'string'
                },
            },
            'required': ['class', 'path']
        },
        'jdbc_config': {
            'type': 'object'
        }
    },
    'required': ['host', 'port', 'user', 'password', 'jdbc_driver',
                 'jdbc_config', 'database', 'schema'],
}


class SparkCredentials(Credentials):
    SCHEMA = SPARK_CREDENTIALS_CONTRACT

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('database', kwargs.get('schema'))
        kwargs.setdefault('jdbc_config', {})

        super(SparkCredentials, self).__init__(*args, **kwargs)

    @property
    def type(self):
        return 'spark'

    def _connection_keys(self):
        return ('host', 'port', 'schema', 'user', 'jdbc_driver',
                'jdbc_config')


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
        # TODO?
        self.handle.close()

    def commit(self, *args, **kwargs):
        logger.debug("NotImplemented: commit")

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def start_transaction(self, *args, **kwargs):
        logger.debug("NotImplemented: start_transaction")

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
        # TODO: introspect into `DatabaseError`s and expose `errorName`,
        # `errorType`, etc instead of stack traces full of garbage!
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            raise dbt.exceptions.RuntimeException(exc)

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
    def _build_jdbc_url(cls, creds):
        jdbc_conf = ";".join(
            "{}={}".format(key, val)
            for (key, val) in creds.jdbc_config.items()
        )

        return JDBC_CONN_STRING.format(creds=creds, jdbc_conf=jdbc_conf)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        jdbc_url = cls._build_jdbc_url(credentials)
        auth = {
            "user": credentials.user,
            "password": credentials.password
        }

        conn = jaydebeapi.connect(
            credentials.jdbc_driver['class'],
            jdbc_url,
            auth,
            credentials.jdbc_driver['path']
        )

        wrapped = ConnectionWrapper(conn)

        connection.state = 'open'
        connection.handle = wrapped
        return connection

    @classmethod
    def get_status(cls, cursor):
        # No status from the cursor...
        return 'OK'

    def cancel(self, connection):
        import ipdb; ipdb.set_trace()
        connection.handle.cancel()
