"""Spark session integration."""

from __future__ import annotations

import datetime as dt
from types import TracebackType
from typing import Any, List, Optional, Tuple

from dbt.events import AdapterLogger
from dbt.utils import DECIMALS
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark import SparkConf
import importlib


logger = AdapterLogger("Spark")
NUMBERS = DECIMALS + (int, float)


class Cursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self, spark_session) -> None:
        self._df: Optional[DataFrame] = None
        self._rows: Optional[List[Row]] = None
        self._spark_session = spark_session

    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self,
        exc_type: Optional[BaseException],
        exc_val: Optional[Exception],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.close()
        return True

    @property
    def description(
        self,
    ) -> List[Tuple[str, str, None, None, None, None, bool]]:
        """
        Get the description.

        Returns
        -------
        out : List[Tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
        if self._df is None:
            description = list()
        else:
            description = [
                (
                    field.name,
                    field.dataType.simpleString(),
                    None,
                    None,
                    None,
                    None,
                    field.nullable,
                )
                for field in self._df.schema.fields
            ]
        return description

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        self._df = None
        self._rows = None

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            sql = sql % parameters
        self._df = self._spark_session.sql(sql)

    def fetchall(self) -> Optional[List[Row]]:
        """
        Fetch all data.

        Returns
        -------
        out : Optional[List[Row]]
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()
        return self._rows

    def fetchone(self) -> Optional[Row]:
        """
        Fetch the first output.

        Returns
        -------
        out : Row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()

        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row


class Connection:
    """
    Mock a pyodbc connection.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Connection
    """
    def __init__(self, spark_configuration=None, python_module=None) -> None:
        self._spark_configuration = spark_configuration
        self._python_module = python_module
        self.build_spark_session()

    def build_spark_session(self):
        spark_conf = SparkConf()
        if self._spark_configuration:
            spark_conf.setAll([(k, v) for k, v in self._spark_configuration.items()])

        if self._python_module:
            path = self._python_module
            logger.debug(f"Attempting to load spark context from {path}")
            module = importlib.import_module(path)
            create_spark_context = getattr(module, "create_spark_context")
            self._spark_session = create_spark_context(spark_conf)
        else:
            self._spark_session = SparkSession.builder.enableHiveSupport().config(conf=spark_conf).getOrCreate()

    def cursor(self) -> Cursor:
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return Cursor(self._spark_session)


class SessionConnectionWrapper(object):
    """Connection wrapper for the sessoin connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        logger.debug("NotImplemented: cancel")

    def close(self):
        if self._cursor:
            self._cursor.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        else:
            return f"'{value}'"
