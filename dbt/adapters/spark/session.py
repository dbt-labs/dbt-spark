"""Spark session integration."""

from __future__ import annotations

import datetime as dt
from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Union, Sequence

from dbt.adapters.spark.connections import SparkConnectionWrapper
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.utils.encoding import DECIMALS
from dbt_common.exceptions import DbtRuntimeError
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.utils import AnalysisException


logger = AdapterLogger("Spark")
NUMBERS = DECIMALS + (int, float)


class Cursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self, *, server_side_parameters: Optional[Dict[str, Any]] = None) -> None:
        self._df: Optional[DataFrame] = None
        self._rows: Optional[List[Row]] = None
        self.server_side_parameters = server_side_parameters or {}

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
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        """
        Get the description.

        Returns
        -------
        out : Sequence[Tuple[str, str, None, None, None, None, bool]]
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

        builder = SparkSession.builder.enableHiveSupport()

        for parameter, value in self.server_side_parameters.items():
            builder = builder.config(parameter, value)

        spark_session = builder.getOrCreate()

        try:
            self._df = spark_session.sql(sql)
        except AnalysisException as exc:
            raise DbtRuntimeError(str(exc)) from exc

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
            self._rows = self._df.take(1)

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

    def __init__(self, *, server_side_parameters: Optional[Dict[Any, str]] = None) -> None:
        self.server_side_parameters = server_side_parameters or {}

    def cursor(self) -> Cursor:
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return Cursor(server_side_parameters=self.server_side_parameters)


class SessionConnectionWrapper(SparkConnectionWrapper):
    """Connection wrapper for the session connection method."""

    handle: Connection
    _cursor: Optional[Cursor]

    def __init__(self, handle: Connection) -> None:
        self.handle = handle
        self._cursor = None

    def cursor(self) -> "SessionConnectionWrapper":
        self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        logger.debug("NotImplemented: cancel")

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> Optional[List[Row]]:
        assert self._cursor, "Cursor not available"
        return self._cursor.fetchall()

    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        assert self._cursor, "Cursor not available"
        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        assert self._cursor, "Cursor not available"
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value: Any) -> Union[str, float]:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        else:
            return f"'{value}'"
