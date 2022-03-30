
from __future__ import annotations

import datetime as dt
from types import TracebackType
from typing import Any

from dbt.events import AdapterLogger
from dbt.utils import DECIMALS


from pyspark.rdd import _load_from_socket
import pyspark.sql.functions as F


import importlib
import sqlalchemy
import re

logger = AdapterLogger("Spark")
NUMBERS = DECIMALS + (int, float)


class PysparkConnectionWrapper(object):
    """Wrap a Spark context"""

    def __init__(self, python_module):
        self.result = None
        if python_module:
            logger.debug(f"Loading spark context from python module {python_module}")
            module = importlib.import_module(python_module)
            create_spark_context = getattr(module, "create_spark_context")
            self.spark = create_spark_context()
        else:
            # Create a default pyspark context
            self.spark = SparkSession.builder.getOrCreate()

    def cursor(self):
        return self

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        try:
            rows = self.result.collect()
            logger.debug(rows)
        except Exception as e:
            logger.debug(f"raising error {e}")
            dbt.exceptions.raise_database_error(e)
        return rows

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]
            sql = sql % tuple(bindings)
        logger.debug(f"execute sql:{sql}")
        try:
            self.result = self.spark.sql(sql)
            logger.debug("Executed with no errors")
            if "show tables" in sql:
                self.result = self.result.withColumn("description", F.lit(""))
        except Exception as e:
            logger.debug(f"raising error {e}")
            dbt.exceptions.raise_database_error(e)

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
           the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return "'" + value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "'"
        elif isinstance(value, str):
            return "'" + value + "'"
        else:
            logger.debug(type(value))
            return "'" + str(value) + "'"

    @property
    def description(self):
        logger.debug(f"Description called returning list of columns: {self.result.columns}")
        ret = []
        # Not sure the type is ever used by specifying it anyways
        string_type = sqlalchemy.types.String
        for column_name in self.result.columns:
            ret.append((column_name, string_type))
        return ret

