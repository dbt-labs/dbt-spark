from typing import Optional

from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException

from typing import Optional, TypeVar, Any, Type, Dict, Union, Iterator, Tuple, Set

Self = TypeVar("Self", bound="BaseRelation")
from dbt.contracts.graph.parsed import ParsedSourceDefinition, ParsedNode
from dbt.utils import filter_null_values, deep_merge, classproperty

import importlib


from datetime import timezone, datetime


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: SparkQuotePolicy = SparkQuotePolicy()
    include_policy: SparkIncludePolicy = SparkIncludePolicy()
    quote_character: str = '`'
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    information: str = None
    source_meta: Dict[str, Any] = None
    meta: Dict[str, Any] = None

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise RuntimeException('Cannot set database in spark!')

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                'Got a spark relation with schema and database set to '
                'include, but only one can be set'
            )
        return super().render()

    @classmethod
    def create_from_source(cls: Type[Self], source: ParsedSourceDefinition, **kwargs: Any) -> Self:
        source_quoting = source.quoting.to_dict(omit_none=True)
        source_quoting.pop("column", None)
        quote_policy = deep_merge(
            cls.get_default_quote_policy().to_dict(omit_none=True),
            source_quoting,
            kwargs.get("quote_policy", {}),
        )

        return cls.create(
            database=source.database,
            schema=source.schema,
            identifier=source.identifier,
            quote_policy=quote_policy,
            source_meta=source.source_meta,
            meta=source.meta,
            **kwargs,
        )

    def load_python_module(self, start_time, end_time):
        logger.debug(f"Creating pyspark view for {self.identifier}")
        from pyspark.sql import SparkSession
        spark = SparkSession._instantiatedSession
        if self.meta and self.meta.get('python_module'):
            path = self.meta.get('python_module')
            logger.debug(f"Loading python module {path}")
            module = importlib.import_module(path)
            create_dataframe = getattr(module, "create_dataframe")
            df = create_dataframe(spark, start_time, end_time)
            df.createOrReplaceTempView(self.identifier)
        elif self.source_meta and self.source_meta.get('python_module'):
            path = self.source_meta.get('python_module')
            logger.debug(f"Loading python module {path}")
            module = importlib.import_module(path)
            create_dataframe_for = getattr(module, "create_dataframe_for")
            df = create_dataframe_for(spark, self.identifier, start_time, end_time)
            df.createOrReplaceTempView(self.identifier)