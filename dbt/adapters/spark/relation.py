from typing import Optional, TypeVar, Any, Type, Dict
from dbt.contracts.graph.parsed import ParsedSourceDefinition
from dbt.utils import deep_merge
from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException
from dbt.events import AdapterLogger

logger = AdapterLogger("Spark")

Self = TypeVar("Self", bound="BaseRelation")
import importlib
import os
import sys
from datetime import datetime


@dataclass
class SparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class SparkIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SparkRelation(BaseRelation):
    quote_policy: SparkQuotePolicy = SparkQuotePolicy()
    include_policy: SparkIncludePolicy = SparkIncludePolicy()
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    is_iceberg: Optional[bool] = None
    information: Optional[str] = None
    loader: Optional[str] = None
    source_meta: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None

    # def __post_init__(self):
    #     if self.database != self.schema and self.database:
    #         raise RuntimeException("Cannot set database in spark!")

    # cccs: create a view from a dataframe
    def load_python_module(self, start_time, end_time, **kwargs):
        logger.debug(f"SparkRelation create source for {self.identifier}")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        if self.meta and self.meta.get('python_module'):
            path = f"{self.meta.get('python_module')}"
        elif self.source_meta and self.source_meta.get('python_module'):
            path = f"{self.source_meta.get('python_module')}"
        if path:
            logger.debug(f"SparkRelation attempting to load generic python module {path}")
            spec = importlib.util.find_spec(path)
            if not spec:
                raise RuntimeException(f"Cannot find python module {path}")

            python_file = spec.origin
            # file modification timestamp of a file
            mod_time = os.path.getmtime(python_file)
            # convert timestamp into DateTime object
            f_date = datetime.fromtimestamp(mod_time).strftime("%Y%m%d%H%M%S")
            s_date = start_time.strftime("%Y%m%d%H%M%S")
            e_date = end_time.strftime("%Y%m%d%H%M%S")
            view_name = f'{self.identifier}_{f_date}_{s_date}_{e_date}'
            if spark.catalog._jcatalog.tableExists(view_name):
                logger.debug(f"View {view_name} already exists")
            else:
                logger.debug(f"Creating view {view_name}")
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                create_dataframe = getattr(module, "create_dataframe")
                df = create_dataframe(
                    spark=spark,
                    table_name=self.identifier, 
                    start_time=start_time, 
                    end_time=end_time, 
                    **kwargs)
                df.createOrReplaceTempView(view_name)
            # Return a relation which only has a table name (spark view have no catalog or schema)
            return SparkRelation.create(database=None, schema=None, identifier=view_name)

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
            loader=source.loader,
            source_meta=source.source_meta,
            meta=source.meta,
            **kwargs,
        )

    def render(self):
        # if self.include_policy.database and self.include_policy.schema:
        #     raise RuntimeException(
        #         "Got a spark relation with schema and database set to "
        #         "include, but only one can be set"
        #     )
        return super().render()
