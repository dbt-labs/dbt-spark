from typing import Optional, TypeVar, Any, Type, Dict
from dbt.contracts.graph.nodes import SourceDefinition
from dbt.utils import deep_merge
from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException
from dbt.events import AdapterLogger

logger = AdapterLogger("Spark")

Self = TypeVar("Self", bound="BaseRelation")


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
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    is_hudi: Optional[bool] = None
    is_iceberg: Optional[bool] = None
    information: Optional[str] = None
    loader: Optional[str] = None
    source_meta: Optional[Dict[str, Any]] = None
    meta: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise RuntimeException("Cannot set database in spark!")

    @classmethod
    def create_from_source(cls: Type[Self], source: SourceDefinition, **kwargs: Any) -> Self:
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
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                "Got a spark relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()
