from dataclasses import dataclass
from typing import TypeVar, Optional

from dbt.adapters.base.column import Column

Self = TypeVar('Self', bound='SparkColumn')


@dataclass
class SparkColumn(Column):
    table_database: Optional[str] = None
    table_schema: Optional[str] = None
    table_name: Optional[str] = None
    table_type: Optional[str] = None
    table_owner: Optional[str] = None
    table_stats: Optional[str] = None
    column_index: Optional[int] = None

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return dtype

    def can_expand_to(self: Self, other_column: Self) -> bool:
        """returns True if both columns are strings"""
        return self.is_string() and other_column.is_string()

    def literal(self, value):
        return "cast({} as {})".format(value, self.dtype)

    @property
    def quoted(self) -> str:
        return '`{}`'.format(self.column)

    @property
    def data_type(self) -> str:
        return self.dtype

    def __repr__(self) -> str:
        return "<SparkColumn {} ({})>".format(self.name, self.data_type)
