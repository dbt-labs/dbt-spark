from dataclasses import dataclass
from typing import TypeVar

from dbt.adapters.base.column import Column

Self = TypeVar('Self', bound='SparkColumn')


@dataclass(init=False)
class SparkColumn(Column):
    column: str
    dtype: str
    comment: str

    def __init__(
        self,
        column: str,
        dtype: str,
        comment: str = None
    ) -> None:
        super().__init__(column, dtype)

        self.comment = comment

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return dtype

    def can_expand_to(self: Self, other_column: Self) -> bool:
        """returns True if both columns are strings"""
        return self.is_string() and other_column.is_string()

    def literal(self, value):
        return "cast({} as {})".format(value, self.dtype)

    @property
    def data_type(self) -> str:
        return self.dtype

    def __repr__(self) -> str:
        return "<SparkColumn {} ({})>".format(self.name, self.data_type)
