from dataclasses import dataclass
from typing import TypeVar, Optional, Dict, Any

from dbt.adapters.base.column import Column

Self = TypeVar('Self', bound='SparkColumn')


@dataclass
class SparkColumn(Column):
    table_database: Optional[str] = None
    table_schema: Optional[str] = None
    table_name: Optional[str] = None
    table_type: Optional[str] = None
    table_owner: Optional[str] = None
    table_stats: Optional[Dict[str, Any]] = None
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

    @staticmethod
    def convert_table_stats(raw_stats: Optional[str]) -> Dict[str, Any]:
        table_stats = {}
        if raw_stats:
            # format: 1109049927 bytes, 14093476 rows
            stats = {
                stats.split(" ")[1]: int(stats.split(" ")[0])
                for stats in raw_stats.split(', ')
            }
            for key, val in stats.items():
                table_stats[f'stats:{key}:label'] = key
                table_stats[f'stats:{key}:value'] = val
                table_stats[f'stats:{key}:description'] = ''
                table_stats[f'stats:{key}:include'] = True
        return table_stats

    def to_dict(self, omit_none=False):
        original_dict = super().to_dict(omit_none=omit_none)
        # If there are stats, merge them into the root of the dict
        original_stats = original_dict.pop('table_stats')
        if original_stats:
            original_dict.update(original_stats)
        return original_dict
