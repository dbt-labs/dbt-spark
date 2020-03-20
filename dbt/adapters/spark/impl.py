from typing import Optional, List, Dict, Any

import agate
import dbt.exceptions
import dbt
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.manifest import Manifest

from dbt.adapters.spark import SparkConnectionManager
from dbt.adapters.spark import SparkRelation
from dbt.adapters.spark import SparkColumn


from dbt.adapters.base import BaseRelation

from dbt.logger import GLOBAL_LOGGER as logger

GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
DROP_RELATION_MACRO_NAME = 'drop_relation'

KEY_TABLE_OWNER = 'Owner'
KEY_TABLE_STATISTICS = 'Statistics'


class SparkAdapter(SQLAdapter):
    COLUMN_NAMES = (
        'table_database',
        'table_schema',
        'table_name',
        'table_type',
        'table_comment',
        'table_owner',
        'column_name',
        'column_index',
        'column_type',
        'column_comment',

        'stats:bytes:label',
        'stats:bytes:value',
        'stats:bytes:description',
        'stats:bytes:include',

        'stats:rows:label',
        'stats:rows:value',
        'stats:rows:description',
        'stats:rows:include',
    )

    Relation = SparkRelation
    Column = SparkColumn
    ConnectionManager = SparkConnectionManager

    AdapterSpecificConfigs = frozenset({"file_format", "location_root",
                                        "partition_by", "clustered_by",
                                        "buckets"})

    @classmethod
    def date_function(cls) -> str:
        return 'current_timestamp()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp"

    def add_schema_to_cache(self, schema) -> str:
        """Cache a new schema in dbt. It will show up in `list relations`."""
        if schema is None:
            name = self.nice_connection_name()
            dbt.exceptions.raise_compiler_error(
                'Attempted to cache a null schema for {}'.format(name)
            )
        if dbt.flags.USE_CACHE:
            self.cache.add_schema(None, schema)
        # so jinja doesn't render things
        return ''

    def list_relations_without_caching(
        self, information_schema, schema
    ) -> List[SparkRelation]:
        kwargs = {'information_schema': information_schema, 'schema': schema}
        try:
            results = self.execute_macro(
                LIST_RELATIONS_MACRO_NAME,
                kwargs=kwargs,
                release=True
            )
        except dbt.exceptions.RuntimeException as e:
            if hasattr(e, 'msg') and f"Database '{schema}' not found" in e.msg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema}: {e.msg}")
                return []

        relations = []
        for _schema, name, _, information in results:
            rel_type = ('view' if 'Type: VIEW' in information else 'table')
            relation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=rel_type
            )
            self.cache_added(relation)
            relations.append(relation)

        return relations

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)

    def parse_describe_extended(
            self,
            relation: Relation,
            raw_rows: List[agate.Row]
    ) -> List[SparkColumn]:
        # Convert the Row to a dict
        dict_rows = [dict(zip(row._keys, row._values)) for row in raw_rows]
        # Find the separator between the rows and the metadata provided
        # by the DESCRIBE TABLE EXTENDED statement
        pos = self.find_table_information_separator(dict_rows)

        # Remove rows that start with a hash, they are comments
        rows = [
            row for row in raw_rows[0:pos]
            if not row['col_name'].startswith('#')
        ]
        metadata = {
            col['col_name']: col['data_type'] for col in raw_rows[pos + 1:]
        }

        raw_table_stats = metadata.get(KEY_TABLE_STATISTICS)
        table_stats = SparkColumn.convert_table_stats(raw_table_stats)
        return [SparkColumn(
            table_database=relation.database,
            table_schema=relation.schema,
            table_name=relation.name,
            table_type=relation.type,
            table_owner=metadata.get(KEY_TABLE_OWNER),
            table_stats=table_stats,
            column=column['col_name'],
            column_index=idx,
            dtype=column['data_type'],
        ) for idx, column in enumerate(rows)]

    @staticmethod
    def find_table_information_separator(rows: List[dict]) -> int:
        pos = 0
        for row in rows:
            if not row['col_name'] or row['col_name'].startswith('#'):
                break
            pos += 1
        return pos

    def get_columns_in_relation(self, relation: Relation) -> List[SparkColumn]:
        rows: List[agate.Row] = super().get_columns_in_relation(relation)
        return self.parse_describe_extended(relation, rows)

    @staticmethod
    def _parse_relation(relation: Relation,
                        table_columns: List[Column],
                        rel_type: str,
                        properties: Dict[str, str] = None) -> List[dict]:
        properties = properties or {}
        statistics = {}
        table_owner_key = 'Owner'

        # First check if it is present in the properties
        table_owner = properties.get(table_owner_key)

        found_detailed_table_marker = False
        for column in table_columns:
            if column.name == '# Detailed Table Information':
                found_detailed_table_marker = True

            # In case there is another column with the name Owner
            if not found_detailed_table_marker:
                continue

            if not table_owner and column.name == table_owner_key:
                table_owner = column.data_type

            if column.name == 'Statistics':
                # format: 1109049927 bytes, 14093476 rows
                statistics = {stats.split(" ")[1]: int(stats.split(" ")[0]) for
                              stats in column.data_type.split(', ')}

        columns = []
        for column_index, column in enumerate(table_columns):
            # Fixes for pseudo-columns with no type
            if column.name in {
                '# Partition Information',
                '# col_name',
                ''
            }:
                continue
            elif column.name == '# Detailed Table Information':
                # Loop until the detailed table information
                break
            elif column.data_type is None:
                continue

            column_data = (
                relation.database,
                relation.schema,
                relation.name,
                rel_type,
                None,
                table_owner,
                column.name,
                column_index,
                column.data_type,
                None,

                # Table level stats
                'Table size',
                statistics.get("bytes"),
                "The size of the table in bytes",
                statistics.get("bytes") is not None,

                # Column level stats
                'Number of rows',
                statistics.get("rows"),
                "The number of rows in the table",
                statistics.get("rows") is not None
            )

            column_dict = dict(zip(SparkAdapter.COLUMN_NAMES, column_data))
            columns.append(column_dict)

        return columns

    def _massage_column_for_catalog(
        self, column: SparkColumn
    ) -> Dict[str, Any]:
        dct = column.to_dict()
        # different expectations here - Column.column is the name
        dct['column_name'] = dct.pop('column')
        dct['column_type'] = dct.pop('dtype')
        # table_database can't be None in core.
        if dct['table_database'] is None:
            dct['table_database'] = dct['table_schema']
        return dct

    def get_catalog(self, manifest: Manifest) -> agate.Table:
        schemas = manifest.get_used_schemas()
        columns = []
        for database, schema in schemas:
            relations = self.list_relations(database, schema)
            for relation in relations:
                logger.debug("Getting table schema for relation {}", relation)
                columns.extend(
                    self._massage_column_for_catalog(col)
                    for col in self.get_columns_in_relation(relation)
                )
        return agate.Table.from_object(columns)

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists
