from concurrent.futures import Future
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union, Iterable
import agate
from dbt.contracts.relation import RelationType

import dbt
import dbt.exceptions

from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.spark import SparkConnectionManager
from dbt.adapters.spark import SparkRelation
from dbt.adapters.spark import SparkColumn
from dbt.adapters.base import BaseRelation
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import executor

GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
DROP_RELATION_MACRO_NAME = 'drop_relation'
FETCH_TBL_PROPERTIES_MACRO_NAME = 'fetch_tbl_properties'

KEY_TABLE_OWNER = 'Owner'
KEY_TABLE_STATISTICS = 'Statistics'


@dataclass
class SparkConfig(AdapterConfig):
    file_format: str = 'parquet'
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None


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
    AdapterSpecificConfigs = SparkConfig

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

    def quote(self, identifier):
        return '`{}`'.format(identifier)

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
        self, schema_relation: SparkRelation
    ) -> List[SparkRelation]:
        kwargs = {'schema_relation': schema_relation}
        try:
            results = self.execute_macro(
                LIST_RELATIONS_MACRO_NAME,
                kwargs=kwargs
            )
        except dbt.exceptions.RuntimeException as e:
            errmsg = getattr(e, 'msg', '')
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for row in results:
            if len(row) != 4:
                raise dbt.exceptions.RuntimeException(
                    f'Invalid value from "show table extended ...", '
                    f'got {len(row)} values, expected 4'
                )
            _schema, name, _, information = row
            rel_type = RelationType.View \
                if 'Type: VIEW' in information else RelationType.Table
            is_delta = 'Provider: delta' in information
            relation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=rel_type,
                is_delta=is_delta
            )
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
            table_database=None,
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

    def _get_columns_for_catalog(
        self, relation: SparkRelation
    ) -> Iterable[Dict[str, Any]]:
        properties = self.get_properties(relation)
        columns = self.get_columns_in_relation(relation)
        owner = properties.get(KEY_TABLE_OWNER)

        for column in columns:
            if owner:
                column.table_owner = owner
            # convert SparkColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict['column_name'] = as_dict.pop('column', None)
            as_dict['column_type'] = as_dict.pop('dtype')
            as_dict['table_database'] = None
            yield as_dict

    def get_properties(self, relation: Relation) -> Dict[str, str]:
        properties = self.execute_macro(
            FETCH_TBL_PROPERTIES_MACRO_NAME,
            kwargs={'relation': relation}
        )
        return dict(properties)

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one database in get_catalog, found '
                f'{list(schema_map)}'
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(tpe.submit_connected(
                        self, schema,
                        self._get_one_catalog, info, [schema], manifest
                    ))
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self, information_schema, schemas, manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            dbt.exceptions.raise_compiler_error(
                f'Expected only one schema in spark _get_one_catalog, found '
                f'{schemas}'
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", relation)
            columns.extend(self._get_columns_for_catalog(relation))
        return agate.Table.from_object(
            columns, column_types=DEFAULT_TYPE_TESTER
        )

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = 'EXCEPT',
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ', '.join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql


# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = '''
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
cross join diff_count
'''.strip()
