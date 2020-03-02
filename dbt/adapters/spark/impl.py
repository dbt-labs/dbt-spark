from typing import Optional, List, Dict

import dbt
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.manifest import Manifest

from dbt.adapters.spark import SparkColumn
from dbt.adapters.spark import SparkConnectionManager
from dbt.adapters.spark.relation import SparkRelation
from dbt.adapters.base import BaseRelation, RelationType

from dbt.clients.agate_helper import table_from_data
from dbt.logger import GLOBAL_LOGGER as logger

import agate

LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
GET_RELATION_TYPE_MACRO_NAME = 'get_relation_type'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'
DROP_RELATION_MACRO_NAME = 'drop_relation'
FETCH_TBL_PROPERTIES_MACRO_NAME = 'fetch_tbl_properties'


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

    AdapterSpecificConfigs = frozenset({"file_format", "partition_by", "cluster_by", "num_buckets", "location"})

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

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)

    def get_relation_type(self, relation):
        kwargs = {'relation': relation}
        return self.execute_macro(
            GET_RELATION_TYPE_MACRO_NAME,
            kwargs=kwargs,
            release=True
        )

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

    def list_relations_without_caching(self, information_schema, schema) -> List[SparkRelation]:
        kwargs = {'information_schema': information_schema, 'schema': schema}

        results = self.execute_macro(
            LIST_RELATIONS_MACRO_NAME,
            kwargs=kwargs
        )

        relations = []
        for _schema, name, _ in results:
            relation = self.Relation.create(
                schema=_schema,
                identifier=name,
                #TODO: append relation type view/table
                ## type=RelationType.Table
            )
            self.cache_added(relation)
            relations.append(relation)

        return relations

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

    def get_properties(self, relation: Relation) -> Dict[str, str]:
        properties = self.execute_macro(
            FETCH_TBL_PROPERTIES_MACRO_NAME,
            kwargs={'relation': relation}
        )
        return {key: value for (key, value) in properties}

    def get_catalog(self, manifest: Manifest) -> agate.Table:
        schemas = manifest.get_used_schemas()

        columns = []
        for database, schema in schemas:
            relations = self.list_relations(database, schema)
            for relation in relations:
                properties = self.get_properties(relation)
                logger.debug("Getting table schema for relation {}".format(relation))  # noqa
                table_columns = self.get_columns_in_relation(relation)
                rel_type = self.get_relation_type(relation)
                columns += self._parse_relation(relation, table_columns, rel_type, properties)

        return table_from_data(columns, SparkAdapter.COLUMN_NAMES)

    # Override that doesn't check the type of the relation -- we do it
    # dynamically in the macro code
    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)

        self.execute_macro(
            DROP_RELATION_MACRO_NAME,
            kwargs={'relation': relation}
        )

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists


    def valid_snapshot_target(self, relation: BaseRelation) -> None:
        """Ensure that the target relation is valid, by making sure it has the
        expected columns.

        :param Relation relation: The relation to check
        :raises dbt.exceptions.CompilationException: If the columns are
            incorrect.
        """
        if not isinstance(relation, self.Relation):
            dbt.exceptions.invalid_type_error(
                method_name='valid_snapshot_target',
                arg_name='relation',
                got_value=relation,
                expected_type=self.Relation)

        columns = self.get_columns_in_relation(relation)
        names = set(c.name.lower() for c in columns if c.name)
        expanded_keys = ('scd_id', 'valid_from', 'valid_to')
        extra = []
        missing = []
        for legacy in expanded_keys:
            desired = 'dbt_' + legacy
            if desired not in names:
                missing.append(desired)
                if legacy in names:
                    extra.append(legacy)

        if missing:
            if extra:
                msg = (
                    'Snapshot target has ("{}") but not ("{}") - is it an '
                    'unmigrated previous version archive?'
                    .format('", "'.join(extra), '", "'.join(missing))
                )
            else:
                msg = (
                    'Snapshot target is not a snapshot table (missing "{}")'
                    .format('", "'.join(missing))
                )
            dbt.exceptions.raise_compiler_error(msg)