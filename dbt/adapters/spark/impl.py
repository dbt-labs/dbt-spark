from typing import Optional, List

from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.manifest import Manifest

from dbt.adapters.spark import SparkColumn
from dbt.adapters.spark import SparkConnectionManager
from dbt.adapters.spark.relation import SparkRelation
from dbt.adapters.base import RelationType, BaseRelation

from dbt.clients.agate_helper import table_from_data
from dbt.logger import GLOBAL_LOGGER as logger

import agate

LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
LIST_EXTENDED_PROPERTIES_MACRO_NAME = 'list_extended_properties'
GET_COLUMNS_IN_RELATION_MACRO_NAME = 'get_columns_in_relation'
LIST_SCHEMAS_MACRO_NAME = 'list_schemas'


class SparkAdapter(SQLAdapter):

    RELATION_TYPES = {
        'MANAGED': RelationType.Table,
        'VIEW': RelationType.View,
        'EXTERNAL': RelationType.External
    }

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

    def list_extended_properties(self, schema, identifier):
        results = self.execute_macro(
            LIST_EXTENDED_PROPERTIES_MACRO_NAME,
            kwargs={'schema': schema, 'identifier': identifier}
        )

        detail_idx = 0
        for idx, row in enumerate(results.rows):
            if row['col_name'] == '# Detailed Table Information':
                detail_idx = idx
                continue

        detail_idx += 1
        return results[detail_idx:] if detail_idx != 0 else results

    def list_relations_without_caching(self, information_schema, schema):
        kwargs = {'information_schema': information_schema, 'schema': schema}

        results = self.execute_macro(
            LIST_RELATIONS_MACRO_NAME,
            kwargs=kwargs
        )

        relations = []
        for _schema, name, _ in results:
            # get extended properties foreach table
            details = self.list_extended_properties(_schema, name)

            _type = None
            for col_name, data_type, _ in details:
                if col_name == 'Type':
                    _type = self.RELATION_TYPES.get(data_type, None)
                    continue

            relations.append(self.Relation.create(
                schema=_schema,
                identifier=name,
                type=_type
            ))

        return relations

    def get_catalog(self, manifest: Manifest) -> agate.Table:
        schemas = manifest.get_used_schemas()

        column_names = (
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
        )

        columns = []
        for database, schema in schemas:
            relations = self.list_relations(database, schema)
            for relation in relations:
                table_columns = self.get_columns_in_relation(relation)

                for column_index, column in enumerate(table_columns):
                    # Fixes for pseudo-columns with no type
                    if column.name in (
                        '# Partition Information',
                        '# col_name'
                    ):
                        continue
                    elif column.data_type is None:
                        continue

                    column_data = (
                        relation.database,
                        relation.schema,
                        relation.name,
                        relation.type,
                        None,
                        None,
                        column.name,
                        column_index,
                        column.data_type,
                        None,
                    )
                    column_dict = dict(zip(column_names, column_data))
                    columns.append(column_dict)

        return table_from_data(columns, column_names)

    def get_columns_in_relation(self, relation) -> List[SparkColumn]:
        table = self.execute_macro(
            GET_COLUMNS_IN_RELATION_MACRO_NAME,
            kwargs={'relation': relation}
        )

        columns = []
        for col in table:
            # Fixes for pseudo-columns with no type
            if col.name in (
                    '# Partition Information',
                    '# col_name'
            ):
                continue
            elif col.data_type is None:
                continue

            column = self.Column(col.name, col.data_type, col.comment)
            columns.append(column)

        return columns

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            LIST_SCHEMAS_MACRO_NAME,
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists
