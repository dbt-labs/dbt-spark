from dbt.adapters.sql import SQLAdapter
from dbt.adapters.spark import SparkRelation
from dbt.adapters.spark import SparkConnectionManager
import dbt.exceptions

import agate


LIST_RELATIONS_MACRO_NAME = 'list_relations_without_caching'
GET_RELATION_TYPE_MACRO_NAME = 'spark_get_relation_type'
DROP_RELATION_MACRO_NAME = 'drop_relation'


class SparkAdapter(SQLAdapter):
    ConnectionManager = SparkConnectionManager
    Relation = SparkRelation

    @classmethod
    def date_function(cls):
        return 'CURRENT_TIMESTAMP()'

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "STRING"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "DOUBLE" if decimals else "BIGINT"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "TIMESTAMP"

    def create_schema(self, database, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            'Schema/Database creation is not supported in the Spark adapter. '
            'Please create the database "{}" manually'.format(database)
        )

    def drop_schema(self, database, schema, model_name=None):
        raise dbt.exceptions.NotImplementedException(
            'Schema/Database deletion is not supported in the Spark adapter. '
            'Please drop the database "{}" manually'.format(database)
        )

    def get_relation_type(self, relation, model_name=None):
        kwargs = {'relation': relation}
        return self.execute_macro(
            GET_RELATION_TYPE_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name,
            release=True
        )

    # Override that creates macros without a known type - adapter macros that
    # require a type will dynamically check at query-time
    def list_relations_without_caching(self, information_schema, schema,
                                       model_name=None):
        kwargs = {'information_schema': information_schema, 'schema': schema}
        results = self.execute_macro(
            LIST_RELATIONS_MACRO_NAME,
            kwargs=kwargs,
            connection_name=model_name,
            release=True
        )

        relations = []
        quote_policy = {
            'schema': True,
            'identifier': True
        }
        for _database, name, _ in results:
            relations.append(self.Relation.create(
                database=_database,
                schema=_database,
                identifier=name,
                quote_policy=quote_policy,
                type=None
            ))
        return relations

    # Override that doesn't check the type of the relation -- we do it 
    # dynamically in the macro code
    def drop_relation(self, relation, model_name=None):
        if dbt.flags.USE_CACHE:
            self.cache.drop(relation)

        self.execute_macro(
            DROP_RELATION_MACRO_NAME,
            kwargs={'relation': relation},
            connection_name=model_name
        )
