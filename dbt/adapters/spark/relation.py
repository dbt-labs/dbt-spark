from dbt.adapters.base.relation import BaseRelation, Column


class SparkRelation(BaseRelation):
    DEFAULTS = {
        'metadata': {
            'type': 'SparkRelation'
        },
        'quote_character': '`',
        'quote_policy': {
            'database': False,
            'schema': False,
            'identifier': False,
        },
        'include_policy': {
            'database': False,
            'schema': True,
            'identifier': True,
        },
        'dbt_created': False,

    }

    SCHEMA = {
        'type': 'object',
        'properties': {
            'metadata': {
                'type': 'object',
                'properties': {
                    'type': {
                        'type': 'string',
                        'const': 'SparkRelation',
                    },
                },
            },
            'type': {
                'enum': BaseRelation.RelationTypes + [None]
            },
            'path': BaseRelation.PATH_SCHEMA,
            'include_policy': BaseRelation.POLICY_SCHEMA,
            'quote_policy': BaseRelation.POLICY_SCHEMA,
            'quote_character': {'type': 'string'},
            'dbt_created': {'type': 'boolean'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character', 'dbt_created']
    }


class SparkColumn(Column):

    def __init__(self,
                 table_database: str,
                 table_schema: str,
                 table_name: str,
                 table_type: str,
                 table_owner: str,
                 column_name: str,
                 column_index: int,
                 column_type: str):
        super(SparkColumn, self).__init__(column_name, column_type)
        self.table_database = table_database
        self.table_schema = table_schema
        self.table_name = table_name
        self.table_type = table_type
        self.table_owner = table_owner
        self.column_name = column_name
        self.column_index = column_index

    @property
    def quoted(self):
        return '`{}`'.format(self.column)
