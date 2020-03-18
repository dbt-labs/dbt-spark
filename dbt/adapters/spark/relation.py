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
                 table_stats: str,
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
        self.table_stats = {}
        if table_stats:
            # format: 1109049927 bytes, 14093476 rows
            stats = {
                stats.split(" ")[1]: int(stats.split(" ")[0])
                for stats in table_stats.split(', ')
            }
            for key, val in stats.items():
                self.table_stats[f'stats:{key}:label'] = key
                self.table_stats[f'stats:{key}:value'] = val
                self.table_stats[f'stats:{key}:description'] = ''
                self.table_stats[f'stats:{key}:include'] = True

    @property
    def quoted(self):
        return '`{}`'.format(self.column)

    def __repr__(self):
        return "<SparkColumn {}, {}>".format(self.name, self.data_type)

    def to_dict(self):
        original_dict = self.__dict__.copy()
        # If there are stats, merge them into the root of the dict
        if self.table_stats:
            original_dict.update(self.table_stats)
        del original_dict['table_stats']
        return original_dict
