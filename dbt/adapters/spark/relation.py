from dbt.adapters.base.relation import BaseRelation


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
        }
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
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'quote_character']
    }
