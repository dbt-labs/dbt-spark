from dbt.adapters.base import BaseRelation
from dbt.adapters.cache import RelationsCache
from dbt.exceptions import MissingRelationError
from dbt.utils import lowercase


class SparkRelationsCache(RelationsCache):
    def get_relation_from_stub(self, relation_stub: BaseRelation) -> BaseRelation:
        """
        Case-insensitively yield all relations matching the given schema.

        :param BaseRelation relation_stub: The relation to look for
        :return BaseRelation: The cached version of the relation
        """
        with self.lock:
            results = [
                relation.inner
                for relation in self.relations.values()
                if all(
                    {
                        lowercase(relation.database) == lowercase(relation_stub.database),
                        lowercase(relation.schema) == lowercase(relation_stub.schema),
                        lowercase(relation.identifier) == lowercase(relation_stub.identifier),
                    }
                )
            ]
        if len(results) == 0:
            raise MissingRelationError(relation_stub)
        return results[0]
