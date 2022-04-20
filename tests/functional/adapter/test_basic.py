import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from dbt.tests.util import AnyFloat


@pytest.mark.skip_profile("spark_session")
class TestSimpleMaterializationsSpark(BaseSimpleMaterializations):
    pass


class TestSingularTestsSpark(BaseSingularTests):
    pass


# The local cluster currently tests on spark 2.x, which does not support this
# if we upgrade it to 3.x, we can enable this test
@pytest.mark.skip_profile("apache_spark")
class TestSingularTestsEphemeralSpark(BaseSingularTestsEphemeral):
    pass


class TestEmptySpark(BaseEmpty):
    pass


@pytest.mark.skip_profile("spark_session")
class TestEphemeralSpark(BaseEphemeral):
    pass


@pytest.mark.skip_profile("spark_session")
class TestIncrementalSpark(BaseIncremental):
    pass


class TestGenericTestsSpark(BaseGenericTests):
    pass


# These tests were not enabled in the dbtspec files, so skipping here.
# Error encountered was: Error running query: java.lang.ClassNotFoundException: delta.DefaultSource
@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotCheckColsSpark(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            },
        }


# hese tests were not enabled in the dbtspec files, so skipping here.
# Error encountered was: Error running query: java.lang.ClassNotFoundException: delta.DefaultSource
@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotTimestampSpark(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            },
        }


class TestBaseAdapterMethod(BaseAdapterMethod):
    pass


def spark_seed_stats():
    return {
        "bytes": {
            "description": None,
            "id": "bytes",
            "include": True,
            "label": "bytes",
            "value": AnyFloat(),
        },
        "has_stats": {
            "description": "Indicates whether there are statistics for this " "table",
            "id": "has_stats",
            "include": False,
            "label": "Has Stats?",
            "value": True,
        },
    }


class TestDocsGenerateSpark(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role="root",
            id_type="long",
            text_type="string",
            time_type="timestamp",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
            seed_stats=spark_seed_stats(),
        )
