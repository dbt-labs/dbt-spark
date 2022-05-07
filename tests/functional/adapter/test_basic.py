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


@pytest.mark.skip_profile('spark_session')
class TestSimpleMaterializationsSpark(BaseSimpleMaterializations):
    pass


class TestSingularTestsSpark(BaseSingularTests):
    pass


# The local cluster currently tests on spark 2.x, which does not support this
# if we upgrade it to 3.x, we can enable this test
@pytest.mark.skip_profile('apache_spark')
class TestSingularTestsEphemeralSpark(BaseSingularTestsEphemeral):
    pass


class TestEmptySpark(BaseEmpty):
    pass


@pytest.mark.skip_profile('spark_session')
class TestEphemeralSpark(BaseEphemeral):
    pass


@pytest.mark.skip_profile('spark_session')
class TestIncrementalSpark(BaseIncremental):
    pass


class TestGenericTestsSpark(BaseGenericTests):
    pass


# These tests were not enabled in the dbtspec files, so skipping here.
# Error encountered was: Error running query: java.lang.ClassNotFoundException: delta.DefaultSource
@pytest.mark.skip_profile('apache_spark', 'spark_session')
class TestSnapshotCheckColsSpark(BaseSnapshotCheckCols):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            }
        }


#hese tests were not enabled in the dbtspec files, so skipping here.
# Error encountered was: Error running query: java.lang.ClassNotFoundException: delta.DefaultSource
@pytest.mark.skip_profile('apache_spark', 'spark_session')
class TestSnapshotTimestampSpark(BaseSnapshotTimestamp):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            }
        }

class TestBaseAdapterMethod(BaseAdapterMethod):
    pass