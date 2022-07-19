import pytest
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestModelGrantsSpark(BaseModelGrants):
    def privilege_grantee_name_overrides(self):
        # insert --> modify
        return {
            "select": "select",
            "insert": "modify",
            "fake_privilege": "fake_privilege",
            "invalid_user": "invalid_user",
        }


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestIncrementalGrantsSpark(BaseIncrementalGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
            }
        }


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSeedGrantsSpark(BaseSeedGrants):
    # seeds in dbt-spark are currently "full refreshed," in such a way that
    # the grants are not carried over
    # see https://github.com/dbt-labs/dbt-spark/issues/388
    def seeds_support_partial_refresh(self):
        return False


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotGrantsSpark(BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "snapshots": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
            }
        }


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestInvalidGrantsSpark(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "RESOURCE_DOES_NOT_EXIST"
        
    def privilege_does_not_exist_error(self):
        return "Action Unknown"
