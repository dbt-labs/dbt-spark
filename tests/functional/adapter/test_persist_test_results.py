from typing import Dict

import pytest

from dbt.tests.adapter.persist_test_results.basic import PersistTestResults


@pytest.mark.skip_profile("spark_session", "apache_spark")
class TestPersistTestResultsDatabricks(PersistTestResults):
    pass


@pytest.mark.skip("Spark handles delete differently and this is not yet accounted for.")
@pytest.mark.skip_profile("spark_session", "databricks_cluster", "databricks_sql_endpoint")
class TestPersistTestResultsSpark(PersistTestResults):
    def delete_record(self, project, record: Dict[str, str]):
        """
        Using "DELETE FROM" with Spark throws the following error:
        dbt.exceptions.DbtDatabaseError: Database Error
            org.apache.hive.service.cli.HiveSQLException:
                Error running query: org.apache.spark.sql.AnalysisException:
                    DELETE is only supported with v2 tables.
        """
        pass
