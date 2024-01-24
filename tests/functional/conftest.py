from multiprocessing import Lock

import pytest

_db_start_lock = Lock()
_DB_CLUSTER_STARTED = False


@pytest.fixture(scope="class", autouse=True)
def start_databricks_cluster(project, request):
    global _DB_CLUSTER_STARTED
    profile_type = request.config.getoption("--profile")
    with _db_start_lock:
        if "databricks" in profile_type and not _DB_CLUSTER_STARTED:
            print("Starting Databricks cluster")
            project.run_sql("SELECT 1")

            _DB_CLUSTER_STARTED = True
