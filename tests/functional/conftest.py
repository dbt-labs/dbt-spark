import time
import pytest


def _wait_for_databricks_cluster(project):
    """
    It takes roughly 3min for the cluster to start, to be safe we'll wait for 5min
    """
    for _ in range(60):
        try:
            project.run_sql("SELECT 1", fetch=True)
            return
        except Exception:
            time.sleep(10)

    raise Exception("Databricks cluster did not start in time")


# Running this should prevent tests from needing to be retried because the Databricks cluster isn't available
@pytest.fixture(scope="class", autouse=True)
def start_databricks_cluster(project, request):
    profile_type = request.config.getoption("--profile")

    if "databricks" in profile_type:
        _wait_for_databricks_cluster(project)

    yield 1
