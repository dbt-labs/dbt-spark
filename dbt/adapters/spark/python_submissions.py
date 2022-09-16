import base64
import time
import requests
from typing import Any, Dict
import uuid

import dbt.exceptions
from dbt.adapters.base import PythonJobHelper
from dbt.adapters.spark import SparkCredentials

DEFAULT_POLLING_INTERVAL = 5
SUBMISSION_LANGUAGE = "python"
DEFAULT_TIMEOUT = 60 * 60 * 24


class BaseDatabricksHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: SparkCredentials) -> None:
        self.check_credentials(credentials)
        self.credentials = credentials
        self.identifier = parsed_model["alias"]
        self.schema = getattr(parsed_model, "schema", self.credentials.schema)
        self.parsed_model = parsed_model
        self.timeout = self.get_timeout()
        self.polling_interval = DEFAULT_POLLING_INTERVAL

    def get_timeout(self) -> int:
        timeout = self.parsed_model["config"].get("timeout", DEFAULT_TIMEOUT)
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        return timeout

    def check_credentials(self, credentials: SparkCredentials) -> None:
        raise NotImplementedError(
            "Overwrite this method to check specific requirement for current submission method"
        )

    def submit(self, compiled_code: str) -> None:
        raise NotImplementedError(
            "BasePythonJobHelper is an abstract class and you should implement submit method."
        )

    def polling(
        self,
        status_func,
        status_func_kwargs,
        get_state_func,
        terminal_states,
        expected_end_state,
        get_state_msg_func,
    ) -> Dict:
        state = None
        start = time.time()
        exceeded_timeout = False
        response = {}
        while state not in terminal_states:
            if time.time() - start > self.timeout:
                exceeded_timeout = True
                break
            # should we do exponential backoff?
            time.sleep(self.polling_interval)
            response = status_func(**status_func_kwargs)
            state = get_state_func(response)
        if exceeded_timeout:
            raise dbt.exceptions.RuntimeException("python model run timed out")
        if state != expected_end_state:
            raise dbt.exceptions.RuntimeException(
                "python model run ended in state"
                f"{state} with state_message\n{get_state_msg_func(response)}"
            )
        return response


class DBNotebookPythonJobHelper(BaseDatabricksHelper):
    def __init__(self, parsed_model: Dict, credentials: SparkCredentials) -> None:
        super().__init__(parsed_model, credentials)
        self.auth_header = {"Authorization": f"Bearer {self.credentials.token}"}

    def check_credentials(self, credentials) -> None:
        if not credentials.user:
            raise ValueError("Databricks user is required for notebook submission method.")

    def _create_work_dir(self, path: str) -> None:
        response = requests.post(
            f"https://{self.credentials.host}/api/2.0/workspace/mkdirs",
            headers=self.auth_header,
            json={
                "path": path,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.RuntimeException(
                f"Error creating work_dir for python notebooks\n {response.content!r}"
            )

    def _upload_notebook(self, path: str, compiled_code: str) -> None:
        b64_encoded_content = base64.b64encode(compiled_code.encode()).decode()
        response = requests.post(
            f"https://{self.credentials.host}/api/2.0/workspace/import",
            headers=self.auth_header,
            json={
                "path": path,
                "content": b64_encoded_content,
                "language": "PYTHON",
                "overwrite": True,
                "format": "SOURCE",
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.RuntimeException(
                f"Error creating python notebook.\n {response.content!r}"
            )

    def _submit_notebook(self, path: str) -> str:
        submit_response = requests.post(
            f"https://{self.credentials.host}/api/2.1/jobs/runs/submit",
            headers=self.auth_header,
            json={
                "run_name": f"{self.schema}-{self.identifier}-{uuid.uuid4()}",
                "existing_cluster_id": self.credentials.cluster,
                "notebook_task": {
                    "notebook_path": path,
                },
            },
        )
        if submit_response.status_code != 200:
            raise dbt.exceptions.RuntimeException(
                f"Error creating python run.\n {submit_response.content!r}"
            )
        return submit_response.json()["run_id"]

    def submit(self, compiled_code: str) -> None:
        # it is safe to call mkdirs even if dir already exists and have content inside
        work_dir = f"/Users/{self.credentials.user}/{self.schema}/"
        self._create_work_dir(work_dir)

        # add notebook
        whole_file_path = f"{work_dir}{self.identifier}"
        self._upload_notebook(whole_file_path, compiled_code)

        # submit job
        run_id = self._submit_notebook(whole_file_path)

        self.polling(
            status_func=requests.get,
            status_func_kwargs={
                "url": f"https://{self.credentials.host}/api/2.1/jobs/runs/get?run_id={run_id}",
                "headers": self.auth_header,
            },
            get_state_func=lambda response: response.json()["state"]["life_cycle_state"],
            terminal_states=("TERMINATED", "SKIPPED", "INTERNAL_ERROR"),
            expected_end_state="TERMINATED",
            get_state_msg_func=lambda response: response.json()["state"]["state_message"],
        )

        # get end state to return to user
        run_output = requests.get(
            f"https://{self.credentials.host}" f"/api/2.1/jobs/runs/get-output?run_id={run_id}",
            headers=self.auth_header,
        )
        json_run_output = run_output.json()
        result_state = json_run_output["metadata"]["state"]["result_state"]
        if result_state != "SUCCESS":
            raise dbt.exceptions.RuntimeException(
                "Python model failed with traceback as:\n"
                "(Note that the line number here does not "
                "match the line number in your code due to dbt templating)\n"
                f"{json_run_output['error_trace']}"
            )


class DBContext:
    def __init__(self, credentials: SparkCredentials) -> None:
        self.auth_header = {"Authorization": f"Bearer {credentials.token}"}
        self.cluster = credentials.cluster
        self.host = credentials.host

    def create(self) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#create-an-execution-context
        response = requests.post(
            f"https://{self.host}/api/1.2/contexts/create",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster,
                "language": SUBMISSION_LANGUAGE,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.RuntimeException(
                f"Error creating an execution context.\n {response.content!r}"
            )
        return response.json()["id"]

    def destroy(self, context_id: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#delete-an-execution-context
        response = requests.post(
            f"https://{self.host}/api/1.2/contexts/destroy",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster,
                "contextId": context_id,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.RuntimeException(
                f"Error deleting an execution context.\n {response.content!r}"
            )
        return response.json()["id"]


class DBCommand:
    def __init__(self, credentials: SparkCredentials) -> None:
        self.auth_header = {"Authorization": f"Bearer {credentials.token}"}
        self.cluster = credentials.cluster
        self.host = credentials.host

    def execute(self, context_id: str, command: str) -> str:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#run-a-command
        response = requests.post(
            f"https://{self.host}/api/1.2/commands/execute",
            headers=self.auth_header,
            json={
                "clusterId": self.cluster,
                "contextId": context_id,
                "language": SUBMISSION_LANGUAGE,
                "command": command,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.RuntimeException(
                f"Error creating a command.\n {response.content!r}"
            )
        return response.json()["id"]

    def status(self, context_id: str, command_id: str) -> Dict[str, Any]:
        # https://docs.databricks.com/dev-tools/api/1.2/index.html#get-information-about-a-command
        response = requests.get(
            f"https://{self.host}/api/1.2/commands/status",
            headers=self.auth_header,
            params={
                "clusterId": self.cluster,
                "contextId": context_id,
                "commandId": command_id,
            },
        )
        if response.status_code != 200:
            raise dbt.exceptions.RuntimeException(
                f"Error getting status of command.\n {response.content!r}"
            )
        return response.json()


class DBCommandsApiPythonJobHelper(BaseDatabricksHelper):
    def check_credentials(self, credentials: SparkCredentials) -> None:
        if not credentials.cluster:
            raise ValueError("Databricks cluster is required for commands submission method.")

    def submit(self, compiled_code: str) -> None:
        context = DBContext(self.credentials)
        command = DBCommand(self.credentials)
        context_id = context.create()
        try:
            command_id = command.execute(context_id, compiled_code)
            # poll until job finish
            response = self.polling(
                status_func=command.status,
                status_func_kwargs={
                    "context_id": context_id,
                    "command_id": command_id,
                },
                get_state_func=lambda response: response["status"],
                terminal_states=("Cancelled", "Error", "Finished"),
                expected_end_state="Finished",
                get_state_msg_func=lambda response: response.json()["results"]["data"],
            )
            if response["results"]["resultType"] == "error":
                raise dbt.exceptions.RuntimeException(
                    f"Python model failed with traceback as:\n" f"{response['results']['cause']}"
                )
        finally:
            context.destroy(context_id)
