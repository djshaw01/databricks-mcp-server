import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks.sdk.service.jobs import RunResultState

from databricks_mcp.notebook_jobs import run_notebook_job


def _successful_wait(run_id: int = 123, task_run_id: int = 456) -> MagicMock:
    wait = MagicMock()
    wait.run_id = run_id
    wait.result.return_value = SimpleNamespace(
        state=SimpleNamespace(result_state=RunResultState.SUCCESS, state_message=None),
        run_page_url=f"https://example.test/runs/{run_id}",
        tasks=[SimpleNamespace(run_id=task_run_id)],
    )
    return wait


class NotebookJobRunnerTests(unittest.TestCase):
    @patch("databricks_mcp.notebook_jobs.get_client")
    def test_runs_existing_notebook_on_cluster_without_upload(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.submit.return_value = _successful_wait()
        client.jobs.get_run.side_effect = [SimpleNamespace(run_page_url="https://example.test/runs/123")]
        client.jobs.get_run_output.return_value = SimpleNamespace(
            notebook_output=SimpleNamespace(result="cluster output"),
            logs=None,
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = run_notebook_job(
            compute_type="cluster",
            notebook_path="/Workspace/Users/tester/existing",
            cluster_id="abc",
        )

        self.assertTrue(result.success)
        self.assertEqual(result.cluster_id, "abc")
        self.assertEqual(result.notebook_path, "/Workspace/Users/tester/existing")
        client.workspace.import_.assert_not_called()
        submit_task = client.jobs.submit.call_args.kwargs["tasks"][0]
        self.assertEqual(submit_task.existing_cluster_id, "abc")
        self.assertEqual(submit_task.notebook_task.notebook_path, "/Workspace/Users/tester/existing")

    @patch("databricks_mcp.notebook_jobs.get_client")
    def test_uploads_and_runs_serverless_notebook_with_parameters(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.submit.return_value = _successful_wait()
        client.jobs.get_run.side_effect = [SimpleNamespace(run_page_url="https://example.test/runs/123")]
        client.jobs.get_run_output.return_value = SimpleNamespace(
            notebook_output=SimpleNamespace(result="serverless output"),
            logs="stdout line",
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = run_notebook_job(
            compute_type="serverless",
            code="print('hello')",
            language="python",
            notebook_path="/Workspace/Users/tester/persisted",
            notebook_parameters={"env": "dev"},
        )

        self.assertTrue(result.success)
        self.assertEqual(result.notebook_path, "/Workspace/Users/tester/persisted")
        client.workspace.import_.assert_called_once()
        client.workspace.delete.assert_not_called()
        submit_task = client.jobs.submit.call_args.kwargs["tasks"][0]
        self.assertEqual(submit_task.environment_key, "Default")
        self.assertEqual(submit_task.notebook_task.base_parameters, {"env": "dev"})
        self.assertEqual(result.output, "serverless output\n\n--- Logs ---\nstdout line")

    @patch("databricks_mcp.notebook_jobs.get_client")
    def test_temp_upload_is_cleaned_up_after_run(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.submit.return_value = _successful_wait()
        client.jobs.get_run.side_effect = [SimpleNamespace(run_page_url="https://example.test/runs/123")]
        client.jobs.get_run_output.return_value = SimpleNamespace(
            notebook_output=SimpleNamespace(result="ok"),
            logs=None,
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = run_notebook_job(
            compute_type="serverless",
            code="print('hello')",
            language="python",
        )

        self.assertTrue(result.success)
        client.workspace.import_.assert_called_once()
        client.workspace.delete.assert_called_once()

    def test_cluster_mode_requires_cluster_id(self) -> None:
        result = run_notebook_job(compute_type="cluster", notebook_path="/Workspace/Users/tester/existing")

        self.assertFalse(result.success)
        self.assertEqual(result.state, "INVALID_INPUT")
        self.assertIn("cluster_id", result.error)


if __name__ == "__main__":
    unittest.main()
