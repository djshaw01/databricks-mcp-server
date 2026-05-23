import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks.sdk.service.jobs import RunResultState

from databricks_mcp.compute_serverless import run_code_on_serverless
from databricks_mcp.server import execute_code


class ExecuteCodeToolTests(unittest.TestCase):
    def test_requires_code_or_file_path(self) -> None:
        result = execute_code()

        self.assertFalse(result["success"])
        self.assertIn("code", result["error"].lower())

    def test_rejects_cluster_compute_type_in_serverless_phase(self) -> None:
        result = execute_code(code="print('hi')", compute_type="cluster")

        self.assertFalse(result["success"])
        self.assertIn("not supported yet", result["error"].lower())

    @patch("databricks_mcp.server.run_code_on_serverless")
    def test_auto_routes_to_serverless(self, mock_run_code_on_serverless: MagicMock) -> None:
        mock_run_code_on_serverless.return_value.to_dict.return_value = {"success": True}

        result = execute_code(code="print('hi')", profile="dan-test-db-2")

        self.assertTrue(result["success"])
        mock_run_code_on_serverless.assert_called_once_with(
            code="print('hi')",
            profile="dan-test-db-2",
            language="python",
            timeout=1800,
            run_name=None,
            cleanup=True,
            workspace_path=None,
        )

    @patch("databricks_mcp.server.run_code_on_serverless")
    def test_workspace_path_disables_cleanup(self, mock_run_code_on_serverless: MagicMock) -> None:
        mock_run_code_on_serverless.return_value.to_dict.return_value = {"success": True}

        execute_code(
            code="print('hi')",
            workspace_path="/Workspace/Users/tester/persisted",
        )

        self.assertFalse(mock_run_code_on_serverless.call_args.kwargs["cleanup"])
        self.assertEqual(
            mock_run_code_on_serverless.call_args.kwargs["workspace_path"],
            "/Workspace/Users/tester/persisted",
        )

    @patch("databricks_mcp.server.run_code_on_serverless")
    def test_file_path_auto_detects_language(self, mock_run_code_on_serverless: MagicMock) -> None:
        mock_run_code_on_serverless.return_value.to_dict.return_value = {"success": True}

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "query.sql"
            path.write_text("SELECT 1", encoding="utf-8")

            execute_code(file_path=str(path))

        self.assertEqual(mock_run_code_on_serverless.call_args.kwargs["language"], "sql")
        self.assertEqual(mock_run_code_on_serverless.call_args.kwargs["code"], "SELECT 1")

    def test_missing_file_returns_error(self) -> None:
        result = execute_code(file_path="/does/not/exist.py")

        self.assertFalse(result["success"])
        self.assertIn("not found", result["error"].lower())


class RunCodeOnServerlessTests(unittest.TestCase):
    def test_rejects_empty_code(self) -> None:
        result = run_code_on_serverless(code="")

        self.assertFalse(result.success)
        self.assertEqual(result.state, "INVALID_INPUT")

    def test_rejects_unsupported_language(self) -> None:
        result = run_code_on_serverless(code="println(42)", language="scala")

        self.assertFalse(result.success)
        self.assertEqual(result.state, "INVALID_INPUT")

    @patch("databricks_mcp.compute_serverless.get_client")
    def test_returns_successful_serverless_result(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        wait = MagicMock()
        wait.run_id = 123
        wait.result.return_value = SimpleNamespace(
            state=SimpleNamespace(result_state=RunResultState.SUCCESS, state_message=None),
            run_page_url="https://example.test/runs/123",
            tasks=[SimpleNamespace(run_id=456)],
        )
        client.jobs.submit.return_value = wait
        client.jobs.get_run.side_effect = [
            SimpleNamespace(run_page_url="https://example.test/runs/123"),
        ]
        client.jobs.get_run_output.return_value = SimpleNamespace(
            notebook_output=SimpleNamespace(result="hello world"),
            logs=None,
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = run_code_on_serverless(
            code="print('hello')",
            profile="dan-test-db-2",
            workspace_path="/Workspace/Users/tester/serverless-demo",
        )

        self.assertTrue(result.success)
        self.assertEqual(result.output, "hello world")
        self.assertEqual(result.run_id, 123)
        self.assertEqual(result.run_url, "https://example.test/runs/123")
        client.workspace.import_.assert_called_once()
        client.jobs.submit.assert_called_once()

    @patch("databricks_mcp.compute_serverless.get_client")
    def test_submit_failure_returns_structured_error(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.submit.side_effect = RuntimeError("submit failed")
        mock_get_client.return_value = client

        result = run_code_on_serverless(
            code="print('hello')",
            workspace_path="/Workspace/Users/tester/serverless-demo",
        )

        self.assertFalse(result.success)
        self.assertEqual(result.state, "SUBMIT_FAILED")
        self.assertIn("submit failed", result.error)


if __name__ == "__main__":
    unittest.main()
