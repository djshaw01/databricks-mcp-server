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

    def test_rejects_unknown_compute_type(self) -> None:
        result = execute_code(code="print('hi')", compute_type="gpu")

        self.assertFalse(result["success"])
        self.assertIn("not valid", result["error"].lower())

    @patch("databricks_mcp.server.run_code_on_cluster")
    def test_cluster_compute_type_routes_to_cluster(self, mock_run: MagicMock) -> None:
        mock_run.return_value.to_dict.return_value = {
            "success": True,
            "output": "42",
            "output_kind": "text",
            "cluster_id": "abc",
            "context_id": "ctx1",
            "context_destroyed": False,
        }
        result = execute_code(
            code="print(42)",
            compute_type="cluster",
            cluster_id="abc",
            profile="test-profile",
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_requested"], "cluster")
        self.assertEqual(result["compute_type_resolved"], "cluster")
        self.assertEqual(result["output_kind"], "text")
        mock_run.assert_called_once_with(
            code="print(42)",
            profile="test-profile",
            cluster_id="abc",
            context_id=None,
            language="python",
            timeout=120,
            destroy_context_on_completion=False,
        )

    @patch("databricks_mcp.server.run_code_on_serverless")
    def test_auto_routes_to_serverless(self, mock_run_code_on_serverless: MagicMock) -> None:
        mock_run_code_on_serverless.return_value.to_dict.return_value = {
            "success": True,
            "output": "hi",
            "output_kind": "text",
        }

        result = execute_code(code="print('hi')", profile="dan-test-db-2")

        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_requested"], "auto")
        self.assertEqual(result["compute_type_resolved"], "serverless")
        self.assertEqual(result["language"], "python")
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

    @patch("databricks_mcp.server.run_code_on_cluster")
    def test_auto_routes_scala_to_cluster(self, mock_run: MagicMock) -> None:
        mock_run.return_value.to_dict.return_value = {"success": True, "output": "scala out", "output_kind": "text"}
        result = execute_code(code='println("hi")', language="scala", cluster_id="abc")
        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_requested"], "auto")
        self.assertEqual(result["compute_type_resolved"], "cluster")
        mock_run.assert_called_once()

    @patch("databricks_mcp.server.run_code_on_cluster")
    def test_no_running_cluster_error_returns_structured_dict(self, mock_run: MagicMock) -> None:
        from databricks_mcp.compute_cluster import NoRunningClusterError

        mock_run.side_effect = NoRunningClusterError(available_clusters=[], startable_clusters=[])
        result = execute_code(code="print(1)", compute_type="cluster")
        self.assertFalse(result["success"])
        self.assertEqual(result["error_type"], "no_running_cluster")
        self.assertEqual(result["compute_type_resolved"], "cluster")

    def test_missing_file_returns_error(self) -> None:
        result = execute_code(file_path="/does/not/exist.py")

        self.assertFalse(result["success"])
        self.assertIn("not found", result["error"].lower())

    def test_rejects_cluster_only_args_for_serverless_route(self) -> None:
        result = execute_code(code="print('hi')", compute_type="serverless", cluster_id="abc")

        self.assertFalse(result["success"])
        self.assertIn("only valid", result["error"].lower())

    def test_rejects_serverless_only_args_for_cluster_route(self) -> None:
        result = execute_code(code="print('hi')", compute_type="cluster", run_name="demo")

        self.assertFalse(result["success"])
        self.assertIn("serverless execution", result["error"].lower())


class RunCodeOnServerlessTests(unittest.TestCase):
    def test_rejects_empty_code(self) -> None:
        result = run_code_on_serverless(code="")

        self.assertFalse(result.success)
        self.assertEqual(result.state, "INVALID_INPUT")
        self.assertEqual(result.output_kind, "none")

    def test_rejects_unsupported_language(self) -> None:
        result = run_code_on_serverless(code="println(42)", language="scala")

        self.assertFalse(result.success)
        self.assertEqual(result.state, "INVALID_INPUT")
        self.assertEqual(result.output_kind, "none")

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
        self.assertEqual(result.output_kind, "text")
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
        self.assertEqual(result.output_kind, "none")
        self.assertIn("submit failed", result.error)

    @patch("databricks_mcp.compute_serverless.get_client")
    def test_success_without_output_uses_none_output_kind(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        wait = MagicMock()
        wait.run_id = 123
        wait.result.return_value = SimpleNamespace(
            state=SimpleNamespace(result_state=RunResultState.SUCCESS, state_message=None),
            run_page_url="https://example.test/runs/123",
            tasks=[SimpleNamespace(run_id=456)],
        )
        client.jobs.submit.return_value = wait
        client.jobs.get_run.side_effect = [SimpleNamespace(run_page_url="https://example.test/runs/123")]
        client.jobs.get_run_output.return_value = SimpleNamespace(
            notebook_output=SimpleNamespace(result=None),
            logs=None,
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = run_code_on_serverless(
            code="print('hello')",
            workspace_path="/Workspace/Users/tester/serverless-demo",
        )

        self.assertTrue(result.success)
        self.assertIsNone(result.output)
        self.assertEqual(result.output_kind, "none")


if __name__ == "__main__":
    unittest.main()
