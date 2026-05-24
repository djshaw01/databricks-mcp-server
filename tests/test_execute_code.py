import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks.sdk.service.jobs import ViewsToExport

from databricks_mcp.compute_serverless import run_code_on_serverless
from databricks_mcp.server import execute_code, execute_notebook, get_job_run, get_job_run_export, get_job_run_output


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


class ExecuteNotebookToolTests(unittest.TestCase):
    def test_requires_notebook_source_or_path(self) -> None:
        result = execute_notebook()

        self.assertFalse(result["success"])
        self.assertIn("notebook_path", result["error"])

    def test_rejects_unknown_compute_type(self) -> None:
        result = execute_notebook(notebook_path="/Workspace/Users/tester/demo", compute_type="auto")

        self.assertFalse(result["success"])
        self.assertIn("not valid", result["error"])

    def test_cluster_requires_cluster_id(self) -> None:
        result = execute_notebook(notebook_path="/Workspace/Users/tester/demo", compute_type="cluster")

        self.assertFalse(result["success"])
        self.assertIn("cluster_id", result["error"])

    @patch("databricks_mcp.server.run_notebook_job")
    def test_runs_existing_notebook(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value.to_dict.return_value = {
            "success": True,
            "run_id": 123,
            "notebook_path": "/Workspace/Users/tester/demo",
            "output_kind": "text",
        }

        result = execute_notebook(notebook_path="/Workspace/Users/tester/demo")

        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_resolved"], "serverless")
        self.assertEqual(result["notebook_path"], "/Workspace/Users/tester/demo")
        mock_run_notebook_job.assert_called_once_with(
            profile="",
            compute_type="serverless",
            notebook_path="/Workspace/Users/tester/demo",
            code=None,
            language="python",
            timeout=1800,
            run_name=None,
            cluster_id=None,
            notebook_parameters=None,
        )

    @patch("databricks_mcp.server.run_notebook_job")
    def test_uploads_file_for_cluster_notebook_run(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value.to_dict.return_value = {
            "success": True,
            "run_id": 321,
            "notebook_path": "/Workspace/Users/tester/demo",
            "cluster_id": "abc",
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "demo.scala"
            path.write_text("// Databricks notebook source\nprintln(42)", encoding="utf-8")
            result = execute_notebook(
                file_path=str(path),
                notebook_path="/Workspace/Users/tester/demo",
                compute_type="cluster",
                cluster_id="abc",
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_resolved"], "cluster")
        self.assertEqual(result["cluster_id"], "abc")
        self.assertEqual(mock_run_notebook_job.call_args.kwargs["language"], "scala")
        self.assertEqual(mock_run_notebook_job.call_args.kwargs["cluster_id"], "abc")
        self.assertEqual(mock_run_notebook_job.call_args.kwargs["notebook_path"], "/Workspace/Users/tester/demo")

    @patch("databricks_mcp.server.run_notebook_job")
    def test_passes_notebook_parameters(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value.to_dict.return_value = {"success": True}

        execute_notebook(
            notebook_path="/Workspace/Users/tester/demo",
            notebook_parameters={"env": "dev", "limit": "10"},
        )

        self.assertEqual(
            mock_run_notebook_job.call_args.kwargs["notebook_parameters"],
            {"env": "dev", "limit": "10"},
        )


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

    @patch("databricks_mcp.compute_serverless.run_notebook_job")
    def test_returns_successful_serverless_result(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value = SimpleNamespace(
            success=True,
            output="hello world",
            output_kind="text",
            error=None,
            run_id=123,
            run_url="https://example.test/runs/123",
            duration_seconds=1.23,
            state="SUCCESS",
            message="ok",
            notebook_path="/Workspace/Users/tester/serverless-demo",
        )

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
        mock_run_notebook_job.assert_called_once()

    @patch("databricks_mcp.compute_serverless.run_notebook_job")
    def test_success_without_workspace_path_keeps_effective_notebook_path(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value = SimpleNamespace(
            success=True,
            output="hello world",
            output_kind="text",
            error=None,
            run_id=123,
            run_url="https://example.test/runs/123",
            duration_seconds=1.23,
            state="SUCCESS",
            message="ok",
            notebook_path="/Workspace/Users/tester/.databricks_mcp_tmp/notebook_serverless_abc123",
        )

        result = run_code_on_serverless(code="print('hello')", profile="dan-test-db-2")

        self.assertEqual(result.notebook_path, "/Workspace/Users/tester/.databricks_mcp_tmp/notebook_serverless_abc123")
        self.assertIsNone(result.workspace_path)
        self.assertEqual(result.to_dict()["notebook_path"], "/Workspace/Users/tester/.databricks_mcp_tmp/notebook_serverless_abc123")
        self.assertNotIn("workspace_path", result.to_dict())

    @patch("databricks_mcp.compute_serverless.run_notebook_job")
    def test_submit_failure_returns_structured_error(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value = SimpleNamespace(
            success=False,
            output=None,
            output_kind="none",
            error="submit failed",
            run_id=None,
            run_url=None,
            duration_seconds=None,
            state="SUBMIT_FAILED",
            message="bad",
            notebook_path="/Workspace/Users/tester/serverless-demo",
        )

        result = run_code_on_serverless(
            code="print('hello')",
            workspace_path="/Workspace/Users/tester/serverless-demo",
        )

        self.assertFalse(result.success)
        self.assertEqual(result.state, "SUBMIT_FAILED")
        self.assertEqual(result.output_kind, "none")
        self.assertIn("submit failed", result.error)

    @patch("databricks_mcp.compute_serverless.run_notebook_job")
    def test_success_without_output_uses_none_output_kind(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value = SimpleNamespace(
            success=True,
            output=None,
            output_kind="none",
            error=None,
            run_id=123,
            run_url="https://example.test/runs/123",
            duration_seconds=1.23,
            state="SUCCESS",
            message="ok",
            notebook_path="/Workspace/Users/tester/serverless-demo",
        )

        result = run_code_on_serverless(
            code="print('hello')",
            workspace_path="/Workspace/Users/tester/serverless-demo",
        )

        self.assertTrue(result.success)
        self.assertIsNone(result.output)
        self.assertEqual(result.output_kind, "none")


class JobRunToolsTests(unittest.TestCase):
    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_includes_task_run_ids(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(
            run_id=123,
            job_id=456,
            run_name="demo",
            trigger="MANUAL",
            state=SimpleNamespace(
                life_cycle_state=SimpleNamespace(value="TERMINATED"),
                result_state=SimpleNamespace(value="SUCCESS"),
                state_message=None,
            ),
            start_time=None,
            end_time=None,
            run_page_url="https://example.test/runs/123",
            tasks=[
                SimpleNamespace(
                    task_key="main",
                    run_id=789,
                    state=SimpleNamespace(
                        life_cycle_state=SimpleNamespace(value="TERMINATED"),
                        result_state=SimpleNamespace(value="SUCCESS"),
                        state_message=None,
                    ),
                    start_time=None,
                    end_time=None,
                    run_page_url="https://example.test/runs/789",
                )
            ],
        )
        mock_get_client.return_value = client

        result = get_job_run(run_id=123)

        self.assertEqual(result["tasks"][0]["task_run_id"], 789)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_output_auto_resolves_single_task_run(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(
            tasks=[SimpleNamespace(task_key="main", run_id=456)]
        )
        client.jobs.get_run_output.return_value = SimpleNamespace(
            as_dict=lambda: {
                "metadata": {"note": "captured"},
                "notebook_output": {"result": "table rows"},
                "logs": "stdout line",
                "error": None,
                "error_trace": None,
            },
            notebook_output=SimpleNamespace(result="table rows"),
            logs="stdout line",
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = get_job_run_output(run_id=123)

        self.assertEqual(result["requested_run_id"], 123)
        self.assertEqual(result["resolved_run_id"], 456)
        self.assertEqual(result["task_key"], "main")
        self.assertEqual(result["task_run_id"], 456)
        self.assertEqual(result["notebook_output_result"], "table rows")
        self.assertEqual(result["output_kind"], "notebook_result+logs")
        self.assertIn("table rows", result["output"])
        self.assertIn("stdout line", result["output"])
        client.jobs.get_run_output.assert_called_once_with(run_id=456)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_output_preserves_task_run_id_when_no_tasks_are_present(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(tasks=[])
        client.jobs.get_run_output.return_value = SimpleNamespace(
            as_dict=lambda: {
                "notebook_output": {"result": "single run"},
                "logs": None,
                "error": None,
                "error_trace": None,
            },
            notebook_output=SimpleNamespace(result="single run"),
            logs=None,
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = get_job_run_output(run_id=123)

        self.assertEqual(result["resolved_run_id"], 123)
        self.assertEqual(result["task_run_id"], 123)
        self.assertIsNone(result["task_key"])
        client.jobs.get_run_output.assert_called_once_with(run_id=123)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_export_preserves_task_run_id_when_no_tasks_are_present(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(tasks=[])
        client.jobs.export_run.return_value = SimpleNamespace(
            views=[
                SimpleNamespace(
                    name="Notebook",
                    type=SimpleNamespace(value="NOTEBOOK"),
                    content="<html><body>rendered output</body></html>",
                )
            ]
        )
        mock_get_client.return_value = client

        result = get_job_run_export(run_id=123)

        self.assertEqual(result["resolved_run_id"], 123)
        self.assertEqual(result["task_run_id"], 123)
        self.assertIsNone(result["task_key"])
        client.jobs.export_run.assert_called_once_with(run_id=123, views_to_export=ViewsToExport.CODE)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_output_requires_task_key_for_multi_task_runs(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(
            tasks=[
                SimpleNamespace(task_key="extract", run_id=456),
                SimpleNamespace(task_key="load", run_id=789),
            ]
        )
        mock_get_client.return_value = client

        with self.assertRaisesRegex(ValueError, "multiple tasks"):
            get_job_run_output(run_id=123)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_output_accepts_explicit_task_key(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(
            tasks=[
                SimpleNamespace(task_key="extract", run_id=456),
                SimpleNamespace(task_key="load", run_id=789),
            ]
        )
        client.jobs.get_run_output.return_value = SimpleNamespace(
            as_dict=lambda: {
                "notebook_output": {"result": None},
                "logs": "load complete",
                "error": None,
                "error_trace": None,
            },
            notebook_output=SimpleNamespace(result=None),
            logs="load complete",
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = get_job_run_output(run_id=123, task_key="load")

        self.assertEqual(result["resolved_run_id"], 789)
        self.assertEqual(result["task_key"], "load")
        self.assertEqual(result["output"], "load complete")
        self.assertEqual(result["output_kind"], "logs")

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_export_auto_resolves_single_task_run(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(
            tasks=[SimpleNamespace(task_key="main", run_id=456)]
        )
        client.jobs.export_run.return_value = SimpleNamespace(
            views=[
                SimpleNamespace(
                    name="Notebook",
                    type=SimpleNamespace(value="NOTEBOOK"),
                    content="<html><body>rendered output</body></html>",
                )
            ]
        )
        mock_get_client.return_value = client

        result = get_job_run_export(run_id=123)

        self.assertEqual(result["requested_run_id"], 123)
        self.assertEqual(result["resolved_run_id"], 456)
        self.assertEqual(result["task_key"], "main")
        self.assertEqual(result["task_run_id"], 456)
        self.assertEqual(result["views_to_export"], "CODE")
        self.assertEqual(result["view_count"], 1)
        self.assertEqual(result["views"][0]["type"], "NOTEBOOK")
        self.assertIn("rendered output", result["views"][0]["content"])
        client.jobs.export_run.assert_called_once_with(run_id=456, views_to_export=ViewsToExport.CODE)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_export_supports_task_key_and_truncation(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(
            tasks=[
                SimpleNamespace(task_key="extract", run_id=456),
                SimpleNamespace(task_key="train", run_id=789),
            ]
        )
        client.jobs.export_run.return_value = SimpleNamespace(
            views=[
                SimpleNamespace(
                    name="Train Notebook",
                    type=SimpleNamespace(value="NOTEBOOK"),
                    content="abcdefghij",
                )
            ]
        )
        mock_get_client.return_value = client

        result = get_job_run_export(run_id=123, task_key="train", views_to_export="all", max_view_characters=5)

        self.assertEqual(result["resolved_run_id"], 789)
        self.assertEqual(result["task_key"], "train")
        self.assertEqual(result["views_to_export"], "ALL")
        self.assertEqual(result["views"][0]["content"], "abcde")
        self.assertTrue(result["views"][0]["content_truncated"])
        self.assertEqual(result["views"][0]["content_length"], 10)
        client.jobs.export_run.assert_called_once_with(run_id=789, views_to_export=ViewsToExport.ALL)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_export_rejects_invalid_view_selection(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(tasks=[])
        mock_get_client.return_value = client

        with self.assertRaisesRegex(ValueError, "views_to_export"):
            get_job_run_export(run_id=123, views_to_export="widgets")


if __name__ == "__main__":
    unittest.main()
