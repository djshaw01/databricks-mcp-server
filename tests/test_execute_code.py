import tempfile
import unittest
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks.sdk.service.jobs import ViewsToExport

from databricks_mcp.compute_cluster import ClusterExecutionResult, run_code_on_cluster, start_cluster
from databricks_mcp.compute_serverless import run_code_on_serverless
from databricks_mcp.server import (
    execute_code,
    execute_notebook,
    get_job_run,
    get_job_run_export,
    get_job_run_output,
    list_compute,
    manage_cluster,
)


class ExecuteCodeToolTests(unittest.TestCase):
    def test_requires_code_or_file_path(self) -> None:
        result = execute_code()

        self.assertFalse(result["success"])
        self.assertIn("code", result["error"].lower())
        self.assertEqual(result["state"], "INVALID_INPUT")
        self.assertEqual(result["compute_type_requested"], "auto")
        self.assertEqual(result["compute_type_resolved"], "none")
        self.assertEqual(result["language"], "python")
        self.assertEqual(result["output_kind"], "none")

    def test_rejects_unknown_compute_type(self) -> None:
        result = execute_code(code="print('hi')", compute_type="gpu")

        self.assertFalse(result["success"])
        self.assertIn("not valid", result["error"].lower())
        self.assertEqual(result["compute_type_resolved"], "none")
        self.assertEqual(result["output_kind"], "none")

    @patch("databricks_mcp.server.run_code_on_serverless")
    def test_whitespace_only_compute_type_treated_as_missing(self, mock_run_code_on_serverless: MagicMock) -> None:
        mock_run_code_on_serverless.return_value.to_dict.return_value = {"success": True}

        result = execute_code(code="print('hi')", compute_type="   ")

        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_requested"], "auto")
        self.assertEqual(result["compute_type_resolved"], "serverless")

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
    def test_trims_whitespace_in_compute_type_and_language(self, mock_run_code_on_serverless: MagicMock) -> None:
        mock_run_code_on_serverless.return_value.to_dict.return_value = {
            "success": True,
            "output": "",
        }

        result = execute_code(code="print('hi')", compute_type=" serverless ", language=" python ")

        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_requested"], "serverless")
        self.assertEqual(result["language"], "python")
        self.assertEqual(result["output_kind"], "text")

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

            with patch("databricks_mcp.server.pathlib.Path.cwd", return_value=Path(temp_dir)):
                execute_code(file_path=str(path))

        self.assertEqual(mock_run_code_on_serverless.call_args.kwargs["language"], "sql")
        self.assertEqual(mock_run_code_on_serverless.call_args.kwargs["code"], "SELECT 1")

    @patch("databricks_mcp.server.run_code_on_serverless")
    def test_rejects_file_path_outside_cwd_by_default(self, mock_run_code_on_serverless: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_dir = Path(temp_dir) / "workspace"
            workspace_dir.mkdir()
            outside_path = Path(temp_dir) / "query.sql"
            outside_path.write_text("SELECT 1", encoding="utf-8")

            with patch("databricks_mcp.server.pathlib.Path.cwd", return_value=workspace_dir):
                result = execute_code(file_path=str(outside_path))

        self.assertFalse(result["success"])
        self.assertIn("current working directory", result["error"])
        mock_run_code_on_serverless.assert_not_called()

    @patch.dict(os.environ, {"DATABRICKS_MCP_ALLOW_ARBITRARY_LOCAL_FILE_PATHS": "1"}, clear=False)
    @patch("databricks_mcp.server.run_code_on_serverless")
    def test_allows_file_path_outside_cwd_when_opted_in(self, mock_run_code_on_serverless: MagicMock) -> None:
        mock_run_code_on_serverless.return_value.to_dict.return_value = {"success": True}

        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_dir = Path(temp_dir) / "workspace"
            workspace_dir.mkdir()
            outside_path = Path(temp_dir) / "query.sql"
            outside_path.write_text("SELECT 1", encoding="utf-8")

            with patch("databricks_mcp.server.pathlib.Path.cwd", return_value=workspace_dir):
                result = execute_code(file_path=str(outside_path))

        self.assertTrue(result["success"])
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

    def test_no_running_cluster_suggestion_avoids_claiming_ownership(self) -> None:
        from databricks_mcp.compute_cluster import NoRunningClusterError

        error = NoRunningClusterError(
            available_clusters=[],
            startable_clusters=[{"cluster_name": "demo", "cluster_id": "abc", "state": "TERMINATED"}],
        )

        self.assertIn("terminated cluster you may be able to start", error.suggestions[0])

    def test_missing_file_returns_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_path = Path(temp_dir) / "does-not-exist.py"
            with patch("databricks_mcp.server.pathlib.Path.cwd", return_value=Path(temp_dir)):
                result = execute_code(file_path=str(missing_path))

        self.assertFalse(result["success"])
        self.assertIn("not found", result["error"].lower())
        self.assertEqual(result["compute_type_resolved"], "auto")
        self.assertEqual(result["output_kind"], "none")

    def test_rejects_ipynb_files_for_execute_code(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "demo.ipynb"
            path.write_text("{}", encoding="utf-8")

            with patch("databricks_mcp.server.pathlib.Path.cwd", return_value=Path(temp_dir)):
                result = execute_code(file_path=str(path))

        self.assertFalse(result["success"])
        self.assertIn("execute_notebook", result["error"])
        self.assertEqual(result["compute_type_resolved"], "auto")
        self.assertEqual(result["output_kind"], "none")

    def test_rejects_cluster_only_args_for_serverless_route(self) -> None:
        result = execute_code(code="print('hi')", compute_type="serverless", cluster_id="abc")

        self.assertFalse(result["success"])
        self.assertIn("only valid", result["error"].lower())
        self.assertEqual(result["compute_type_resolved"], "serverless")
        self.assertEqual(result["output_kind"], "none")

    def test_rejects_serverless_only_args_for_cluster_route(self) -> None:
        result = execute_code(code="print('hi')", compute_type="cluster", run_name="demo")

        self.assertFalse(result["success"])
        self.assertIn("serverless execution", result["error"].lower())
        self.assertEqual(result["compute_type_resolved"], "cluster")
        self.assertEqual(result["output_kind"], "none")

    def test_context_id_requires_cluster_id(self) -> None:
        result = execute_code(code="print('hi')", compute_type="cluster", context_id="ctx-1")

        self.assertFalse(result["success"])
        self.assertIn("cluster_id", result["error"])
        self.assertEqual(result["compute_type_resolved"], "cluster")
        self.assertEqual(result["output_kind"], "none")

    @patch("databricks_mcp.server.run_code_on_cluster")
    def test_cluster_path_returns_structured_error_for_configuration_failures(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = ValueError("missing host")

        result = execute_code(code="print('hi')", compute_type="cluster", cluster_id="abc")

        self.assertFalse(result["success"])
        self.assertEqual(result["state"], "FAILED")
        self.assertEqual(result["compute_type_resolved"], "cluster")

    @patch("databricks_mcp.server.run_code_on_cluster")
    def test_trims_cluster_identifiers(self, mock_run: MagicMock) -> None:
        mock_run.return_value.to_dict.return_value = {"success": True}

        result = execute_code(
            code="print('hi')",
            compute_type=" cluster ",
            cluster_id=" abc ",
            context_id=" ctx-1 ",
        )

        self.assertTrue(result["success"])
        self.assertEqual(mock_run.call_args.kwargs["cluster_id"], "abc")
        self.assertEqual(mock_run.call_args.kwargs["context_id"], "ctx-1")


class ExecuteNotebookToolTests(unittest.TestCase):
    def test_requires_notebook_source_or_path(self) -> None:
        result = execute_notebook()

        self.assertFalse(result["success"])
        self.assertIn("notebook_path", result["error"])
        self.assertEqual(result["compute_type_requested"], "serverless")
        self.assertEqual(result["compute_type_resolved"], "none")
        self.assertEqual(result["output_kind"], "none")

    def test_rejects_unknown_compute_type(self) -> None:
        result = execute_notebook(notebook_path="/Workspace/Users/tester/demo", compute_type="auto")

        self.assertFalse(result["success"])
        self.assertIn("not valid", result["error"])
        self.assertEqual(result["compute_type_resolved"], "none")
        self.assertEqual(result["output_kind"], "none")

    def test_cluster_requires_cluster_id(self) -> None:
        result = execute_notebook(notebook_path="/Workspace/Users/tester/demo", compute_type="cluster")

        self.assertFalse(result["success"])
        self.assertIn("cluster_id", result["error"])
        self.assertEqual(result["compute_type_resolved"], "cluster")
        self.assertEqual(result["output_kind"], "none")

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
            with patch("databricks_mcp.server.pathlib.Path.cwd", return_value=Path(temp_dir)):
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

    @patch("databricks_mcp.server.run_notebook_job")
    def test_trims_whitespace_in_execute_notebook_inputs(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value.to_dict.return_value = {"success": True, "output": ""}

        result = execute_notebook(
            notebook_path="/Workspace/Users/tester/demo",
            compute_type=" cluster ",
            language=" python ",
            cluster_id="abc",
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["compute_type_requested"], "cluster")
        self.assertEqual(result["language"], "python")
        self.assertEqual(result["output_kind"], "text")

    @patch("databricks_mcp.server.run_notebook_job")
    def test_execute_notebook_trims_remaining_string_inputs(self, mock_run_notebook_job: MagicMock) -> None:
        mock_run_notebook_job.return_value.to_dict.return_value = {"success": True}

        result = execute_notebook(
            notebook_path=" /Workspace/Users/tester/demo ",
            compute_type=" cluster ",
            language=" python ",
            run_name=" nightly-run ",
            cluster_id=" abc ",
        )

        self.assertTrue(result["success"])
        self.assertEqual(mock_run_notebook_job.call_args.kwargs["notebook_path"], "/Workspace/Users/tester/demo")
        self.assertEqual(mock_run_notebook_job.call_args.kwargs["run_name"], "nightly-run")
        self.assertEqual(mock_run_notebook_job.call_args.kwargs["cluster_id"], "abc")

    @patch("databricks_mcp.server.run_notebook_job")
    def test_execute_notebook_rejects_file_path_outside_cwd_by_default(self, mock_run_notebook_job: MagicMock) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace_dir = Path(temp_dir) / "workspace"
            workspace_dir.mkdir()
            outside_path = Path(temp_dir) / "demo.scala"
            outside_path.write_text("// Databricks notebook source\nprintln(42)", encoding="utf-8")

            with patch("databricks_mcp.server.pathlib.Path.cwd", return_value=workspace_dir):
                result = execute_notebook(file_path=str(outside_path), notebook_path="/Workspace/Users/tester/demo")

        self.assertFalse(result["success"])
        self.assertIn("current working directory", result["error"])
        mock_run_notebook_job.assert_not_called()


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
    def test_get_job_run_output_strips_whitespace_from_task_key(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(
            tasks=[
                SimpleNamespace(task_key="extract", run_id=456),
                SimpleNamespace(task_key="load", run_id=789),
            ]
        )
        client.jobs.get_run_output.return_value = SimpleNamespace(
            as_dict=lambda: {"notebook_output": {"result": None}, "logs": "load complete", "error": None, "error_trace": None},
            notebook_output=SimpleNamespace(result=None),
            logs="load complete",
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = get_job_run_output(run_id=123, task_key=" load ")

        self.assertEqual(result["resolved_run_id"], 789)
        self.assertEqual(result["task_key"], "load")
        client.jobs.get_run_output.assert_called_once_with(run_id=789)

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_output_preserves_empty_notebook_output(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(tasks=[SimpleNamespace(task_key="main", run_id=456)])
        client.jobs.get_run_output.return_value = SimpleNamespace(
            as_dict=lambda: {
                "notebook_output": {"result": ""},
                "logs": None,
                "error": None,
                "error_trace": None,
            },
            notebook_output=SimpleNamespace(result=""),
            logs=None,
            error=None,
            error_trace=None,
        )
        mock_get_client.return_value = client

        result = get_job_run_output(run_id=123)

        self.assertEqual(result["output"], "")
        self.assertEqual(result["output_kind"], "notebook_result")

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

    @patch("databricks_mcp.server._get_client")
    def test_get_job_run_export_counts_empty_html_view_content(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.jobs.get_run.return_value = SimpleNamespace(tasks=[])
        client.jobs.export_run.return_value = SimpleNamespace(
            views=[
                SimpleNamespace(
                    name="Notebook",
                    type=SimpleNamespace(value="NOTEBOOK"),
                    content="",
                )
            ]
        )
        mock_get_client.return_value = client

        result = get_job_run_export(run_id=123)

        self.assertEqual(result["html_view_count"], 1)
        self.assertEqual(result["views"][0]["content"], "")


class RunCodeOnClusterTests(unittest.TestCase):
    def test_cluster_result_defaults_context_destroyed_to_false(self) -> None:
        result = ClusterExecutionResult(success=False, error="boom")

        self.assertFalse(result.context_destroyed)

    @patch("databricks_mcp.compute_cluster.destroy_context")
    @patch("databricks_mcp.compute_cluster._run_on_context")
    @patch("databricks_mcp.compute_cluster.create_context")
    @patch("databricks_mcp.compute_cluster.get_client")
    def test_destroy_context_on_failure_uses_failure_message(
        self,
        mock_get_client: MagicMock,
        mock_create_context: MagicMock,
        mock_run_on_context: MagicMock,
        mock_destroy_context: MagicMock,
    ) -> None:
        mock_get_client.return_value = MagicMock()
        mock_create_context.return_value = "ctx-1"
        mock_run_on_context.return_value = ClusterExecutionResult(
            success=False,
            error="boom",
            output_kind="none",
            cluster_id="abc",
            context_id="ctx-1",
            context_destroyed=False,
        )

        result = run_code_on_cluster(code="print(1)", cluster_id="abc", destroy_context_on_completion=True)

        self.assertFalse(result.success)
        self.assertEqual(result.message, "Execution failed. Context was destroyed.")
        self.assertTrue(result.context_destroyed)
        mock_destroy_context.assert_called_once_with("abc", "ctx-1", "")

    @patch("databricks_mcp.compute_cluster.destroy_context")
    @patch("databricks_mcp.compute_cluster._run_on_context")
    @patch("databricks_mcp.compute_cluster.create_context")
    @patch("databricks_mcp.compute_cluster.get_client")
    def test_unexpected_exception_returns_structured_failure(
        self,
        mock_get_client: MagicMock,
        mock_create_context: MagicMock,
        mock_run_on_context: MagicMock,
        mock_destroy_context: MagicMock,
    ) -> None:
        mock_get_client.return_value = MagicMock()
        mock_create_context.return_value = "ctx-2"
        mock_run_on_context.side_effect = RuntimeError("command execution exploded")

        result = run_code_on_cluster(code="print(1)", cluster_id="abc", destroy_context_on_completion=True)

        self.assertFalse(result.success)
        self.assertEqual(result.error, "command execution exploded")
        self.assertEqual(result.context_id, "ctx-2")
        self.assertTrue(result.context_destroyed)
        self.assertEqual(result.message, "Execution failed. Context was destroyed.")
        mock_destroy_context.assert_called_once_with("abc", "ctx-2", "")

    @patch("databricks_mcp.compute_cluster.destroy_context")
    @patch("databricks_mcp.compute_cluster.create_context")
    @patch("databricks_mcp.compute_cluster.get_client")
    def test_exception_before_context_creation_does_not_report_destroyed(
        self,
        mock_get_client: MagicMock,
        mock_create_context: MagicMock,
        mock_destroy_context: MagicMock,
    ) -> None:
        mock_get_client.return_value = MagicMock()
        mock_create_context.side_effect = RuntimeError("context creation failed")

        result = run_code_on_cluster(code="print(1)", cluster_id="abc", destroy_context_on_completion=True)

        self.assertFalse(result.success)
        self.assertFalse(result.context_destroyed)
        self.assertEqual(result.message, "Execution failed.")
        mock_destroy_context.assert_not_called()

    def test_context_reuse_requires_cluster_id(self) -> None:
        result = run_code_on_cluster(code="print(1)", context_id="ctx-1")

        self.assertFalse(result.success)
        self.assertIn("cluster_id", result.error)

    @patch("databricks_mcp.compute_cluster._run_on_context")
    @patch("databricks_mcp.compute_cluster.create_context")
    @patch("databricks_mcp.compute_cluster._select_best_cluster")
    @patch("databricks_mcp.compute_cluster.get_current_username")
    @patch("databricks_mcp.compute_cluster.get_client")
    def test_auto_select_uses_best_cluster_without_raising_no_running_cluster(
        self,
        mock_get_client: MagicMock,
        mock_get_current_username: MagicMock,
        mock_select_best_cluster: MagicMock,
        mock_create_context: MagicMock,
        mock_run_on_context: MagicMock,
    ) -> None:
        mock_get_client.return_value = MagicMock()
        mock_get_current_username.return_value = "user@example.com"
        mock_select_best_cluster.return_value = ("abc", [])
        mock_create_context.return_value = "ctx-4"
        mock_run_on_context.return_value = ClusterExecutionResult(
            success=True,
            output="ok",
            output_kind="text",
            cluster_id="abc",
            context_id="ctx-4",
        )

        result = run_code_on_cluster(code="print(1)")

        self.assertTrue(result.success)
        mock_create_context.assert_called_once_with("abc", "python", "")
        mock_run_on_context.assert_called_once()

    @patch("databricks_mcp.compute_cluster.destroy_context", return_value=False)
    @patch("databricks_mcp.compute_cluster._run_on_context")
    @patch("databricks_mcp.compute_cluster.create_context")
    @patch("databricks_mcp.compute_cluster.get_client")
    def test_destroy_context_failure_does_not_claim_success(
        self,
        mock_get_client: MagicMock,
        mock_create_context: MagicMock,
        mock_run_on_context: MagicMock,
        mock_destroy_context: MagicMock,
    ) -> None:
        mock_get_client.return_value = MagicMock()
        mock_create_context.return_value = "ctx-3"
        mock_run_on_context.return_value = ClusterExecutionResult(
            success=True,
            output="ok",
            output_kind="text",
            cluster_id="abc",
            context_id="ctx-3",
            context_destroyed=False,
        )

        result = run_code_on_cluster(code="print(1)", cluster_id="abc", destroy_context_on_completion=True)

        self.assertFalse(result.context_destroyed)
        self.assertEqual(result.message, "Execution succeeded, but the context could not be destroyed.")


class StartClusterTests(unittest.TestCase):
    @patch("databricks_mcp.compute_cluster.get_client")
    def test_start_cluster_guidance_uses_manage_cluster_status(self, mock_get_client: MagicMock) -> None:
        client = MagicMock()
        client.clusters.get.return_value = SimpleNamespace(
            cluster_name="demo",
            state=SimpleNamespace(value="TERMINATED"),
        )
        mock_get_client.return_value = client

        result = start_cluster("abc")

        self.assertIn("manage_cluster(action='status'", result["message"])


class ComputeToolTests(unittest.TestCase):
    @patch("databricks_mcp.server.list_clusters")
    def test_list_compute_forwards_include_terminated(self, mock_list_clusters: MagicMock) -> None:
        mock_list_clusters.return_value = [{"cluster_id": "abc"}]

        result = list_compute(include_terminated=True, profile="test-profile")

        self.assertEqual(result, [{"cluster_id": "abc"}])
        mock_list_clusters.assert_called_once_with(profile="test-profile", include_terminated=True)

    @patch("databricks_mcp.server.get_cluster_status")
    def test_manage_cluster_status_returns_normalized_success_payload(self, mock_get_cluster_status: MagicMock) -> None:
        mock_get_cluster_status.return_value = {
            "cluster_id": "abc",
            "cluster_name": "demo",
            "state": "RUNNING",
            "message": "ready",
        }

        result = manage_cluster(action="status", cluster_id="abc", profile="test-profile")

        self.assertTrue(result["success"])
        self.assertIsNone(result["error"])
        self.assertEqual(result["cluster_id"], "abc")
        mock_get_cluster_status.assert_called_once_with("abc", "test-profile")

    @patch("databricks_mcp.server.start_cluster")
    def test_manage_cluster_start_returns_normalized_success_payload(self, mock_start_cluster: MagicMock) -> None:
        mock_start_cluster.return_value = {
            "cluster_id": "abc",
            "cluster_name": "demo",
            "state": "PENDING",
            "message": "starting",
        }

        result = manage_cluster(action="start", cluster_id="abc", profile="test-profile")

        self.assertTrue(result["success"])
        self.assertIsNone(result["error"])
        self.assertEqual(result["state"], "PENDING")
        mock_start_cluster.assert_called_once_with("abc", "test-profile")

    def test_manage_cluster_rejects_unknown_action(self) -> None:
        result = manage_cluster(action="stop", cluster_id="abc")

        self.assertFalse(result["success"])
        self.assertIn("Unknown action", result["error"])


if __name__ == "__main__":
    unittest.main()
