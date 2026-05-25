"""
Databricks MCP Server

Exposes Databricks Jobs and Delta Live Tables (Pipelines) as MCP tools.
"""

import pathlib
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState, ViewsToExport
from fastmcp import FastMCP

from databricks_mcp.compute_cluster import (
    NoRunningClusterError,
    get_cluster_status,
    list_clusters,
    run_code_on_cluster,
    start_cluster,
)
from databricks_mcp.compute_serverless import run_code_on_serverless
from databricks_mcp.notebook_jobs import run_notebook_job
from databricks_mcp.sql_query import (
    QueryValidationError,
    execute_safe_query,
)
from databricks_mcp.workspace import (
    get_client as _get_client,
    get_sql_poll_timeout_seconds as _get_sql_poll_timeout_seconds,
    get_warehouse_id as _get_warehouse_id,
    parse_positive_poll_timeout_override as _parse_positive_poll_timeout_override,
)

load_dotenv()

mcp = FastMCP(
    "Databricks",
    instructions=(
        "Browse and inspect Databricks jobs, job runs, Delta Live Tables pipelines, "
        "Unity Catalog metadata, and read-only SQL queries. Prefer Unity Catalog "
        "tools (list_catalogs/list_schemas/list_tables/get_table/search_tables/"
        "search_columns) when finding catalogs, schemas, tables, or columns, use "
        "query_sql only when you need query result rows, and use execute_code for "
        "Databricks code execution on serverless workflows or interactive clusters. "
        "Use execute_notebook when creating, modifying, rerunning, or reviewing notebooks "
        "on either serverless or existing clusters. "
        "When execute_code returns a serverless run_id, use get_job_run_output to "
        "inspect notebook result text, logs, and task-level output for the flow of the job. "
        "Use get_job_run_export when you need the exported HTML notebook view for richer "
        "rendering during notebook iteration."
    ),
)

_FILE_EXT_LANGUAGE = {
    ".ipynb": "python",
    ".py": "python",
    ".sql": "sql",
    ".scala": "scala",
    ".r": "r",
}

_TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}


def _none_if_empty(value: str | None) -> str | None:
    if value is None:
        return None
    return None if value.strip() == "" else value


def _normalize_optional_string(value: str | None) -> str | None:
    normalized = _none_if_empty(value)
    return normalized.strip() if normalized is not None else None


def _allow_arbitrary_local_file_paths() -> bool:
    return os.getenv("DATABRICKS_MCP_ALLOW_ARBITRARY_LOCAL_FILE_PATHS", "").strip().lower() in _TRUTHY_ENV_VALUES


def _validate_local_file_path(file_path: str) -> str:
    expanded_path = pathlib.Path(file_path).expanduser()
    if _allow_arbitrary_local_file_paths():
        return str(expanded_path)

    cwd = pathlib.Path.cwd().resolve()
    resolved_path = expanded_path.resolve(strict=False)
    try:
        resolved_path.relative_to(cwd)
    except ValueError as exc:
        raise ValueError(
            "file_path must stay within the current working directory unless "
            "DATABRICKS_MCP_ALLOW_ARBITRARY_LOCAL_FILE_PATHS=1 is set."
        ) from exc
    return str(expanded_path)


def _read_local_source_file(file_path: str) -> tuple[str, str]:
    validated_file_path = _validate_local_file_path(file_path)
    with open(validated_file_path, "r", encoding="utf-8") as source_file:
        return source_file.read(), pathlib.Path(validated_file_path).suffix.lower()


def _normalize_execute_code_response(
    *,
    result: dict[str, Any],
    requested_compute_type: str,
    resolved_compute_type: str,
    language: str,
) -> dict[str, Any]:
    return {
        "success": result.get("success", False),
        "error": result.get("error"),
        "message": result.get("message"),
        "output": result.get("output"),
        "output_kind": result.get("output_kind", "text" if result.get("output") is not None else "none"),
        "language": language,
        "compute_type_requested": requested_compute_type,
        "compute_type_resolved": resolved_compute_type,
        "run_id": result.get("run_id"),
        "run_url": result.get("run_url"),
        "duration_seconds": result.get("duration_seconds"),
        "state": result.get("state"),
        "workspace_path": result.get("workspace_path"),
        "notebook_path": result.get("notebook_path"),
        "cluster_id": result.get("cluster_id"),
        "context_id": result.get("context_id"),
        "context_destroyed": result.get("context_destroyed"),
        "error_type": result.get("error_type"),
        "available_clusters": result.get("available_clusters"),
        "startable_clusters": result.get("startable_clusters"),
        "skipped_clusters": result.get("skipped_clusters"),
        "suggestions": result.get("suggestions"),
    }

def _fmt_ts(ms: int | None) -> str:
    """Convert epoch milliseconds to a human-readable UTC string."""
    if ms is None:
        return "—"
    return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


def _build_run_output_preview(run_output: Any) -> tuple[str | None, str]:
    notebook_output = getattr(run_output, "notebook_output", None)
    notebook_result = getattr(notebook_output, "result", None)
    logs = getattr(run_output, "logs", None)

    if notebook_result is not None and logs is not None:
        return f"{notebook_result}\n\n--- Logs ---\n{logs}", "notebook_result+logs"
    if notebook_result is not None:
        return notebook_result, "notebook_result"
    if logs is not None:
        return logs, "logs"
    return None, "none"


def _parse_views_to_export(value: str) -> ViewsToExport:
    normalized_value = value.strip().upper()
    if not normalized_value:
        return ViewsToExport.CODE

    try:
        return ViewsToExport(normalized_value)
    except ValueError as exc:
        valid_values = ", ".join(view.value for view in ViewsToExport)
        raise ValueError(f"views_to_export must be one of: {valid_values}.") from exc


def _truncate_content(content: str | None, max_characters: int | None) -> tuple[str | None, bool]:
    if content is None or max_characters is None:
        return content, False
    if max_characters <= 0:
        raise ValueError("max_view_characters must be a positive integer when provided.")
    if len(content) <= max_characters:
        return content, False
    return content[:max_characters], True


def _resolve_run_output_target(
    *,
    client: Any,
    run_id: int,
    task_key: str | None,
) -> tuple[int, str | None]:
    run = client.jobs.get_run(run_id)
    tasks = run.tasks or []

    if task_key:
        for task in tasks:
            if task.task_key == task_key:
                return task.run_id or run_id, task.task_key
        available = [task.task_key for task in tasks if task.task_key]
        raise ValueError(
            f"Run {run_id} does not have task_key={task_key!r}. Available task keys: {available or ['<none>']}."
        )

    if len(tasks) > 1:
        available = [task.task_key for task in tasks if task.task_key]
        raise ValueError(
            f"Run {run_id} contains multiple tasks. Pass task_key to select one. Available task keys: {available}."
        )

    if len(tasks) == 1:
        task = tasks[0]
        return task.run_id or run_id, task.task_key

    return run_id, None


# ─── Jobs ────────────────────────────────────────────────────────────────────


@mcp.tool()
def execute_code(
    code: str | None = None,
    file_path: str | None = None,
    compute_type: str = "auto",
    language: str = "python",
    timeout: int | None = None,
    workspace_path: str | None = None,
    run_name: str | None = None,
    cluster_id: str | None = None,
    context_id: str | None = None,
    destroy_context_on_completion: bool = False,
    profile: str = "",
) -> dict[str, Any]:
    """
    Execute code on Databricks compute (serverless or interactive cluster).

    Routing:
      - "serverless"  -> Databricks serverless workflows (Jobs API, notebooks)
      - "cluster"     -> interactive cluster via Command Execution API
      - "auto"        -> serverless for Python/SQL, cluster for Scala/R

    Use this tool for snippets, one-off commands, and REPL-style cluster iteration.
    For notebook authoring, reruns, or reviewing rendered notebook output, prefer
    `execute_notebook` so the run always goes through Jobs and can be inspected with
    `get_job_run_output` / `get_job_run_export`.

    Cluster execution supports Python, SQL, Scala, and R and can reuse a
    returned context_id with the same cluster_id to preserve state across calls.

    Serverless execution creates a Databricks Jobs run. The immediate `output`
    field is a convenience summary only:
      - notebook result text when Databricks captures notebook_output.result
      - stdout/stderr logs when Databricks captures logs
      - both combined when both are available
      - no rich notebook rendering payload beyond what Databricks exposes in run output

    For serverless runs, always retain the returned `run_id`. An agent can call
    `get_job_run_output(run_id=...)` after `execute_code` to inspect the full
    run output, review task-level flow, and fetch logs or notebook result text
    again. For multi-task runs, use `get_job_run` first to discover `task_key`
    or `task_run_id`, then call `get_job_run_output(run_id=..., task_key=...)`.
    When richer rendered notebook views are needed, call
    `get_job_run_export(run_id=...)` to retrieve the HTML export that Databricks
    produces for the run.

    Args:
        code: Source code to execute remotely.
        file_path: Optional local file path (.py, .sql, .scala, .r). By default it must
                   resolve under the current working directory unless
                   DATABRICKS_MCP_ALLOW_ARBITRARY_LOCAL_FILE_PATHS=1 is set.
        compute_type: "auto", "serverless", or "cluster".
        language: Execution language for inline code.
        timeout: Optional run timeout in seconds.
        workspace_path: Optional Databricks workspace path to persist the notebook.
                        Valid only for serverless execution.
        run_name: Optional Jobs run name for serverless execution.
        cluster_id: Optional interactive cluster ID for cluster execution. Required when
                    reusing context_id.
        context_id: Optional existing execution context to reuse on a cluster. Requires
                    the original cluster_id.
        destroy_context_on_completion: Destroy the execution context after a cluster run.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        A normalized result with stable top-level fields for both backends:
        - success: Whether execution completed successfully.
        - error: Error text when execution fails.
        - message: Human-readable summary.
        - output: Captured text output. For serverless runs this may include
          notebook result text and/or logs. For cluster runs this is the command result.
        - output_kind: "text" when output text is present, otherwise "none".
        - language: Resolved execution language.
        - compute_type_requested / compute_type_resolved: Requested vs actual backend.
        - run_id / run_url / duration_seconds / state / workspace_path: Serverless run metadata.
        - cluster_id / context_id / context_destroyed: Cluster execution metadata.
        - error_type / available_clusters / startable_clusters / skipped_clusters / suggestions:
          Structured cluster-routing diagnostics when applicable.
    """
    code = _none_if_empty(code)
    file_path = _normalize_optional_string(file_path)
    compute_type = (_normalize_optional_string(compute_type) or "auto").lower()
    requested_compute_type = compute_type
    language = (_normalize_optional_string(language) or "python").lower()
    workspace_path = _normalize_optional_string(workspace_path)
    run_name = _normalize_optional_string(run_name)
    cluster_id = _normalize_optional_string(cluster_id)
    context_id = _normalize_optional_string(context_id)

    if not code and not file_path:
        return {"success": False, "error": "Either 'code' or 'file_path' must be provided."}

    if compute_type not in {"auto", "serverless", "cluster"}:
        return {
            "success": False,
            "error": (
                f"compute_type={compute_type!r} is not valid. "
                "Must be 'auto', 'serverless', or 'cluster'."
            ),
        }

    if file_path:
        try:
            code, suffix = _read_local_source_file(file_path)
        except ValueError as exc:
            return {"success": False, "error": str(exc)}
        except FileNotFoundError:
            return {"success": False, "error": f"File not found: {file_path}"}
        except Exception as exc:
            return {"success": False, "error": f"Failed to read file: {exc}"}
        if suffix == ".ipynb":
            return {
                "success": False,
                "error": "execute_code does not support .ipynb notebooks. Use execute_notebook instead.",
            }
        detected_language = _FILE_EXT_LANGUAGE.get(suffix)
        if detected_language:
            language = detected_language

    if compute_type == "auto" and language in ("scala", "r"):
        compute_type = "cluster"

    cluster_only_args_used = cluster_id is not None or context_id is not None or destroy_context_on_completion
    serverless_only_args_used = workspace_path is not None or run_name is not None

    if compute_type in ("auto", "serverless") and cluster_only_args_used:
        return {
            "success": False,
            "error": (
                "cluster_id, context_id, and destroy_context_on_completion are only valid "
                "when compute_type resolves to 'cluster'. Use compute_type='cluster' to target a cluster."
            ),
        }

    if compute_type == "cluster" and serverless_only_args_used:
        return {
            "success": False,
            "error": (
                "workspace_path and run_name are only valid for serverless execution. "
                "Remove them or use compute_type='serverless'."
            ),
        }

    if compute_type == "cluster" and context_id is not None and cluster_id is None:
        return {
            "success": False,
            "error": "cluster_id is required when reusing context_id for cluster execution.",
        }

    if compute_type in ("auto", "serverless"):
        resolved_timeout = timeout if timeout is not None else 1800
        try:
            result = run_code_on_serverless(
                code=code or "",
                profile=profile,
                language=language,
                timeout=resolved_timeout,
                run_name=run_name,
                cleanup=workspace_path is None,
                workspace_path=workspace_path,
            )
            return _normalize_execute_code_response(
                result=result.to_dict(),
                requested_compute_type=requested_compute_type,
                resolved_compute_type="serverless",
                language=language,
            )
        except (ValueError, DatabricksError) as exc:
            return _normalize_execute_code_response(
                result={"success": False, "error": str(exc), "state": "FAILED"},
                requested_compute_type=requested_compute_type,
                resolved_compute_type="serverless",
                language=language,
            )

    resolved_timeout = timeout if timeout is not None else 120
    try:
        result = run_code_on_cluster(
            code=code or "",
            profile=profile,
            cluster_id=cluster_id,
            context_id=context_id,
            language=language,
            timeout=resolved_timeout,
            destroy_context_on_completion=destroy_context_on_completion,
        )
        return _normalize_execute_code_response(
            result=result.to_dict(),
            requested_compute_type=requested_compute_type,
            resolved_compute_type="cluster",
            language=language,
        )
    except NoRunningClusterError as exc:
        return _normalize_execute_code_response(
            result=exc.to_dict(),
            requested_compute_type=requested_compute_type,
            resolved_compute_type="cluster",
            language=language,
        )
    except (ValueError, DatabricksError) as exc:
        return _normalize_execute_code_response(
            result={"success": False, "error": str(exc), "state": "FAILED"},
            requested_compute_type=requested_compute_type,
            resolved_compute_type="cluster",
            language=language,
        )


@mcp.tool()
def execute_notebook(
    code: str | None = None,
    file_path: str | None = None,
    notebook_path: str | None = None,
    compute_type: str = "serverless",
    language: str = "python",
    timeout: int | None = None,
    run_name: str | None = None,
    cluster_id: str | None = None,
    profile: str = "",
    notebook_parameters: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Execute a Databricks notebook through the Jobs API on serverless or an existing cluster.

    This is the preferred tool for notebook development and iteration. It supports:
      - running an existing notebook at `notebook_path`
      - uploading notebook content from `code` or `file_path`, then running it
      - inspecting the resulting run with `get_job_run_output` and `get_job_run_export`

    Unlike cluster-based `execute_code`, this tool does not use the Command Execution API
    and does not return or reuse `context_id`. Every run is a Jobs notebook task with a
    stable `run_id`.

    Args:
        code: Optional notebook source or raw .ipynb JSON content to upload and run.
        file_path: Optional local file path (.py, .sql, .ipynb, .scala, .r) to upload and run.
                   By default it must resolve under the current working directory unless
                   DATABRICKS_MCP_ALLOW_ARBITRARY_LOCAL_FILE_PATHS=1 is set.
        notebook_path: Optional existing Databricks workspace notebook path to run, or the
                       destination path to overwrite when `code` / `file_path` is supplied.
        compute_type: "serverless" or "cluster".
        language: Execution language for inline source content.
        timeout: Optional run timeout in seconds.
        run_name: Optional Jobs run name.
        cluster_id: Required when compute_type="cluster". Existing cluster to use.
        profile: Optional Databricks profile name for workspace selection.
        notebook_parameters: Optional base parameters passed to the notebook task.

    Returns:
        A normalized Jobs-backed notebook run result with:
        - success, error, message, output, output_kind
        - language, compute_type_requested, compute_type_resolved
        - run_id, run_url, duration_seconds, state
        - notebook_path
        - cluster_id (for cluster-backed notebook runs)
    """
    code = _none_if_empty(code)
    file_path = _normalize_optional_string(file_path)
    notebook_path = _normalize_optional_string(notebook_path)
    compute_type = (_normalize_optional_string(compute_type) or "serverless").lower()
    requested_compute_type = compute_type
    language = (_normalize_optional_string(language) or "python").lower()
    run_name = _normalize_optional_string(run_name)
    cluster_id = _normalize_optional_string(cluster_id)

    if not code and not file_path and not notebook_path:
        return {
            "success": False,
            "error": "Provide notebook_path to run an existing notebook, or code/file_path to upload and run a notebook.",
        }

    if compute_type not in {"serverless", "cluster"}:
        return {
            "success": False,
            "error": f"compute_type={compute_type!r} is not valid. Must be 'serverless' or 'cluster'.",
        }

    if file_path:
        try:
            code, suffix = _read_local_source_file(file_path)
        except ValueError as exc:
            return {"success": False, "error": str(exc)}
        except FileNotFoundError:
            return {"success": False, "error": f"File not found: {file_path}"}
        except Exception as exc:
            return {"success": False, "error": f"Failed to read file: {exc}"}
        detected_language = _FILE_EXT_LANGUAGE.get(suffix)
        if detected_language:
            language = detected_language

    if compute_type == "cluster" and not cluster_id:
        return {
            "success": False,
            "error": "cluster_id is required when compute_type='cluster' for execute_notebook.",
        }

    resolved_timeout = timeout if timeout is not None else 1800
    try:
        result = run_notebook_job(
            profile=profile,
            compute_type=compute_type,
            notebook_path=notebook_path,
            code=code,
            language=language,
            timeout=resolved_timeout,
            run_name=run_name,
            cluster_id=cluster_id,
            notebook_parameters=notebook_parameters,
        )
        return _normalize_execute_code_response(
            result=result.to_dict(),
            requested_compute_type=requested_compute_type,
            resolved_compute_type=compute_type,
            language=language,
        )
    except (ValueError, DatabricksError) as exc:
        return _normalize_execute_code_response(
            result={"success": False, "error": str(exc), "state": "FAILED"},
            requested_compute_type=requested_compute_type,
            resolved_compute_type=compute_type,
            language=language,
        )


@mcp.tool()
def query_sql(
    query: str,
    catalog: str = "",
    schema: str = "",
    profile: str = "",
    poll_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """
    Execute a single read-only SQL query against the configured Databricks SQL warehouse.
    Prefer Unity Catalog metadata tools for catalog/schema/table/column discovery.
    Use this tool for row-level data retrieval when a SQL result set is required.

    Args:
        query: A single SELECT statement. WITH CTEs are supported.
        catalog: Optional default catalog for statement execution.
        schema: Optional default schema for statement execution.
        profile: Optional Databricks profile name. When set, the tool reads
                 DATABRICKS_PROFILE_<PROFILE>_* values from .env and uses the
                 same profile for SDK authentication.
        poll_timeout_seconds: Optional per-request override for query polling.
                 Must be a positive integer when set.

    Returns:
        JSON-digestible query results or a JSON-digestible error payload.
    """
    try:
        warehouse_id = _get_warehouse_id(profile)
        resolved_poll_timeout_seconds = _parse_positive_poll_timeout_override(poll_timeout_seconds)
        if resolved_poll_timeout_seconds is None:
            resolved_poll_timeout_seconds = _get_sql_poll_timeout_seconds(profile)
        client = _get_client(profile)
        return execute_safe_query(
            warehouse_id=warehouse_id,
            client=client,
            query=query,
            catalog=catalog or None,
            schema=schema or None,
            poll_timeout_seconds=resolved_poll_timeout_seconds,
        )
    except QueryValidationError as exc:
        return exc.as_dict()
    except ValueError as exc:
        return {
            "status": "error",
            "error_type": "configuration_error",
            "message": str(exc),
        }
    except DatabricksError as exc:
        return {
            "status": "error",
            "error_type": "databricks_api_error",
            "message": str(exc),
        }


@mcp.tool()
def list_jobs(name_filter: str = "", profile: str = "") -> list[dict[str, Any]]:
    """
    List all Databricks jobs in the workspace.

    Args:
        name_filter: Optional substring to filter jobs by name (case-insensitive).
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of jobs with id, name, creator, created_time, and schedule info.
    """
    client = _get_client(profile)
    results = []
    for job in client.jobs.list(expand_tasks=False):
        name = job.settings.name or ""
        if name_filter and name_filter.lower() not in name.lower():
            continue
        schedule = None
        if job.settings.schedule:
            schedule = {
                "quartz_cron": job.settings.schedule.quartz_cron_expression,
                "timezone": job.settings.schedule.timezone_id,
                "paused": str(job.settings.schedule.pause_status),
            }
        results.append(
            {
                "job_id": job.job_id,
                "name": name,
                "creator": job.creator_user_name,
                "created_time": _fmt_ts(job.created_time),
                "schedule": schedule,
            }
        )
    return results


@mcp.tool()
def get_job(job_id: int, profile: str = "") -> dict[str, Any]:
    """
    Get detailed information about a specific Databricks job.

    Args:
        job_id: The numeric job ID.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Full job configuration including tasks, clusters, schedule, and parameters.
    """
    client = _get_client(profile)
    job = client.jobs.get(job_id)
    settings = job.settings

    tasks = []
    for t in settings.tasks or []:
        task_info: dict[str, Any] = {"task_key": t.task_key}
        if t.notebook_task:
            task_info["type"] = "notebook"
            task_info["notebook_path"] = t.notebook_task.notebook_path
        elif t.python_wheel_task:
            task_info["type"] = "python_wheel"
            task_info["package"] = t.python_wheel_task.package_name
            task_info["entry_point"] = t.python_wheel_task.entry_point
        elif t.spark_python_task:
            task_info["type"] = "spark_python"
            task_info["python_file"] = t.spark_python_task.python_file
        elif t.pipeline_task:
            task_info["type"] = "pipeline"
            task_info["pipeline_id"] = t.pipeline_task.pipeline_id
        elif t.dbt_task:
            task_info["type"] = "dbt"
            task_info["commands"] = t.dbt_task.commands
        else:
            task_info["type"] = "other"
        if t.depends_on:
            task_info["depends_on"] = [d.task_key for d in t.depends_on]
        tasks.append(task_info)

    return {
        "job_id": job.job_id,
        "name": settings.name,
        "creator": job.creator_user_name,
        "created_time": _fmt_ts(job.created_time),
        "tasks": tasks,
        "max_concurrent_runs": settings.max_concurrent_runs,
        "tags": settings.tags or {},
    }


@mcp.tool()
def list_job_runs(
    job_id: int,
    limit: int = 20,
    active_only: bool = False,
    profile: str = "",
) -> list[dict[str, Any]]:
    """
    List recent runs for a Databricks job.

    Args:
        job_id: The numeric job ID.
        limit: Maximum number of runs to return (default 20, max 100).
        active_only: If True, only return currently active/running runs.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of runs with run_id, state, result, start/end times, and duration.
    """
    client = _get_client(profile)
    limit = min(limit, 100)
    results = []
    for run in client.jobs.list_runs(job_id=job_id, active_only=active_only, limit=limit):
        state = run.state
        lifecycle = state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN"
        result = state.result_state.value if state and state.result_state else "—"
        duration_s = None
        if run.start_time and run.end_time:
            duration_s = round((run.end_time - run.start_time) / 1000)

        results.append(
            {
                "run_id": run.run_id,
                "run_name": run.run_name,
                "trigger": str(run.trigger) if run.trigger else "MANUAL",
                "lifecycle_state": lifecycle,
                "result_state": result,
                "start_time": _fmt_ts(run.start_time),
                "end_time": _fmt_ts(run.end_time),
                "duration_seconds": duration_s,
                "run_page_url": run.run_page_url,
            }
        )
    return results


@mcp.tool()
def get_job_run(run_id: int, profile: str = "") -> dict[str, Any]:
    """
    Get detailed information about a specific job run, including per-task status.

    Args:
        run_id: The numeric run ID.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Run details with overall state, task states, error messages, and URLs.
    """
    client = _get_client(profile)
    run = client.jobs.get_run(run_id)
    state = run.state

    task_details = []
    for t in run.tasks or []:
        ts = t.state
        t_lifecycle = ts.life_cycle_state.value if ts and ts.life_cycle_state else "UNKNOWN"
        t_result = ts.result_state.value if ts and ts.result_state else "—"
        t_msg = ts.state_message if ts else None
        task_details.append(
            {
                "task_key": t.task_key,
                "task_run_id": t.run_id,
                "lifecycle_state": t_lifecycle,
                "result_state": t_result,
                "state_message": t_msg,
                "start_time": _fmt_ts(t.start_time),
                "end_time": _fmt_ts(t.end_time),
                "run_page_url": t.run_page_url,
            }
        )

    return {
        "run_id": run.run_id,
        "job_id": run.job_id,
        "run_name": run.run_name,
        "trigger": str(run.trigger) if run.trigger else "MANUAL",
        "lifecycle_state": state.life_cycle_state.value if state and state.life_cycle_state else "UNKNOWN",
        "result_state": state.result_state.value if state and state.result_state else "—",
        "state_message": state.state_message if state else None,
        "start_time": _fmt_ts(run.start_time),
        "end_time": _fmt_ts(run.end_time),
        "run_page_url": run.run_page_url,
        "tasks": task_details,
    }


@mcp.tool()
def get_job_run_output(run_id: int, task_key: str = "", profile: str = "") -> dict[str, Any]:
    """
    Get notebook result text, stdout/stderr logs, and error details for a Databricks job run.

    For single-task runs, including runs created by `execute_code` on serverless compute,
    the task run is resolved automatically from the parent run_id. For multi-task jobs,
    pass task_key to choose which task's output to inspect.

    Args:
        run_id: The numeric parent run ID or task run ID.
        task_key: Optional task key for multi-task runs.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Run output details including notebook_output, logs, error, error_trace, and a
        convenience `output` field that combines notebook result text and logs.
    """
    client = _get_client(profile)
    normalized_task_key = _none_if_empty(task_key)
    if normalized_task_key is not None:
        normalized_task_key = normalized_task_key.strip()
    resolved_run_id, resolved_task_key = _resolve_run_output_target(
        client=client,
        run_id=run_id,
        task_key=normalized_task_key,
    )
    run_output = client.jobs.get_run_output(run_id=resolved_run_id)
    payload = run_output.as_dict() if hasattr(run_output, "as_dict") else {}
    notebook_output = payload.get("notebook_output") or {}
    output_preview, output_kind = _build_run_output_preview(run_output)

    return {
        "requested_run_id": run_id,
        "resolved_run_id": resolved_run_id,
        "task_key": resolved_task_key,
        "task_run_id": resolved_run_id,
        "output": output_preview,
        "output_kind": output_kind,
        "notebook_output_result": notebook_output.get("result"),
        **payload,
    }


@mcp.tool()
def get_job_run_export(
    run_id: int,
    task_key: str = "",
    views_to_export: str = "CODE",
    max_view_characters: int | None = 50000,
    profile: str = "",
) -> dict[str, Any]:
    """
    Export a Databricks job run as HTML views for notebook iteration and richer rendering review.

    For single-task runs, including runs created by `execute_code` on serverless compute,
    the task run is resolved automatically from the parent run_id. For multi-task jobs,
    pass task_key to choose which task's exported notebook view to retrieve.

    Args:
        run_id: The numeric parent run ID or task run ID.
        task_key: Optional task key for multi-task runs.
        views_to_export: Which views to export: "CODE", "DASHBOARDS", or "ALL".
        max_view_characters: Optional per-view content limit to keep responses manageable.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Export metadata plus HTML view items. Each view includes name, type, content,
        content_length, and whether the content was truncated in the MCP response.
    """
    client = _get_client(profile)
    normalized_task_key = _none_if_empty(task_key)
    if normalized_task_key is not None:
        normalized_task_key = normalized_task_key.strip()
    resolved_run_id, resolved_task_key = _resolve_run_output_target(
        client=client,
        run_id=run_id,
        task_key=normalized_task_key,
    )
    export_view = _parse_views_to_export(views_to_export)
    export_output = client.jobs.export_run(run_id=resolved_run_id, views_to_export=export_view)

    views = []
    html_view_count = 0
    for view in export_output.views or []:
        content, truncated = _truncate_content(getattr(view, "content", None), max_view_characters)
        original_content = getattr(view, "content", None)
        if original_content is not None:
            html_view_count += 1
        views.append(
            {
                "name": getattr(view, "name", None),
                "type": getattr(getattr(view, "type", None), "value", None),
                "content": content,
                "content_length": len(original_content) if original_content is not None else 0,
                "content_truncated": truncated,
            }
        )

    return {
        "requested_run_id": run_id,
        "resolved_run_id": resolved_run_id,
        "task_key": resolved_task_key,
        "task_run_id": resolved_run_id,
        "views_to_export": export_view.value,
        "view_count": len(views),
        "html_view_count": html_view_count,
        "views": views,
    }


@mcp.tool()
def cancel_job_run(run_id: int, profile: str = "") -> dict[str, str]:
    """
    Cancel an active Databricks job run.

    Args:
        run_id: The numeric run ID to cancel.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Confirmation message.
    """
    client = _get_client(profile)
    client.jobs.cancel_run(run_id)
    return {"status": "cancel requested", "run_id": str(run_id)}


@mcp.tool()
def run_job(job_id: int, profile: str = "") -> dict[str, Any]:
    """
    Trigger a new run of a Databricks job.

    Args:
        job_id: The numeric job ID to run.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        The new run_id and a link to the run page.
    """
    client = _get_client(profile)
    response = client.jobs.run_now(job_id=job_id)
    run = response.result()
    return {
        "job_id": job_id,
        "run_id": run.run_id,
        "run_page_url": run.run_page_url,
    }


# ─── Delta Live Tables (Pipelines) ───────────────────────────────────────────


@mcp.tool()
def list_pipelines(name_filter: str = "", profile: str = "") -> list[dict[str, Any]]:
    """
    List all Delta Live Tables (DLT) pipelines in the workspace.

    Args:
        name_filter: Optional substring to filter pipelines by name (case-insensitive).
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of pipelines with id, name, state, creator, and cluster spec summary.
    """
    client = _get_client(profile)
    results = []
    for p in client.pipelines.list_pipelines():
        name = p.name or ""
        if name_filter and name_filter.lower() not in name.lower():
            continue
        results.append(
            {
                "pipeline_id": p.pipeline_id,
                "name": name,
                "state": p.state.value if p.state else "UNKNOWN",
                "creator": p.creator_user_name,
                "run_as_user": p.run_as_user_name,
                "continuous": p.continuous if hasattr(p, "continuous") else None,
            }
        )
    return results


@mcp.tool()
def get_pipeline(pipeline_id: str, profile: str = "") -> dict[str, Any]:
    """
    Get detailed information about a Delta Live Tables pipeline.

    Args:
        pipeline_id: The pipeline UUID.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Pipeline configuration including state, clusters, libraries, and last update.
    """
    client = _get_client(profile)
    p = client.pipelines.get(pipeline_id)
    spec = p.spec or {}

    libraries = []
    if hasattr(spec, "libraries") and spec.libraries:
        for lib in spec.libraries:
            if lib.notebook:
                libraries.append({"type": "notebook", "path": lib.notebook.path})
            elif lib.file:
                libraries.append({"type": "file", "path": lib.file.path})

    clusters = []
    if hasattr(spec, "clusters") and spec.clusters:
        for c in spec.clusters:
            clusters.append(
                {
                    "label": c.label,
                    "node_type": c.node_type_id,
                    "autoscale": (
                        {"min": c.autoscale.min_workers, "max": c.autoscale.max_workers}
                        if c.autoscale
                        else None
                    ),
                }
            )

    return {
        "pipeline_id": p.pipeline_id,
        "name": p.name,
        "state": p.state.value if p.state else "UNKNOWN",
        "continuous": spec.continuous if hasattr(spec, "continuous") else False,
        "development": spec.development if hasattr(spec, "development") else False,
        "catalog": spec.catalog if hasattr(spec, "catalog") else None,
        "target_schema": spec.target if hasattr(spec, "target") else None,
        "creator": p.creator_user_name,
        "libraries": libraries,
        "clusters": clusters,
        "latest_updates": [
            {
                "update_id": u.update_id,
                "state": u.state.value if u.state else "UNKNOWN",
                "creation_time": _fmt_ts(u.creation_time),
            }
            for u in (p.latest_updates or [])[:5]
        ],
    }


@mcp.tool()
def list_pipeline_updates(
    pipeline_id: str,
    limit: int = 10,
    profile: str = "",
) -> list[dict[str, Any]]:
    """
    List recent update runs for a Delta Live Tables pipeline.

    Args:
        pipeline_id: The pipeline UUID.
        limit: Maximum number of updates to return (default 10).
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of pipeline updates with update_id, state, cause, and timing.
    """
    client = _get_client(profile)
    response = client.pipelines.list_updates(pipeline_id=pipeline_id, max_results=limit)
    results = []
    for u in response.updates or []:
        results.append(
            {
                "update_id": u.update_id,
                "state": u.state.value if u.state else "UNKNOWN",
                "cause": u.cause,
                "full_refresh": u.full_refresh,
                "creation_time": _fmt_ts(u.creation_time),
            }
        )
    return results


@mcp.tool()
def get_pipeline_update(pipeline_id: str, update_id: str, profile: str = "") -> dict[str, Any]:
    """
    Get details about a specific Delta Live Tables pipeline update, including events/errors.

    Args:
        pipeline_id: The pipeline UUID.
        update_id: The update UUID.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Update state and the most recent pipeline events (errors, warnings, info).
    """
    client = _get_client(profile)
    update = client.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
    u = update.update

    # Fetch recent events filtered to this update
    events = []
    for event in client.pipelines.list_pipeline_events(
        pipeline_id=pipeline_id,
        filter=f"update_id='{update_id}'",
        max_results=50,
    ):
        entry: dict[str, Any] = {
            "timestamp": event.timestamp,
            "level": event.level.value if event.level else "INFO",
            "event_type": event.event_type,
            "message": event.message,
        }
        if event.error:
            entry["error"] = {
                "fatal": event.error.fatal,
                "exceptions": [
                    {"class": ex.class_name, "message": ex.message}
                    for ex in (event.error.exceptions or [])
                ],
            }
        events.append(entry)

    return {
        "update_id": u.update_id if u else update_id,
        "state": u.state.value if u and u.state else "UNKNOWN",
        "cause": u.cause if u else None,
        "full_refresh": u.full_refresh if u else None,
        "creation_time": _fmt_ts(u.creation_time) if u else None,
        "recent_events": events[:20],
    }


@mcp.tool()
def start_pipeline_update(
    pipeline_id: str,
    full_refresh: bool = False,
    profile: str = "",
) -> dict[str, str]:
    """
    Trigger a new update (run) for a Delta Live Tables pipeline.

    Args:
        pipeline_id: The pipeline UUID.
        full_refresh: If True, recompute all data from scratch (default False).
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        The new update_id.
    """
    client = _get_client(profile)
    response = client.pipelines.start_update(
        pipeline_id=pipeline_id,
        full_refresh=full_refresh,
    )
    return {"pipeline_id": pipeline_id, "update_id": response.update_id}


# ─── Unity Catalog ───────────────────────────────────────────────────────────


@mcp.tool()
def list_catalogs(profile: str = "") -> list[dict[str, Any]]:
    """
    List all Unity Catalog catalogs in the workspace.

    Args:
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of catalogs with name, type, owner, and comment.
    """
    client = _get_client(profile)
    return [
        {
            "name": c.name,
            "catalog_type": str(c.catalog_type) if c.catalog_type else None,
            "owner": c.owner,
            "comment": c.comment,
        }
        for c in client.catalogs.list()
    ]


@mcp.tool()
def list_schemas(catalog_name: str, profile: str = "") -> list[dict[str, Any]]:
    """
    List all schemas (databases) inside a Unity Catalog catalog.

    Args:
        catalog_name: Name of the catalog (e.g. "main", "hive_metastore").
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of schemas with name, owner, and full_name.
    """
    client = _get_client(profile)
    return [
        {
            "name": s.name,
            "full_name": s.full_name,
            "owner": s.owner,
            "comment": s.comment,
        }
        for s in client.schemas.list(catalog_name=catalog_name)
    ]


@mcp.tool()
def list_tables(
    catalog_name: str,
    schema_name: str,
    name_filter: str = "",
    profile: str = "",
) -> list[dict[str, Any]]:
    """
    List all tables and views in a schema.
    Prefer this over query_sql for metadata discovery.

    Args:
        catalog_name: Catalog name (e.g. "main").
        schema_name: Schema name (e.g. "default").
        name_filter: Optional substring to filter table names (case-insensitive).
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of tables with full_name, table_type, owner, and column count.
    """
    client = _get_client(profile)
    results = []
    for t in client.tables.list(catalog_name=catalog_name, schema_name=schema_name):
        if name_filter and name_filter.lower() not in (t.name or "").lower():
            continue
        results.append(
            {
                "name": t.name,
                "full_name": t.full_name,
                "table_type": str(t.table_type) if t.table_type else None,
                "data_source_format": str(t.data_source_format) if t.data_source_format else None,
                "owner": t.owner,
                "columns": len(t.columns) if t.columns else None,
                "comment": t.comment,
            }
        )
    return results


@mcp.tool()
def get_table(full_table_name: str, profile: str = "") -> dict[str, Any]:
    """
    Get full metadata for a table including all columns, types, and comments.
    Prefer this over query_sql when you need schema metadata.

    Args:
        full_table_name: Three-part name: catalog.schema.table
                         (e.g. "main.sales.orders").
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Table metadata with columns (name, type, nullable, comment), owner,
        storage location, and table properties.
    """
    client = _get_client(profile)
    t = client.tables.get(full_name=full_table_name)
    return {
        "name": t.name,
        "full_name": t.full_name,
        "table_type": str(t.table_type) if t.table_type else None,
        "data_source_format": str(t.data_source_format) if t.data_source_format else None,
        "owner": t.owner,
        "comment": t.comment,
        "storage_location": t.storage_location,
        "created_at": _fmt_ts(t.created_at),
        "updated_at": _fmt_ts(t.updated_at),
        "columns": [
            {
                "name": col.name,
                "type": col.type_text,
                "nullable": col.nullable,
                "comment": col.comment,
                "partition_index": col.partition_index,
            }
            for col in (t.columns or [])
        ],
        "properties": t.properties or {},
    }


@mcp.tool()
def search_tables(
    name_pattern: str,
    catalog_name: str = "",
    profile: str = "",
) -> list[dict[str, Any]]:
    """
    Search for tables whose name contains a given pattern, across all (or one) catalog.
    Use this to answer "how many tables start with xyz" or "find tables named like xyz".
    Prefer this over query_sql for table discovery.

    Args:
        name_pattern: Substring to match against table names (case-insensitive).
        catalog_name: Restrict search to this catalog. Searches all catalogs if empty.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        Matching tables with full_name, table_type, owner, and column count.
    """
    client = _get_client(profile)

    catalogs = (
        [type("C", (), {"name": catalog_name})]
        if catalog_name
        else list(client.catalogs.list())
    )

    matches = []
    for cat in catalogs:
        try:
            for schema in client.schemas.list(catalog_name=cat.name):
                try:
                    for t in client.tables.list(
                        catalog_name=cat.name, schema_name=schema.name
                    ):
                        if name_pattern.lower() in (t.name or "").lower():
                            matches.append(
                                {
                                    "name": t.name,
                                    "full_name": t.full_name,
                                    "table_type": str(t.table_type) if t.table_type else None,
                                    "owner": t.owner,
                                    "columns": len(t.columns) if t.columns else None,
                                }
                            )
                except Exception:
                    pass  # skip schemas with no access
        except Exception:
            pass  # skip catalogs with no access

    return matches


@mcp.tool()
def search_columns(
    column_name_pattern: str,
    catalog_name: str = "",
    schema_name: str = "",
    profile: str = "",
) -> list[dict[str, Any]]:
    """
    Find all tables that contain a column matching the given name pattern.
    Use this to answer "where can I find a column called xyz" or
    "which tables have a customer_id column".
    Prefer this over query_sql for column discovery.

    Args:
        column_name_pattern: Substring to match against column names (case-insensitive).
        catalog_name: Restrict search to this catalog. Searches all catalogs if empty.
        schema_name: Restrict search to this schema (requires catalog_name if set).
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of matches with table full_name, column name, type, and comment.
    """
    client = _get_client(profile)

    # Build the list of (catalog, schema) pairs to search
    search_scope: list[tuple[str, str]] = []
    if catalog_name and schema_name:
        search_scope = [(catalog_name, schema_name)]
    elif catalog_name:
        for s in client.schemas.list(catalog_name=catalog_name):
            search_scope.append((catalog_name, s.name))
    else:
        for cat in client.catalogs.list():
            try:
                for s in client.schemas.list(catalog_name=cat.name):
                    search_scope.append((cat.name, s.name))
            except Exception:
                pass

    matches = []
    for cat_name, sch_name in search_scope:
        try:
            for t in client.tables.list(catalog_name=cat_name, schema_name=sch_name):
                for col in t.columns or []:
                    if column_name_pattern.lower() in (col.name or "").lower():
                        matches.append(
                            {
                                "table": t.full_name,
                                "column": col.name,
                                "type": col.type_text,
                                "nullable": col.nullable,
                                "comment": col.comment,
                            }
                        )
        except Exception:
            pass  # skip tables with no access

    return matches


@mcp.tool()
def list_compute(
    include_terminated: bool = False,
    profile: str = "",
) -> list[dict[str, Any]]:
    """
    List user-created interactive Databricks clusters.

    Args:
        include_terminated: When True, also include terminated and error clusters.
        profile: Optional Databricks profile name for workspace selection.

    Returns:
        List of cluster summaries with cluster_id, cluster_name, state, and creator.
    """
    return list_clusters(profile=profile, include_terminated=include_terminated)


@mcp.tool()
def manage_cluster(
    action: str,
    cluster_id: str,
    profile: str = "",
) -> dict[str, Any]:
    """
    Manage the lifecycle of an interactive Databricks cluster.

    Supported actions:
      - "status": return the current cluster state
      - "start": start a terminated cluster
    """
    action = (action or "").strip().lower()
    if action == "status":
        result = get_cluster_status(cluster_id, profile)
        return {"success": True, "error": None, **result}
    if action == "start":
        result = start_cluster(cluster_id, profile)
        return {"success": True, "error": None, **result}
    return {
        "success": False,
        "error": f"Unknown action {action!r}. Must be 'status' or 'start'.",
    }


def main():
    mcp.run()


if __name__ == "__main__":
    main()
