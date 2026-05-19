"""
Databricks MCP Server

Exposes Databricks Jobs and Delta Live Tables (Pipelines) as MCP tools.
"""

import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service.pipelines import PipelineState
from mcp.server.fastmcp import FastMCP

from databricks_mcp.sql_query import (
    QueryValidationError,
    DEFAULT_SQL_POLL_TIMEOUT_SECONDS,
    execute_safe_query,
)

load_dotenv()

mcp = FastMCP(
    "Databricks",
    instructions=(
        "Browse and inspect Databricks jobs, job runs, Delta Live Tables pipelines, "
        "pipeline updates, and read-only SQL queries. Use these tools to monitor "
        "workflow status, diagnose failures, inspect data, and retrieve run details."
    ),
)


@dataclass(frozen=True)
class WorkspaceConfig:
    host: str
    warehouse_id: str | None
    sql_poll_timeout_seconds: int
    profile: str | None


def _resolve_profile(profile: str = "") -> str | None:
    normalized_profile = profile.strip()
    if not normalized_profile:
        return None
    if not re.search(r"[A-Za-z0-9]", normalized_profile):
        raise ValueError("profile must contain at least one letter or number.")
    return normalized_profile


def _profile_env_prefix(profile: str) -> str:
    normalized_profile = re.sub(r"[^A-Za-z0-9]+", "_", profile).strip("_").upper()
    if not normalized_profile:
        raise ValueError("profile must contain at least one letter or number.")
    return f"DATABRICKS_PROFILE_{normalized_profile}"


def _get_env_value(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _parse_positive_int_env(name: str, value: str | None) -> int:
    if not value:
        return DEFAULT_SQL_POLL_TIMEOUT_SECONDS

    try:
        parsed_value = int(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be a positive integer number of seconds."
        ) from exc

    if parsed_value <= 0:
        raise ValueError(
            f"{name} must be a positive integer number of seconds."
        )
    return parsed_value


def _get_workspace_config(profile: str = "") -> WorkspaceConfig:
    resolved_profile = _resolve_profile(profile)
    if resolved_profile is None:
        host = _get_env_value("DATABRICKS_HOST")
        if not host:
            raise ValueError(
                "DATABRICKS_HOST must be set (e.g. https://adb-xxx.azuredatabricks.net). "
                "Authenticate with: databricks auth login --host <workspace-url>"
            )
        return WorkspaceConfig(
            host=host,
            warehouse_id=_get_env_value("DATABRICKS_WAREHOUSE_ID"),
            sql_poll_timeout_seconds=_parse_positive_int_env(
                "DATABRICKS_SQL_POLL_TIMEOUT_SECONDS",
                _get_env_value("DATABRICKS_SQL_POLL_TIMEOUT_SECONDS"),
            ),
            profile=None,
        )

    env_prefix = _profile_env_prefix(resolved_profile)
    host_key = f"{env_prefix}_HOST"
    warehouse_id_key = f"{env_prefix}_WAREHOUSE_ID"
    timeout_key = f"{env_prefix}_SQL_POLL_TIMEOUT_SECONDS"

    host = _get_env_value(host_key)
    if not host:
        raise ValueError(
            f"{host_key} must be set when profile='{resolved_profile}'."
        )

    timeout_value = _get_env_value(timeout_key)
    if timeout_value is None:
        timeout_key = "DATABRICKS_SQL_POLL_TIMEOUT_SECONDS"
        timeout_value = _get_env_value(timeout_key)

    return WorkspaceConfig(
        host=host,
        warehouse_id=_get_env_value(warehouse_id_key) or _get_env_value("DATABRICKS_WAREHOUSE_ID"),
        sql_poll_timeout_seconds=_parse_positive_int_env(timeout_key, timeout_value),
        profile=resolved_profile,
    )


def _get_client(profile: str = "") -> WorkspaceClient:
    # The SDK resolves credentials automatically in this order:
    #   1. DATABRICKS_HOST + DATABRICKS_TOKEN env vars (PAT fallback)
    #   2. OAuth U2M token stored by `databricks auth login` (~/.databrickscfg),
    #      optionally scoped by the provided profile name
    #   3. Azure CLI / GCP ADC / AWS instance profile (cloud environments)
    # For local development, just run:  databricks auth login --host <workspace-url>
    workspace_config = _get_workspace_config(profile)
    client_kwargs: dict[str, str] = {"host": workspace_config.host}
    if workspace_config.profile:
        client_kwargs["profile"] = workspace_config.profile
    return WorkspaceClient(**client_kwargs)


def _get_warehouse_id(profile: str = "") -> str:
    workspace_config = _get_workspace_config(profile)
    warehouse_id = workspace_config.warehouse_id
    if not warehouse_id:
        if workspace_config.profile:
            profile_warehouse_key = f"{_profile_env_prefix(workspace_config.profile)}_WAREHOUSE_ID"
            raise ValueError(
                f"{profile_warehouse_key} or DATABRICKS_WAREHOUSE_ID must be set to the serverless SQL warehouse to use for read-only queries."
            )
        raise ValueError(
            "DATABRICKS_WAREHOUSE_ID must be set to the serverless SQL warehouse to use for read-only queries."
        )
    return warehouse_id


def _get_sql_poll_timeout_seconds(profile: str = "") -> int:
    return _get_workspace_config(profile).sql_poll_timeout_seconds


def _parse_positive_poll_timeout_override(poll_timeout_seconds: int | None) -> int | None:
    if poll_timeout_seconds is None:
        return None
    if poll_timeout_seconds <= 0:
        raise ValueError("poll_timeout_seconds must be a positive integer number of seconds.")
    return poll_timeout_seconds


def _fmt_ts(ms: int | None) -> str:
    """Convert epoch milliseconds to a human-readable UTC string."""
    if ms is None:
        return "—"
    return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


# ─── Jobs ────────────────────────────────────────────────────────────────────


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


def main():
    mcp.run()


if __name__ == "__main__":
    main()
