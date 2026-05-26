import base64
import datetime
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any

from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, NotebookTask, RunResultState, SubmitTask
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks_mcp.workspace import get_client, get_current_username

logger = logging.getLogger(__name__)

_LANGUAGE_MAP = {
    "python": Language.PYTHON,
    "sql": Language.SQL,
    "scala": Language.SCALA,
    "r": Language.R,
}

_SERVERLESS_LANGUAGES = {"python", "sql"}


@dataclass
class NotebookJobRunResult:
    success: bool
    output: str | None = None
    output_kind: str = "none"
    error: str | None = None
    run_id: int | None = None
    run_url: str | None = None
    duration_seconds: float | None = None
    state: str | None = None
    message: str | None = None
    notebook_path: str | None = None
    cluster_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "success": self.success,
            "output": self.output,
            "output_kind": self.output_kind,
            "error": self.error,
            "run_id": self.run_id,
            "run_url": self.run_url,
            "duration_seconds": self.duration_seconds,
            "state": self.state,
            "message": self.message,
            "cluster_id": self.cluster_id,
        }
        if self.notebook_path:
            result["notebook_path"] = self.notebook_path
        return result


def get_temp_notebook_path(profile: str, run_label: str) -> str:
    username = get_current_username(profile)
    base = f"/Workspace/Users/{username}" if username else "/Workspace"
    return f"{base}/.databricks_mcp_tmp/{run_label}"


def is_ipynb(content: str) -> bool:
    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        return False
    return isinstance(payload, dict) and "cells" in payload


def upload_workspace_notebook(
    *,
    client: Any,
    code: str,
    language: str,
    workspace_path: str,
    is_jupyter: bool,
) -> None:
    content_b64 = base64.b64encode(code.encode("utf-8")).decode("utf-8")
    parent = workspace_path.rsplit("/", 1)[0]

    try:
        client.workspace.mkdirs(parent)
    except Exception:
        logger.debug("Workspace directory %s already exists or could not be created", parent)

    if is_jupyter:
        client.workspace.import_(
            path=workspace_path,
            content=content_b64,
            format=ImportFormat.JUPYTER,
            overwrite=True,
        )
        return

    client.workspace.import_(
        path=workspace_path,
        content=content_b64,
        language=_LANGUAGE_MAP[language],
        format=ImportFormat.SOURCE,
        overwrite=True,
    )


def cleanup_workspace_notebook(*, client: Any, workspace_path: str) -> None:
    try:
        client.workspace.delete(path=workspace_path, recursive=False)
    except Exception as exc:
        logger.debug("Cleanup of %s failed: %s", workspace_path, exc)


def get_run_output(*, client: Any, task_run_id: int) -> dict[str, str | None]:
    result: dict[str, str | None] = {"output": None, "error": None}

    try:
        run_output = client.jobs.get_run_output(run_id=task_run_id)
    except Exception as exc:
        logger.debug("Failed to fetch output for task run %s: %s", task_run_id, exc)
        return {"output": None, "error": str(exc)}

    if run_output.notebook_output and run_output.notebook_output.result is not None:
        result["output"] = run_output.notebook_output.result

    if run_output.logs is not None:
        if result["output"] is not None:
            result["output"] += f"\n\n--- Logs ---\n{run_output.logs}"
        else:
            result["output"] = run_output.logs

    if run_output.error:
        error_parts = [run_output.error]
        if run_output.error_trace:
            error_parts.append(run_output.error_trace)
        result["error"] = "\n\n".join(error_parts)

    return result


def run_notebook_job(
    *,
    profile: str = "",
    compute_type: str,
    notebook_path: str | None = None,
    code: str | None = None,
    language: str = "python",
    timeout: int = 1800,
    run_name: str | None = None,
    cleanup: bool = True,
    cluster_id: str | None = None,
    notebook_parameters: dict[str, str] | None = None,
    job_extra_params: dict[str, Any] | None = None,
) -> NotebookJobRunResult:
    normalized_compute_type = compute_type.strip().lower()
    if normalized_compute_type not in {"serverless", "cluster"}:
        return NotebookJobRunResult(
            success=False,
            error=f"Unsupported compute_type: {compute_type!r}. Must be 'serverless' or 'cluster'.",
            output_kind="none",
            state="INVALID_INPUT",
            message=f"Unsupported compute_type {compute_type!r}.",
            notebook_path=notebook_path,
            cluster_id=cluster_id,
        )

    if not notebook_path and not code:
        return NotebookJobRunResult(
            success=False,
            error="Either notebook_path or code must be provided.",
            output_kind="none",
            state="INVALID_INPUT",
            message="No notebook content or existing notebook path was provided.",
            cluster_id=cluster_id,
        )

    if code is not None and not code.strip():
        return NotebookJobRunResult(
            success=False,
            error="Code cannot be empty.",
            output_kind="none",
            state="INVALID_INPUT",
            message="No notebook content was provided to execute.",
            notebook_path=notebook_path,
            cluster_id=cluster_id,
        )

    is_jupyter_notebook = bool(code) and is_ipynb(code)
    language = language.strip().lower()

    if not is_jupyter_notebook and language not in _LANGUAGE_MAP:
        return NotebookJobRunResult(
            success=False,
            error=f"Unsupported language: {language!r}. Must be one of: {', '.join(_LANGUAGE_MAP)}.",
            output_kind="none",
            state="INVALID_INPUT",
            message=f"Unsupported language {language!r}.",
            notebook_path=notebook_path,
            cluster_id=cluster_id,
        )

    if normalized_compute_type == "serverless" and not is_jupyter_notebook and language not in _SERVERLESS_LANGUAGES:
        return NotebookJobRunResult(
            success=False,
            error=f"Unsupported language for serverless notebook execution: {language!r}. Use python, sql, or .ipynb content.",
            output_kind="none",
            state="INVALID_INPUT",
            message=f"Serverless notebook execution does not support language {language!r}.",
            notebook_path=notebook_path,
        )

    if normalized_compute_type == "cluster" and not cluster_id:
        return NotebookJobRunResult(
            success=False,
            error="cluster_id is required when compute_type='cluster' for notebook execution.",
            output_kind="none",
            state="INVALID_INPUT",
            message="Cluster-backed notebook execution requires an existing cluster_id.",
        )

    unique_id = uuid.uuid4().hex[:12]
    if not run_name:
        run_name = f"databricks_mcp_notebook_{normalized_compute_type}_{unique_id}"

    effective_notebook_path = notebook_path or get_temp_notebook_path(profile, f"notebook_{normalized_compute_type}_{unique_id}")
    should_upload = code is not None
    if notebook_path:
        cleanup = False
    elif not should_upload:
        cleanup = False

    client = get_client(profile)
    start_time = time.time()
    run_id: int | None = None
    run_url: str | None = None
    skip_cleanup = False

    try:
        if should_upload:
            try:
                upload_workspace_notebook(
                    client=client,
                    code=code or "",
                    language=language,
                    workspace_path=effective_notebook_path,
                    is_jupyter=is_jupyter_notebook,
                )
            except Exception as exc:
                return NotebookJobRunResult(
                    success=False,
                    error=f"Failed to upload notebook to workspace: {exc}",
                    output_kind="none",
                    state="UPLOAD_FAILED",
                    message=f"Could not upload notebook for execution: {exc}",
                    notebook_path=effective_notebook_path,
                    cluster_id=cluster_id,
                )

        try:
            extra = job_extra_params or {}
            notebook_task = NotebookTask(
                notebook_path=effective_notebook_path,
                base_parameters=notebook_parameters,
            )
            task_kwargs: dict[str, Any] = {
                "task_key": "main",
                "notebook_task": notebook_task,
            }
            submit_kwargs: dict[str, Any] = {
                "run_name": run_name,
                "tasks": [SubmitTask(**task_kwargs)],
            }

            if normalized_compute_type == "serverless":
                environment_key = "Default"
                if "environments" in extra and extra["environments"]:
                    environment_key = extra["environments"][0].get("environment_key", "Default")
                task_kwargs["environment_key"] = environment_key
                submit_kwargs["tasks"] = [SubmitTask(**task_kwargs)]
                if "environments" not in extra:
                    submit_kwargs["environments"] = [
                        JobEnvironment(
                            environment_key="Default",
                            spec=Environment(client="1"),
                        )
                    ]
            else:
                task_kwargs["existing_cluster_id"] = cluster_id
                submit_kwargs["tasks"] = [SubmitTask(**task_kwargs)]

            submit_kwargs.update(extra)
            wait = client.jobs.submit(**submit_kwargs)
            run_id = getattr(wait, "run_id", None)
            if run_id is None and hasattr(wait, "response"):
                run_id = getattr(wait.response, "run_id", None)

            if run_id:
                try:
                    run_url = client.jobs.get_run(run_id=run_id).run_page_url
                except Exception:
                    logger.debug("Failed to fetch initial run URL for run_id=%s", run_id)
        except Exception as exc:
            return NotebookJobRunResult(
                success=False,
                error=f"Failed to submit notebook run: {exc}",
                output_kind="none",
                state="SUBMIT_FAILED",
                message=f"Jobs API runs/submit call failed: {exc}",
                notebook_path=effective_notebook_path,
                cluster_id=cluster_id,
            )

        try:
            run = wait.result(timeout=datetime.timedelta(seconds=timeout))
        except TimeoutError:
            elapsed = round(time.time() - start_time, 2)
            cancel_message = ""
            if run_id is not None:
                try:
                    client.jobs.cancel_run(run_id=run_id)
                    cancel_message = " Cancel requested."
                except Exception:
                    logger.debug("Failed to request cancellation for timed out run_id=%s", run_id)
                    cancel_message = " Cancel could not be requested."
            if cleanup and should_upload:
                skip_cleanup = True
                cancel_message += (
                    f" Temporary notebook cleanup was skipped for {effective_notebook_path} "
                    "because the remote run may still be active."
                )
            return NotebookJobRunResult(
                success=False,
                error=f"Run timed out after {timeout}s.",
                output_kind="none",
                run_id=run_id,
                run_url=run_url,
                duration_seconds=elapsed,
                state="TIMEDOUT",
                message=f"Notebook run {run_id} did not complete within {timeout}s.{cancel_message}",
                notebook_path=effective_notebook_path,
                cluster_id=cluster_id,
            )
        except Exception as exc:
            elapsed = round(time.time() - start_time, 2)
            error_text = str(exc)

            if run_id:
                try:
                    failed_run = client.jobs.get_run(run_id=run_id)
                    if failed_run.tasks:
                        output_data = get_run_output(client=client, task_run_id=failed_run.tasks[0].run_id)
                        if output_data.get("error"):
                            error_text = output_data["error"]
                except Exception:
                    logger.debug("Failed to retrieve detailed output for failed run_id=%s", run_id)

            return NotebookJobRunResult(
                success=False,
                error=error_text,
                output_kind="none",
                run_id=run_id,
                run_url=run_url,
                duration_seconds=elapsed,
                state="FAILED",
                message=f"Run {run_id} failed: {exc}",
                notebook_path=effective_notebook_path,
                cluster_id=cluster_id,
            )

        elapsed = round(time.time() - start_time, 2)
        result_state = run.state.result_state if run.state else None
        state_message = run.state.state_message if run.state else None
        if run.run_page_url:
            run_url = run.run_page_url

        is_success = result_state == RunResultState.SUCCESS
        state_str = result_state.value if result_state else "UNKNOWN"

        output_text = None
        error_text = None
        if run.tasks:
            output_data = get_run_output(client=client, task_run_id=run.tasks[0].run_id)
            output_text = output_data["output"]
            error_text = output_data["error"]

        if is_success:
            output_kind = "text" if output_text is not None else "none"
            message = (
                f"Notebook executed successfully on {normalized_compute_type} compute in {elapsed}s."
                if output_text is not None
                else f"Notebook executed successfully on {normalized_compute_type} compute in {elapsed}s with no captured output."
            )
        else:
            output_kind = "none"
            if not error_text:
                error_text = state_message or f"Run ended with state: {state_str}"
            message = (
                f"Notebook run failed with state {state_str}. Check {run_url} for details."
                if run_url
                else f"Notebook run failed with state {state_str}. Check the Jobs UI for details."
            )

        return NotebookJobRunResult(
            success=is_success,
            output=output_text if is_success else None,
            output_kind=output_kind,
            error=error_text if not is_success else None,
            run_id=run_id,
            run_url=run_url,
            duration_seconds=elapsed,
            state=state_str,
            message=message,
            notebook_path=effective_notebook_path,
            cluster_id=cluster_id,
        )
    finally:
        if cleanup and should_upload and not skip_cleanup:
            cleanup_workspace_notebook(client=client, workspace_path=effective_notebook_path)
