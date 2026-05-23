import base64
import datetime
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any

from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobEnvironment,
    NotebookTask,
    RunResultState,
    SubmitTask,
)
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks_mcp.workspace import get_client, get_current_username

logger = logging.getLogger(__name__)

_LANGUAGE_MAP = {
    "python": Language.PYTHON,
    "sql": Language.SQL,
}


@dataclass
class ServerlessRunResult:
    success: bool
    output: str | None = None
    error: str | None = None
    run_id: int | None = None
    run_url: str | None = None
    duration_seconds: float | None = None
    state: str | None = None
    message: str | None = None
    workspace_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "run_id": self.run_id,
            "run_url": self.run_url,
            "duration_seconds": self.duration_seconds,
            "state": self.state,
            "message": self.message,
        }
        if self.workspace_path:
            result["workspace_path"] = self.workspace_path
        return result


def _get_temp_notebook_path(profile: str, run_label: str) -> str:
    username = get_current_username(profile)
    base = f"/Workspace/Users/{username}" if username else "/Workspace"
    return f"{base}/.databricks_mcp_tmp/{run_label}"


def _is_ipynb(content: str) -> bool:
    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        return False
    return isinstance(payload, dict) and "cells" in payload


def _upload_workspace_notebook(
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


def _cleanup_workspace_notebook(*, client: Any, workspace_path: str) -> None:
    try:
        client.workspace.delete(path=workspace_path, recursive=False)
    except Exception as exc:
        logger.debug("Cleanup of %s failed: %s", workspace_path, exc)


def _get_run_output(*, client: Any, task_run_id: int) -> dict[str, str | None]:
    result: dict[str, str | None] = {"output": None, "error": None}

    try:
        run_output = client.jobs.get_run_output(run_id=task_run_id)
    except Exception as exc:
        logger.debug("Failed to fetch output for task run %s: %s", task_run_id, exc)
        return {"output": None, "error": str(exc)}

    if run_output.notebook_output and run_output.notebook_output.result:
        result["output"] = run_output.notebook_output.result

    if run_output.logs:
        if result["output"]:
            result["output"] += f"\n\n--- Logs ---\n{run_output.logs}"
        else:
            result["output"] = run_output.logs

    if run_output.error:
        error_parts = [run_output.error]
        if run_output.error_trace:
            error_parts.append(run_output.error_trace)
        result["error"] = "\n\n".join(error_parts)

    return result


def run_code_on_serverless(
    *,
    code: str,
    profile: str = "",
    language: str = "python",
    timeout: int = 1800,
    run_name: str | None = None,
    cleanup: bool = True,
    workspace_path: str | None = None,
    job_extra_params: dict[str, Any] | None = None,
) -> ServerlessRunResult:
    if not code or not code.strip():
        return ServerlessRunResult(
            success=False,
            error="Code cannot be empty.",
            state="INVALID_INPUT",
            message="No code provided to execute.",
        )

    is_jupyter = _is_ipynb(code)
    language = language.lower()
    if not is_jupyter and language not in _LANGUAGE_MAP:
        return ServerlessRunResult(
            success=False,
            error=f"Unsupported language: {language!r}. Must be 'python' or 'sql'.",
            state="INVALID_INPUT",
            message=f"Unsupported language {language!r}. Use 'python' or 'sql'.",
        )

    unique_id = uuid.uuid4().hex[:12]
    if not run_name:
        run_name = f"databricks_mcp_serverless_{unique_id}"

    notebook_path = workspace_path or _get_temp_notebook_path(profile, f"serverless_{unique_id}")
    if workspace_path:
        cleanup = False

    client = get_client(profile)
    start_time = time.time()
    run_id: int | None = None
    run_url: str | None = None

    try:
        try:
            _upload_workspace_notebook(
                client=client,
                code=code,
                language=language,
                workspace_path=notebook_path,
                is_jupyter=is_jupyter,
            )
        except Exception as exc:
            return ServerlessRunResult(
                success=False,
                error=f"Failed to upload code to workspace: {exc}",
                state="UPLOAD_FAILED",
                message=f"Could not upload notebook for execution: {exc}",
            )

        try:
            extra = job_extra_params or {}
            environment_key = "Default"
            if "environments" in extra and extra["environments"]:
                environment_key = extra["environments"][0].get("environment_key", "Default")

            submit_kwargs: dict[str, Any] = {
                "run_name": run_name,
                "tasks": [
                    SubmitTask(
                        task_key="main",
                        notebook_task=NotebookTask(notebook_path=notebook_path),
                        environment_key=environment_key,
                    )
                ],
            }

            if "environments" not in extra:
                submit_kwargs["environments"] = [
                    JobEnvironment(
                        environment_key="Default",
                        spec=Environment(client="1"),
                    )
                ]

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
            return ServerlessRunResult(
                success=False,
                error=f"Failed to submit serverless run: {exc}",
                state="SUBMIT_FAILED",
                message=f"Jobs API runs/submit call failed: {exc}",
                workspace_path=workspace_path,
            )

        try:
            run = wait.result(timeout=datetime.timedelta(seconds=timeout))
        except TimeoutError:
            elapsed = round(time.time() - start_time, 2)
            return ServerlessRunResult(
                success=False,
                error=f"Run timed out after {timeout}s.",
                run_id=run_id,
                run_url=run_url,
                duration_seconds=elapsed,
                state="TIMEDOUT",
                message=f"Serverless run {run_id} did not complete within {timeout}s.",
                workspace_path=workspace_path,
            )
        except Exception as exc:
            elapsed = round(time.time() - start_time, 2)
            error_text = str(exc)

            if run_id:
                try:
                    failed_run = client.jobs.get_run(run_id=run_id)
                    if failed_run.tasks:
                        output_data = _get_run_output(client=client, task_run_id=failed_run.tasks[0].run_id)
                        if output_data.get("error"):
                            error_text = output_data["error"]
                except Exception:
                    logger.debug("Failed to retrieve detailed output for failed run_id=%s", run_id)

            return ServerlessRunResult(
                success=False,
                error=error_text,
                run_id=run_id,
                run_url=run_url,
                duration_seconds=elapsed,
                state="FAILED",
                message=f"Run {run_id} failed: {exc}",
                workspace_path=workspace_path,
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
            output_data = _get_run_output(client=client, task_run_id=run.tasks[0].run_id)
            output_text = output_data["output"]
            error_text = output_data["error"]

        if is_success:
            if not output_text:
                output_text = "Success (no output)"
            message = f"Code executed successfully on serverless compute in {elapsed}s."
        else:
            if not error_text:
                error_text = state_message or f"Run ended with state: {state_str}"
            message = f"Serverless run failed with state {state_str}. Check {run_url} for details."

        return ServerlessRunResult(
            success=is_success,
            output=output_text if is_success else None,
            error=error_text if not is_success else None,
            run_id=run_id,
            run_url=run_url,
            duration_seconds=elapsed,
            state=state_str,
            message=message,
            workspace_path=workspace_path,
        )
    finally:
        if cleanup:
            _cleanup_workspace_notebook(client=client, workspace_path=notebook_path)
