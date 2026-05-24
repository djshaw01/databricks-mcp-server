from dataclasses import dataclass
from typing import Any

from databricks_mcp.notebook_jobs import run_notebook_job


@dataclass
class ServerlessRunResult:
    success: bool
    output: str | None = None
    output_kind: str = "none"
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
            "output_kind": self.output_kind,
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
    result = run_notebook_job(
        profile=profile,
        compute_type="serverless",
        notebook_path=workspace_path,
        code=code,
        language=language,
        timeout=timeout,
        run_name=run_name,
        cleanup=cleanup,
        job_extra_params=job_extra_params,
    )
    return ServerlessRunResult(
        success=result.success,
        output=result.output,
        output_kind=result.output_kind,
        error=result.error,
        run_id=result.run_id,
        run_url=result.run_url,
        duration_seconds=result.duration_seconds,
        state=result.state,
        message=result.message,
        workspace_path=result.notebook_path,
    )
