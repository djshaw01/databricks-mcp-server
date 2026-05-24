"""
Cluster-backed code execution via the Databricks Command Execution API.

Supports Python, Scala, SQL, and R. Execution contexts can be reused
across calls to preserve state (variables, imports) and avoid
re-initialisation overhead.
"""

import datetime
import logging
from dataclasses import dataclass, field
from typing import Any

from databricks.sdk.service.compute import (
    CommandStatus,
    DataSecurityMode,
    Language,
    ListClustersFilterBy,
    ClusterSource,
    State,
)

from databricks_mcp.workspace import get_client, get_current_username

logger = logging.getLogger(__name__)

_LANGUAGE_MAP = {
    "python": Language.PYTHON,
    "scala": Language.SCALA,
    "sql": Language.SQL,
    "r": Language.R,
}

_USER_CLUSTER_SOURCES = [ClusterSource.UI, ClusterSource.API]


# ─── Result types ─────────────────────────────────────────────────────────────


@dataclass
class ClusterExecutionResult:
    success: bool
    output: str | None = None
    output_kind: str = "none"
    error: str | None = None
    cluster_id: str | None = None
    context_id: str | None = None
    context_destroyed: bool = False
    message: str | None = None

    def __post_init__(self) -> None:
        if self.message:
            return
        if self.success and self.context_id and not self.context_destroyed:
            self.message = (
                f"Execution successful. To speed up follow-up commands and maintain "
                f"state (variables, imports), reuse context_id='{self.context_id}' with "
                f"cluster_id='{self.cluster_id}'."
            )
        elif self.success and self.context_destroyed:
            self.message = "Execution successful. Context was destroyed."

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "output": self.output,
            "output_kind": self.output_kind,
            "error": self.error,
            "cluster_id": self.cluster_id,
            "context_id": self.context_id,
            "context_destroyed": self.context_destroyed,
            "message": self.message,
        }


@dataclass
class NoRunningClusterError(Exception):
    available_clusters: list[dict[str, Any]] = field(default_factory=list)
    skipped_clusters: list[dict[str, Any]] = field(default_factory=list)
    startable_clusters: list[dict[str, Any]] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.suggestions = self._build_suggestions()
        super().__init__(self._build_message())

    def _build_suggestions(self) -> list[str]:
        suggestions = []
        if self.startable_clusters:
            best = self.startable_clusters[0]
            suggestions.append(
                f"ASK THE USER: \"I found your terminated cluster '{best['cluster_name']}'. "
                f"Would you like me to start it? (It typically takes 3–8 minutes.)\" "
                f"If they approve, start it, wait until RUNNING, then retry."
            )
            for c in self.startable_clusters[1:3]:
                suggestions.append(
                    f"Alternative: '{c['cluster_name']}' (cluster_id='{c['cluster_id']}', state={c['state']})"
                )
        suggestions.append(
            "For SQL-only workloads, use query_sql() instead — it routes through "
            "SQL warehouses and doesn't require a cluster."
        )
        suggestions.append("Ask a workspace admin for access to a shared cluster.")
        return suggestions

    def _build_message(self) -> str:
        msg = "No running cluster available for the current user."
        if self.startable_clusters:
            cluster_list = "\n".join(
                f"  - {c['cluster_name']} ({c['cluster_id']}) — {c['state']}"
                for c in self.startable_clusters[:10]
            )
            msg += f"\n\nTerminated clusters you could start:\n{cluster_list}"
        if self.skipped_clusters:
            skipped_list = "\n".join(
                f"  - {c['cluster_name']} ({c['cluster_id']}) — owned by {c.get('single_user_name', 'unknown')}"
                for c in self.skipped_clusters
            )
            msg += (
                f"\n\n{len(self.skipped_clusters)} running cluster(s) skipped "
                f"(single-user, assigned to another user):\n{skipped_list}"
            )
        msg += "\n\nSuggestions:\n"
        for i, s in enumerate(self.suggestions, 1):
            msg += f"  {i}. {s}\n"
        return msg

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": False,
            "error": str(self),
            "error_type": "no_running_cluster",
            "available_clusters": self.available_clusters,
            "startable_clusters": self.startable_clusters,
            "skipped_clusters": self.skipped_clusters,
            "suggestions": self.suggestions,
        }


# ─── Cluster helpers ──────────────────────────────────────────────────────────


def _is_cluster_accessible(cluster: Any, current_user: str | None) -> bool:
    if current_user is None:
        return True
    dsm = getattr(cluster, "data_security_mode", None)
    single_user = getattr(cluster, "single_user_name", None)
    if dsm == DataSecurityMode.SINGLE_USER and single_user:
        if single_user.lower() != current_user.lower():
            return False
    return True


def list_clusters(profile: str = "", include_terminated: bool = True) -> list[dict[str, Any]]:
    client = get_client(profile)
    clusters: list[dict[str, Any]] = []

    running_filter = ListClustersFilterBy(
        cluster_sources=_USER_CLUSTER_SOURCES,
        cluster_states=[State.RUNNING, State.PENDING, State.RESIZING, State.RESTARTING],
    )
    for c in client.clusters.list(filter_by=running_filter):
        clusters.append({
            "cluster_id": c.cluster_id,
            "cluster_name": c.cluster_name or "",
            "state": c.state.value if c.state else None,
            "creator_user_name": c.creator_user_name,
        })

    if include_terminated:
        terminated_filter = ListClustersFilterBy(
            cluster_sources=_USER_CLUSTER_SOURCES,
            cluster_states=[State.TERMINATED, State.TERMINATING, State.ERROR],
        )
        for c in client.clusters.list(filter_by=terminated_filter):
            clusters.append({
                "cluster_id": c.cluster_id,
                "cluster_name": c.cluster_name or "",
                "state": c.state.value if c.state else None,
                "creator_user_name": c.creator_user_name,
            })

    return clusters


def get_cluster_status(cluster_id: str, profile: str = "") -> dict[str, Any]:
    client = get_client(profile)
    cluster = client.clusters.get(cluster_id)
    name = cluster.cluster_name or cluster_id
    state = cluster.state.value if cluster.state else "UNKNOWN"

    if state == "RUNNING":
        message = f"Cluster '{name}' is running and ready."
    elif state in ("PENDING", "RESTARTING", "RESIZING"):
        message = f"Cluster '{name}' is {state.lower()}. Check again in 30–60 seconds."
    elif state == "TERMINATED":
        message = f"Cluster '{name}' is terminated."
    else:
        message = f"Cluster '{name}' is in state: {state}."

    return {"cluster_id": cluster_id, "cluster_name": name, "state": state, "message": message}


def start_cluster(cluster_id: str, profile: str = "") -> dict[str, Any]:
    client = get_client(profile)
    cluster = client.clusters.get(cluster_id)
    name = cluster.cluster_name or cluster_id
    state = cluster.state.value if cluster.state else "UNKNOWN"

    if state == "RUNNING":
        return {"cluster_id": cluster_id, "cluster_name": name, "state": "RUNNING",
                "message": f"Cluster '{name}' is already running."}
    if state not in ("TERMINATED", "ERROR"):
        return {"cluster_id": cluster_id, "cluster_name": name, "state": state,
                "message": f"Cluster '{name}' is in state {state}; it may already be starting."}

    client.clusters.start(cluster_id)
    return {
        "cluster_id": cluster_id,
        "cluster_name": name,
        "previous_state": state,
        "state": "PENDING",
        "message": (
            f"Cluster '{name}' is starting (typically 3–8 minutes). "
            f"Use get_cluster_status(cluster_id='{cluster_id}') to check progress."
        ),
    }


def _select_best_cluster(client: Any, current_user: str | None) -> tuple[str | None, list[dict]]:
    """Return (best_cluster_id, skipped_clusters)."""
    running_filter = ListClustersFilterBy(
        cluster_sources=_USER_CLUSTER_SOURCES,
        cluster_states=[State.RUNNING],
    )
    running: list[dict] = []
    skipped: list[dict] = []

    for c in client.clusters.list(filter_by=running_filter):
        if not _is_cluster_accessible(c, current_user):
            skipped.append({
                "cluster_id": c.cluster_id,
                "cluster_name": c.cluster_name or "",
                "single_user_name": getattr(c, "single_user_name", None) or "unknown",
            })
            continue
        running.append({"cluster_id": c.cluster_id, "cluster_name": c.cluster_name or ""})

    if not running:
        return None, skipped

    for c in running:
        if "shared" in c["cluster_name"].lower():
            return c["cluster_id"], skipped
    for c in running:
        if "demo" in c["cluster_name"].lower():
            return c["cluster_id"], skipped
    return running[0]["cluster_id"], skipped


# ─── Context helpers ──────────────────────────────────────────────────────────


def create_context(cluster_id: str, language: str = "python", profile: str = "") -> str:
    client = get_client(profile)
    lang_enum = _LANGUAGE_MAP.get(language.lower(), Language.PYTHON)
    result = client.command_execution.create(
        cluster_id=cluster_id, language=lang_enum
    ).result()
    return result.id


def destroy_context(cluster_id: str, context_id: str, profile: str = "") -> None:
    client = get_client(profile)
    try:
        client.command_execution.destroy(cluster_id=cluster_id, context_id=context_id)
    except Exception as exc:
        logger.debug("Context destroy failed (cluster=%s context=%s): %s", cluster_id, context_id, exc)


# ─── Core execution ───────────────────────────────────────────────────────────


def _run_on_context(
    *,
    client: Any,
    cluster_id: str,
    context_id: str,
    code: str,
    language: str,
    timeout: int,
) -> ClusterExecutionResult:
    lang_enum = _LANGUAGE_MAP.get(language.lower(), Language.PYTHON)

    try:
        result = client.command_execution.execute(
            cluster_id=cluster_id,
            context_id=context_id,
            language=lang_enum,
            command=code,
        ).result(timeout=datetime.timedelta(seconds=timeout))
    except TimeoutError:
        return ClusterExecutionResult(
            success=False,
            error=f"Command timed out after {timeout}s.",
            output_kind="none",
            cluster_id=cluster_id,
            context_id=context_id,
            context_destroyed=False,
        )

    if result.status == CommandStatus.FINISHED:
        if result.results and result.results.result_type and result.results.result_type.value == "error":
            error_msg = result.results.cause or "Unknown error"
            return ClusterExecutionResult(
                success=False,
                error=error_msg,
                output_kind="none",
                cluster_id=cluster_id,
                context_id=context_id,
                context_destroyed=False,
            )
        output = result.results.data if result.results and result.results.data else None
        return ClusterExecutionResult(
            success=True,
            output=str(output) if output is not None else None,
            output_kind="text" if output is not None else "none",
            cluster_id=cluster_id,
            context_id=context_id,
            context_destroyed=False,
        )

    if result.status in (CommandStatus.ERROR, CommandStatus.CANCELLED):
        error_msg = (result.results.cause if result.results and result.results.cause else None) or str(result.status)
        return ClusterExecutionResult(
            success=False,
            error=error_msg,
            output_kind="none",
            cluster_id=cluster_id,
            context_id=context_id,
            context_destroyed=False,
        )

    return ClusterExecutionResult(
        success=False,
        error=f"Unexpected command status: {result.status}",
        output_kind="none",
        cluster_id=cluster_id,
        context_id=context_id,
        context_destroyed=False,
    )


def run_code_on_cluster(
    *,
    code: str,
    profile: str = "",
    cluster_id: str | None = None,
    context_id: str | None = None,
    language: str = "python",
    timeout: int = 120,
    destroy_context_on_completion: bool = False,
) -> ClusterExecutionResult:
    """
    Execute code on an interactive Databricks cluster via the Command Execution API.

    If context_id is provided, the existing context is reused (faster; preserves state).
    If cluster_id is omitted, auto-selects the best running accessible cluster.

    Raises:
        NoRunningClusterError: when no running cluster is available and none was specified.
    """
    if not code or not code.strip():
        return ClusterExecutionResult(
            success=False,
            error="Code cannot be empty.",
            output_kind="none",
            cluster_id=cluster_id,
        )

    language = language.lower()
    if language not in _LANGUAGE_MAP:
        return ClusterExecutionResult(
            success=False,
            error=f"Unsupported language: {language!r}. Must be one of: {', '.join(_LANGUAGE_MAP)}.",
            output_kind="none",
            cluster_id=cluster_id,
        )

    client = get_client(profile)

    if cluster_id is None:
        current_user = get_current_username(profile)
        best_id, skipped = _select_best_cluster(client, current_user)
        if best_id is None:
            all_clusters = list_clusters(profile=profile)
            terminated = [c for c in all_clusters if c.get("state") in ("TERMINATED", "ERROR")]
            raise NoRunningClusterError(
                available_clusters=all_clusters,
                skipped_clusters=skipped,
                startable_clusters=terminated,
            )
        cluster_id = best_id

    try:
        if context_id is None:
            context_id = create_context(cluster_id, language, profile)

        result = _run_on_context(
            client=client,
            cluster_id=cluster_id,
            context_id=context_id,
            code=code,
            language=language,
            timeout=timeout,
        )

        if destroy_context_on_completion:
            destroy_context(cluster_id, context_id, profile)
            result.context_destroyed = True
            result.message = (
                "Execution successful. Context was destroyed."
                if result.success
                else "Execution failed. Context was destroyed."
            )

        return result

    except Exception as exc:
        context_destroyed = False
        if destroy_context_on_completion and context_id is not None:
            destroy_context(cluster_id, context_id, profile)
            context_destroyed = True
        return ClusterExecutionResult(
            success=False,
            error=str(exc),
            output_kind="none",
            cluster_id=cluster_id,
            context_id=context_id,
            context_destroyed=context_destroyed,
            message=(
                "Execution failed. Context was destroyed."
                if context_destroyed
                else "Execution failed."
            ),
        )
