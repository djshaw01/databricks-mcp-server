import os
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from databricks_mcp.sql_query import DEFAULT_SQL_POLL_TIMEOUT_SECONDS


@dataclass(frozen=True)
class WorkspaceConfig:
    host: str
    warehouse_id: str | None
    sql_poll_timeout_seconds: int
    profile: str | None


def resolve_profile(profile: str = "") -> str | None:
    normalized_profile = profile.strip()
    if not normalized_profile:
        return None
    if not re.search(r"[A-Za-z0-9]", normalized_profile):
        raise ValueError("profile must contain at least one letter or number.")
    return normalized_profile


def profile_env_prefix(profile: str) -> str:
    normalized_profile = re.sub(r"[^A-Za-z0-9]+", "_", profile).strip("_").upper()
    if not normalized_profile:
        raise ValueError("profile must contain at least one letter or number.")
    return f"DATABRICKS_PROFILE_{normalized_profile}"


def get_env_value(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def parse_positive_int_env(name: str, value: str | None) -> int:
    if not value:
        return DEFAULT_SQL_POLL_TIMEOUT_SECONDS

    try:
        parsed_value = int(value)
    except ValueError as exc:
        raise ValueError(
            f"{name} must be a positive integer number of seconds."
        ) from exc

    if parsed_value <= 0:
        raise ValueError(f"{name} must be a positive integer number of seconds.")
    return parsed_value


def get_workspace_config(profile: str = "") -> WorkspaceConfig:
    resolved_profile = resolve_profile(profile)
    if resolved_profile is None:
        host = get_env_value("DATABRICKS_HOST")
        if not host:
            raise ValueError(
                "DATABRICKS_HOST must be set (e.g. https://adb-xxx.azuredatabricks.net). "
                "Authenticate with: databricks auth login --host <workspace-url>"
            )
        return WorkspaceConfig(
            host=host,
            warehouse_id=get_env_value("DATABRICKS_WAREHOUSE_ID"),
            sql_poll_timeout_seconds=parse_positive_int_env(
                "DATABRICKS_SQL_POLL_TIMEOUT_SECONDS",
                get_env_value("DATABRICKS_SQL_POLL_TIMEOUT_SECONDS"),
            ),
            profile=None,
        )

    env_prefix = profile_env_prefix(resolved_profile)
    host_key = f"{env_prefix}_HOST"
    warehouse_id_key = f"{env_prefix}_WAREHOUSE_ID"
    timeout_key = f"{env_prefix}_SQL_POLL_TIMEOUT_SECONDS"

    host = get_env_value(host_key)
    if not host:
        raise ValueError(f"{host_key} must be set when profile='{resolved_profile}'.")

    timeout_value = get_env_value(timeout_key)
    if timeout_value is None:
        timeout_key = "DATABRICKS_SQL_POLL_TIMEOUT_SECONDS"
        timeout_value = get_env_value(timeout_key)

    return WorkspaceConfig(
        host=host,
        warehouse_id=get_env_value(warehouse_id_key) or get_env_value("DATABRICKS_WAREHOUSE_ID"),
        sql_poll_timeout_seconds=parse_positive_int_env(timeout_key, timeout_value),
        profile=resolved_profile,
    )


def get_client(profile: str = "") -> WorkspaceClient:
    workspace_config = get_workspace_config(profile)
    client_kwargs: dict[str, str] = {"host": workspace_config.host}
    if workspace_config.profile:
        client_kwargs["profile"] = workspace_config.profile
    return WorkspaceClient(**client_kwargs)


def get_current_username(profile: str = "") -> str | None:
    try:
        return get_client(profile).current_user.me().user_name
    except Exception:
        return None


def get_warehouse_id(profile: str = "") -> str:
    workspace_config = get_workspace_config(profile)
    warehouse_id = workspace_config.warehouse_id
    if not warehouse_id:
        if workspace_config.profile:
            profile_warehouse_key = f"{profile_env_prefix(workspace_config.profile)}_WAREHOUSE_ID"
            raise ValueError(
                f"{profile_warehouse_key} or DATABRICKS_WAREHOUSE_ID must be set to the serverless SQL warehouse to use for read-only queries."
            )
        raise ValueError(
            "DATABRICKS_WAREHOUSE_ID must be set to the serverless SQL warehouse to use for read-only queries."
        )
    return warehouse_id


def get_sql_poll_timeout_seconds(profile: str = "") -> int:
    return get_workspace_config(profile).sql_poll_timeout_seconds


def parse_positive_poll_timeout_override(poll_timeout_seconds: int | None) -> int | None:
    if poll_timeout_seconds is None:
        return None
    if poll_timeout_seconds <= 0:
        raise ValueError("poll_timeout_seconds must be a positive integer number of seconds.")
    return poll_timeout_seconds
