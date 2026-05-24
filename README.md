# Databricks MCP Server

Browse and operate Databricks Jobs, Delta Live Tables pipelines, and Unity Catalog metadata via the Model Context Protocol (MCP). Works with VS Code Copilot agent mode.

## Tools

### Jobs
| Tool | Description |
|------|-------------|
| `list_jobs` | List all jobs with optional name filter |
| `get_job` | Full job config (tasks, clusters, schedule) |
| `list_job_runs` | Recent runs for a job with state and timing |
| `get_job_run` | Per-task breakdown with error messages |
| `get_job_run_output` | Notebook result text, logs, and error details for a run or task |
| `get_job_run_export` | Export a run as HTML notebook views for richer rendering review |
| `cancel_job_run` | Cancel an active run |
| `run_job` | Trigger a new job run |

### Delta Live Tables
| Tool | Description |
|------|-------------|
| `list_pipelines` | List DLT pipelines with current state |
| `get_pipeline` | Pipeline config (libraries, clusters, last updates) |
| `list_pipeline_updates` | Recent update history |
| `get_pipeline_update` | Events and errors for a specific update |
| `start_pipeline_update` | Trigger a full or incremental pipeline refresh |

### Unity Catalog
| Tool | Description |
|------|-------------|
| `list_catalogs` | List all catalogs in the workspace (preferred for metadata discovery) |
| `list_schemas` | List schemas inside a catalog (preferred for metadata discovery) |
| `list_tables` | List tables in a schema with optional name filter (preferred for metadata discovery) |
| `get_table` | Full table metadata including all columns and types (preferred for metadata discovery) |
| `search_tables` | Find tables by name pattern across catalogs (preferred for metadata discovery) |
| `search_columns` | Find which tables contain a column by name (preferred for metadata discovery) |

### SQL
| Tool | Description |
|------|-------------|
| `query_sql` | Execute a single sanitized read-only `SELECT` / `WITH ... SELECT` query on the configured SQL warehouse for row-level results; avoid for table/column discovery when Unity Catalog tools can answer the request |

### Code Execution
| Tool | Description |
|------|-------------|
| `execute_code` | Execute one-off snippets on serverless workflows or interactive clusters; best for short commands and REPL-style cluster iteration |
| `execute_notebook` | Create, modify, or rerun notebooks on serverless or an existing cluster through Jobs runs; keep the returned `run_id` and use `get_job_run_output` / `get_job_run_export` to inspect execution |
| `list_compute` | List interactive clusters that can be targeted by `execute_code` |
| `manage_cluster` | Get cluster status or start a terminated cluster |

---

## Setup

### 1. Install

```bash
git clone <this-repo>
cd databricks-mcp-server
uv sync
```

### 2. Authenticate with OAuth (recommended)

```bash
# Install Databricks CLI
brew install databricks

# Login — opens browser, stores token in ~/.databrickscfg
databricks auth login --host https://adb-xxx.azuredatabricks.net

# Optional: login with a named profile for multi-workspace setups
databricks auth login --host https://adb-xxx.azuredatabricks.net --profile prod-west
```

No token stored in files. The SDK picks up the OAuth session automatically.

### 3. Set workspace URL

```bash
cp .env.example .env
# Edit .env and set:
# DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
# DATABRICKS_WAREHOUSE_ID=<serverless-sql-warehouse-id>
# Optional: DATABRICKS_SQL_POLL_TIMEOUT_SECONDS=120
```

`DATABRICKS_WAREHOUSE_ID` is required for `query_sql`. It is **not** used by
`execute_code`. Serverless code execution runs on Databricks serverless
workflows, not on a SQL warehouse.

To configure multiple workspaces in the same `.env`, add named entries using
`DATABRICKS_PROFILE_<PROFILE>_*` keys. For example, `profile="prod-west"`
maps to `DATABRICKS_PROFILE_PROD_WEST_HOST` and
`DATABRICKS_PROFILE_PROD_WEST_WAREHOUSE_ID`, and optional profile-specific poll
timeouts can be set with `DATABRICKS_PROFILE_PROD_WEST_SQL_POLL_TIMEOUT_SECONDS`.
If a profile-specific poll timeout is not set, `query_sql` falls back to
`DATABRICKS_SQL_POLL_TIMEOUT_SECONDS`, then to 120 seconds.

If you omit `profile`, tools keep using the existing default `DATABRICKS_*`
settings.

### 4. Workspace prerequisites for code execution

| Backend | Workspace / permission requirements |
|---------|-------------------------------------|
| `execute_code` with `compute_type="serverless"` | Databricks serverless workflows must be enabled for the workspace, and the caller must be able to submit and read Jobs runs |
| `execute_code` with `compute_type="cluster"` | The caller must have access to an interactive cluster that supports the Command Execution API; starting a terminated cluster also requires cluster start permission |
| `query_sql` | A configured SQL warehouse is required, but that warehouse does not satisfy the serverless workflows requirement above |

If you use named profiles, authenticate that profile explicitly:

```bash
databricks auth login --host https://adb-xxx.azuredatabricks.net --profile prod-west
```

---

## Use with VS Code Copilot

### Prerequisites
- VS Code 1.99+
- GitHub Copilot extension with agent mode enabled

### Steps

1. Open this folder in VS Code — `.vscode/mcp.json` is auto-discovered.
2. `Cmd+Shift+P` → **MCP: List Servers** → you should see `databricks`.
3. Click **Start** (or it starts automatically when Copilot needs it).
4. Open Copilot Chat, switch to **Agent** mode.
5. Start prompting.

### Troubleshooting

| Problem | Fix |
|---------|-----|
| Server not discovered | Open the *folder*, not a single file — `.vscode/mcp.json` must be at workspace root |
| `uv: command not found` | Replace `"command": "uv"` in `mcp.json` with the output of `which uv` |
| Auth error | Run `databricks auth login --host <your-host>` or `databricks auth login --host <your-host> --profile <profile>` and ensure `DATABRICKS_HOST` or `DATABRICKS_PROFILE_<PROFILE>_HOST` in `.env` has `https://` |
| SQL query tool returns configuration error | Set `DATABRICKS_WAREHOUSE_ID` to the serverless warehouse used for statement execution |
| SQL query polling needs more or less time | Set `DATABRICKS_SQL_POLL_TIMEOUT_SECONDS` (or profile-specific timeout env vars) to the desired limit in seconds; `query_sql` also supports per-request `poll_timeout_seconds`; default is 120 |
| `execute_code` on serverless says serverless compute is not enabled | Enable Databricks serverless workflows for the workspace and confirm the caller can submit/read Jobs runs; a SQL warehouse alone is not enough |
| `execute_code` cannot find a cluster | Use `list_compute` to see accessible clusters, pass `cluster_id` explicitly, or start a terminated cluster with `manage_cluster(action="start", ...)` |
| `execute_code` rejects `cluster_id` or another new argument as an extra property | Restart the MCP server in VS Code so the client refreshes the live schema |

---

## Example Copilot Prompts

### Code Execution

See also: [`docs/agent-tool-guide.md`](docs/agent-tool-guide.md) for user-facing guidance on how to tell the agent which execution tool to use.

`execute_code` returns a normalized response with:

- `success`, `error`, `message`
- `output` and `output_kind`
- `language`, `compute_type_requested`, `compute_type_resolved`
- serverless metadata when applicable: `run_id`, `run_url`, `duration_seconds`, `state`, `workspace_path`
- cluster metadata when applicable: `cluster_id`, `context_id`, `context_destroyed`

Use `execute_code` for snippets and REPL-style iteration. For notebook development, prefer `execute_notebook`.

For `compute_type="serverless"`, the returned `output` is only the captured text Databricks exposes for the run: notebook result text, logs, or both. To review the flow of the job after execution, keep the returned `run_id` and call `get_job_run_output`. When you need richer notebook renderings, call `get_job_run_export` to retrieve Databricks' exported HTML views for the run. For multi-task job runs, use `get_job_run` to discover task keys, then call `get_job_run_output(run_id=..., task_key=...)` or `get_job_run_export(run_id=..., task_key=...)`.

```
Run this Python snippet with execute_code on serverless compute: print(1 + 1)
```

```
Run this Python snippet with execute_code, then reuse the returned context_id on the next cluster call
```

```
Run this Python notebook with execute_notebook, then use the returned run_id with get_job_run_output to inspect the job flow
```

```
Run this Python notebook with execute_notebook, then use the returned run_id with get_job_run_export to inspect the rendered HTML notebook view
```

```
Run the existing notebook /Workspace/Users/me/demo on cluster 0522-121745-8myg24rm with execute_notebook
```

```
Run this Scala snippet with execute_code on cluster 0522-121745-8myg24rm: println(42)
```

```
Use execute_code with compute_type="cluster" and reuse the returned context_id on the next call
```

```
List accessible interactive clusters with list_compute, then start cluster 0522-121745-8myg24rm with manage_cluster
```

### Jobs

```
List all my Databricks jobs
```
```
Show the last 5 runs of job 12345 — any failures?
```
```
Get the notebook output and logs for run 67890
```
```
Export run 67890 as HTML so I can inspect the rendered notebook output
```
```
Get the output for task "train" from multi-task run 67890
```
```
Export the "train" task from multi-task run 67890 with views_to_export="ALL"
```
```
Which jobs have a failed latest run?
```

### Pipelines

```
List all DLT pipelines and their current state
```
```
Get the error events from the latest update of the sales pipeline
```

### Catalog

```
What catalogs and schemas do I have?
```
```
How many tables start with "fact_" in the main catalog?
```
```
Where can I find a column called customer_id?
```
```
Show me all columns and types for main.sales.orders
```

### SQL

```
Run a read-only query for the latest 25 orders in main.sales
```
```
Use query_sql to select customer_id, order_id, and order_total from sales.orders where order_date = '2026-05-01'
```

### Bootstrap Retry Agent

Paste this into Copilot Chat (Agent mode) to auto-detect and rerun bootstrap-failed jobs:

```
Scan all Databricks jobs and find bootstrap failures:

1. Call list_jobs to get every job
2. For each job call list_job_runs with limit=1 to get the latest run
3. For any run where result_state is FAILED, call get_job_run to read the full error
4. Decide if the failure is a bootstrap error — look for:
   - "bootstrap" in state_message
   - cluster failed to start / init script failure
   - Spark context could not be initialized
   - driver failed during startup before job logic ran
5. For confirmed bootstrap failures call run_job to rerun
6. Show a summary: jobs checked / failed / rerun / skipped with reason
```

For a dry run (inspect only, no reruns):
```
Do the same but DO NOT call run_job — only tell me which jobs you would rerun and why.
```

---

## Project Structure

```
databricks-mcp-server/
├── src/databricks_mcp/
│   ├── server.py          # MCP tools
│   └── sql_query.py       # Read-only SQL sanitization and statement execution
├── .vscode/
│   ├── mcp.json           # VS Code MCP server config (auto-discovered)
│   └── settings.json      # Enables MCP in Copilot Chat
├── .env.example           # Copy to .env and set DATABRICKS_HOST
├── tests/
│   └── test_sql_query.py  # Unit tests for safe SQL querying
└── pyproject.toml
```

---

## `execute_code` contract

### Request fields

| Field | Required | Applies to | Notes |
|------|----------|------------|------|
| `code` | one of `code`/`file_path` | both | Inline source to execute |
| `file_path` | one of `code`/`file_path` | both | Local `.py`, `.sql`, `.ipynb`, `.scala`, or `.r` file |
| `compute_type` | no | both | `auto` (default), `serverless`, or `cluster` |
| `language` | no | both | Defaults to `python`; overridden by supported file extension |
| `timeout` | no | both | Default `1800` for serverless, `120` for cluster |
| `profile` | no | both | Named workspace profile from `.env` |
| `workspace_path` | no | serverless only | Persist uploaded notebook and skip cleanup |
| `run_name` | no | serverless only | Optional Jobs run name |
| `cluster_id` | no | cluster only | Target interactive cluster; omitted means auto-select a running cluster |
| `context_id` | no | cluster only | Reuse an existing command execution context |
| `destroy_context_on_completion` | no | cluster only | Destroy the execution context after the run |

### Routing rules

1. `compute_type="serverless"` always uses serverless workflows.
2. `compute_type="cluster"` always uses the Command Execution API on an interactive cluster.
3. `compute_type="auto"` resolves to:
   - `serverless` for Python and SQL
   - `cluster` for Scala and R
4. If `file_path` is provided, its extension overrides `language`.
5. If cluster execution is selected and `cluster_id` is omitted, the server picks the best running accessible cluster by preferring names containing `shared`, then `demo`, then the first remaining running cluster.
6. `cluster_id`, `context_id`, and `destroy_context_on_completion` are invalid for serverless execution.
7. `workspace_path` and `run_name` are invalid for cluster execution.

### Normalized response fields

All `execute_code` responses include the same top-level contract:

| Field | Meaning |
|------|---------|
| `success` | Whether execution succeeded |
| `error` | Error text on failure, else `null` |
| `message` | Human-readable summary |
| `output` | Captured textual output, or `null` when none was captured |
| `output_kind` | `text` or `none` |
| `language` | Final execution language after file extension detection |
| `compute_type_requested` | Original `compute_type` input |
| `compute_type_resolved` | Actual backend used: `serverless` or `cluster` |

Backend-specific fields are always present but may be `null`:

| Field | Serverless | Cluster |
|------|------------|---------|
| `run_id` | run id | `null` |
| `run_url` | Jobs UI URL | `null` |
| `duration_seconds` | populated | `null` |
| `state` | Jobs result state | `null` unless cluster error payload uses it |
| `workspace_path` | populated when persisted | `null` |
| `cluster_id` | `null` | cluster id |
| `context_id` | `null` | context id |
| `context_destroyed` | `null` | boolean |

### Output semantics and current limitation

- Cluster execution returns the textual result produced by the Command
  Execution API, when one is available.
- Serverless execution currently maps Databricks Jobs run output into the
  `output` field by returning `notebook_output.result` and appending `logs`
  when logs are present.
- This server does **not** yet expose a standalone MCP tool for fetching the
  full raw `/api/2.2/jobs/runs/get-output` payload for an existing run after
  the fact. Today, `execute_code` only returns the normalized execution result.

### Troubleshooting schema drift

If an MCP client rejects newly-added fields like `cluster_id` as `additionalProperties`, restart the MCP server in VS Code so the client refreshes the live tool schema from the current source.
