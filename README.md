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
| `list_catalogs` | List all catalogs in the workspace |
| `list_schemas` | List schemas inside a catalog |
| `list_tables` | List tables in a schema with optional name filter |
| `get_table` | Full table metadata including all columns and types |
| `search_tables` | Find tables by name pattern across catalogs |
| `search_columns` | Find which tables contain a column by name |

### SQL
| Tool | Description |
|------|-------------|
| `query_sql` | Execute a single sanitized read-only `SELECT` / `WITH ... SELECT` query on the configured SQL warehouse |

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

To configure multiple workspaces in the same `.env`, add named entries using
`DATABRICKS_PROFILE_<PROFILE>_*` keys. For example, `profile="prod-west"`
maps to `DATABRICKS_PROFILE_PROD_WEST_HOST` and
`DATABRICKS_PROFILE_PROD_WEST_WAREHOUSE_ID`, and optional profile-specific poll
timeouts can be set with `DATABRICKS_PROFILE_PROD_WEST_SQL_POLL_TIMEOUT_SECONDS`.
If a profile-specific poll timeout is not set, `query_sql` falls back to
`DATABRICKS_SQL_POLL_TIMEOUT_SECONDS`, then to 120 seconds.

If you omit `profile`, tools keep using the existing default `DATABRICKS_*`
settings.

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
| SQL query polling needs more or less time | Set `DATABRICKS_SQL_POLL_TIMEOUT_SECONDS` to the desired limit in seconds; default is 120 |

---

## Example Copilot Prompts

### Jobs

```
List all my Databricks jobs
```
```
Show the last 5 runs of job 12345 — any failures?
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
