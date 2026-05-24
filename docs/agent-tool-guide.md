# Agent tool guide

Use this guide when you want to tell the agent which Databricks MCP tool to use.

## Quick selection table

| Goal | Tool |
|------|------|
| Run a short code snippet or do REPL-style cluster iteration | `execute_code` |
| Create a new notebook, modify an existing notebook, rerun a notebook, or inspect notebook runs | `execute_notebook` |
| List jobs | `list_jobs` |
| Inspect one job's configuration | `get_job` |
| List recent runs for a job | `list_job_runs` |
| Inspect one run in detail | `get_job_run` |
| Read notebook result text and logs from a Jobs run | `get_job_run_output` |
| Inspect exported HTML notebook views for richer rendering | `get_job_run_export` |
| Cancel a running job run | `cancel_job_run` |
| Trigger a job run | `run_job` |
| Run a read-only SQL query for result rows | `query_sql` |
| List DLT pipelines | `list_pipelines` |
| Inspect a DLT pipeline | `get_pipeline` |
| List recent DLT updates | `list_pipeline_updates` |
| Inspect one DLT update and its events | `get_pipeline_update` |
| Trigger a DLT update | `start_pipeline_update` |
| List catalogs | `list_catalogs` |
| List schemas in a catalog | `list_schemas` |
| List tables in a schema | `list_tables` |
| Inspect one table's metadata | `get_table` |
| Search tables by name | `search_tables` |
| Search columns across tables | `search_columns` |
| List accessible interactive clusters | `list_compute` |
| Check cluster status or start a cluster | `manage_cluster` |

## When to use `execute_code`

Use `execute_code` when you want the agent to run a one-off command.

Examples:

```text
Use execute_code on serverless to run this Python snippet: print(1 + 1)
```

```text
Use execute_code with compute_type="cluster" on cluster 0522-121745-8myg24rm and reuse the returned context_id for follow-up commands
```

```text
Use execute_code on cluster to try this Scala snippet: println(spark.version)
```

`execute_code` is best when:

- the task is a short snippet
- you want fast iteration on a cluster with `context_id`
- you do not need a notebook-first workflow

## When to use `execute_notebook`

Use `execute_notebook` when you want the agent to work in a notebook lifecycle instead of a snippet lifecycle.

Examples:

```text
Create a notebook at /Workspace/Users/me/customer-demo and run it with execute_notebook on serverless
```

```text
Modify the existing notebook /Workspace/Users/me/customer-demo, rerun it with execute_notebook, and inspect the run output
```

```text
Run the existing notebook /Workspace/Users/me/customer-demo on cluster 0522-121745-8myg24rm with execute_notebook
```

```text
Use execute_notebook to upload notebook content from this local file and run it on cluster 0522-121745-8myg24rm: notebooks/train_model.scala
```

```text
Run /Workspace/Users/me/customer-demo with execute_notebook and pass notebook parameters env=dev and limit=10
```

`execute_notebook` is best when:

- you are authoring a new notebook
- you are modifying an existing notebook
- you want a Jobs `run_id`
- you want to inspect logs, notebook result text, or HTML exports after the run

## Jobs tools

### `list_jobs`

Use this when you want the agent to discover jobs before deciding what to inspect or run.

Examples:

```text
Use list_jobs and show me all Databricks jobs with "daily" in the name
```

```text
Use list_jobs and tell me which ETL jobs exist in this workspace
```

### `get_job`

Use this when you want task configuration, dependencies, schedule details, or notebook paths for a specific job.

Examples:

```text
Use get_job for job 12345 and summarize its tasks and schedule
```

```text
Use get_job for job 12345 and tell me which notebook paths it runs
```

### `list_job_runs`

Use this when you want recent run history for a job.

Examples:

```text
Use list_job_runs for job 12345 and show me the last 10 runs
```

```text
Use list_job_runs for job 12345 and tell me whether any recent runs failed
```

### `get_job_run`

Use this when you want detailed status for a specific run, including task-level state.

Examples:

```text
Use get_job_run for run 67890 and show me task states and errors
```

```text
Use get_job_run for run 67890 and tell me which task failed first
```

### `get_job_run_output`

Use this to inspect notebook result text and logs.

Examples:

```text
Use get_job_run_output for run 12345 and summarize the execution flow
```

```text
Use get_job_run_output for run 12345 task_key="train"
```

### `get_job_run_export`

Use this to inspect exported HTML notebook views when the text/log output is not enough.

Examples:

```text
Use get_job_run_export for run 12345 so I can inspect the rendered notebook output
```

```text
Use get_job_run_export for run 12345 with views_to_export="ALL"
```

```text
Use get_job_run_export for run 12345 task_key="train" and inspect the rendered notebook view
```

### `cancel_job_run`

Use this when you want the agent to stop an active Jobs run.

Examples:

```text
Use cancel_job_run for run 67890
```

```text
Cancel the stuck Databricks job run 67890 with cancel_job_run
```

### `run_job`

Use this when you want the agent to trigger an existing Databricks job.

Examples:

```text
Use run_job for job 12345
```

```text
Trigger job 12345 and then inspect the new run
```

## SQL tool

### `query_sql`

Use this when you need actual query result rows, not just metadata.

Examples:

```text
Use query_sql to show me the latest 25 orders in main.sales.orders
```

```text
Use query_sql to count rows in main.sales.orders where order_date = '2026-05-01'
```

Avoid using `query_sql` when a metadata tool can answer the question more directly.

## Delta Live Tables tools

### `list_pipelines`

Use this when you want the agent to discover pipelines or check high-level state.

Examples:

```text
Use list_pipelines and show me all current DLT pipelines
```

```text
Use list_pipelines and find any pipeline with "sales" in the name
```

### `get_pipeline`

Use this when you want a pipeline's configuration, libraries, or cluster spec.

Examples:

```text
Use get_pipeline for pipeline 1234-567890-abcd and summarize its libraries and target schema
```

```text
Use get_pipeline for pipeline 1234-567890-abcd and tell me whether it is in development mode
```

### `list_pipeline_updates`

Use this when you want recent update history for one pipeline.

Examples:

```text
Use list_pipeline_updates for pipeline 1234-567890-abcd and show me the last 5 updates
```

```text
Use list_pipeline_updates for pipeline 1234-567890-abcd and tell me whether recent updates succeeded
```

### `get_pipeline_update`

Use this when you want the recent events, warnings, and errors for a specific update.

Examples:

```text
Use get_pipeline_update for pipeline 1234-567890-abcd update 9999-8888 and summarize the errors
```

```text
Use get_pipeline_update for pipeline 1234-567890-abcd update 9999-8888 and tell me why it failed
```

### `start_pipeline_update`

Use this when you want the agent to trigger a pipeline refresh.

Examples:

```text
Use start_pipeline_update for pipeline 1234-567890-abcd
```

```text
Use start_pipeline_update for pipeline 1234-567890-abcd with full_refresh=true
```

## Unity Catalog tools

### `list_catalogs`

Use this when you want a high-level view of available catalogs.

Examples:

```text
Use list_catalogs and show me all accessible catalogs
```

```text
Use list_catalogs and tell me whether main is available
```

### `list_schemas`

Use this when you already know the catalog and want its schemas.

Examples:

```text
Use list_schemas for catalog main
```

```text
Use list_schemas for catalog finance and summarize the available schemas
```

### `list_tables`

Use this when you know the catalog and schema and want to browse tables or views.

Examples:

```text
Use list_tables for catalog main schema sales
```

```text
Use list_tables for catalog main schema sales with name_filter="fact_"
```

### `get_table`

Use this when you want full schema metadata for one table.

Examples:

```text
Use get_table for main.sales.orders
```

```text
Use get_table for main.sales.orders and summarize all columns and types
```

### `search_tables`

Use this when you want table discovery across schemas or catalogs.

Examples:

```text
Use search_tables for name_pattern="orders"
```

```text
Use search_tables for name_pattern="fact_" in catalog main
```

### `search_columns`

Use this when you want to know where a column exists.

Examples:

```text
Use search_columns for column_name_pattern="customer_id"
```

```text
Use search_columns for column_name_pattern="order_total" in catalog main schema sales
```

## Compute management tools

### `list_compute`

Use this when you want the agent to discover accessible interactive clusters.

Examples:

```text
Use list_compute and show me running clusters
```

```text
Use list_compute with include_terminated=true and show me any cluster I could restart
```

### `manage_cluster`

Use this when you want the agent to inspect cluster state or start a terminated cluster.

Examples:

```text
Use manage_cluster action="status" for cluster 0522-121745-8myg24rm
```

```text
Use manage_cluster action="start" for cluster 0522-121745-8myg24rm
```

## Recommended prompt patterns

### New notebook development

```text
Create a new Databricks notebook with execute_notebook, save it at /Workspace/Users/me/demo-notebook, run it on serverless, then inspect the run with get_job_run_output and get_job_run_export
```

### Existing notebook modification

```text
Update the notebook /Workspace/Users/me/demo-notebook, rerun it with execute_notebook on cluster 0522-121745-8myg24rm, then use get_job_run_output to review logs and get_job_run_export to inspect rendered output
```

### Cluster REPL iteration

```text
Use execute_code with compute_type="cluster" for interactive iteration on cluster 0522-121745-8myg24rm, and keep reusing the same context_id
```

### Job investigation

```text
Use list_jobs to find the sales pipeline jobs, then use list_job_runs and get_job_run to identify the latest failure, and use get_job_run_output if it is a notebook-backed run
```

### Metadata discovery

```text
Use list_catalogs, list_schemas, list_tables, and get_table to find the orders tables and summarize their schemas without using query_sql unless result rows are required
```

### Pipeline troubleshooting

```text
Use list_pipelines to find the sales pipeline, then list_pipeline_updates and get_pipeline_update to explain the latest failure
```
