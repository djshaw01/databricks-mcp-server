# Databricks MCP test prompts

Use these prompts in Copilot Chat / Agent mode to manually verify the MCP server behavior. They are grouped from low-risk discovery prompts to execution and notebook-iteration prompts.

## How to use this file

- Start with **discovery prompts** to confirm the server is reachable and the schema is current.
- Then run the **read-only prompts**.
- Run the **execution prompts** only in a workspace where you are comfortable creating runs or starting clusters.
- Replace placeholder IDs, cluster IDs, workspace paths, catalogs, schemas, and table names with values from your environment.

---

## 1. Basic server discovery

```text
List the Databricks MCP tools you currently have available and group them by jobs, pipelines, catalog, SQL, compute, and notebook execution.
```

```text
Use list_catalogs and summarize the available catalogs.
```

```text
Use list_compute with include_terminated=true and summarize the clusters I can access.
```

---

## 2. Unity Catalog read-only verification

```text
Use list_catalogs and then list_schemas for the main catalog.
```

```text
Use list_tables for catalog main schema default and summarize the first 10 tables.
```

```text
Use search_tables for name_pattern="fact_" in catalog main.
```

```text
Use search_columns for column_name_pattern="customer_id" and summarize which tables contain it.
```

```text
Use get_table for main.default.<TABLE_NAME> and summarize its columns and types.
```

**What to check**

- catalog, schema, table, and column metadata come back as structured results
- the agent prefers metadata tools instead of `query_sql` for discovery questions

---

## 3. SQL query verification

```text
Use query_sql to run SELECT 1 AS answer.
```

```text
Use query_sql to count rows in main.default.<TABLE_NAME>.
```

```text
Use query_sql to select the latest 5 rows from main.default.<TABLE_NAME>.
```

**What to check**

- query results come back as rows/columns
- read-only statements succeed
- the agent uses `query_sql` only when result rows are actually needed

---

## 4. Jobs inspection verification

```text
Use list_jobs and summarize the first 10 jobs.
```

```text
Use list_jobs with name_filter="daily" and show me matching jobs.
```

```text
Use get_job for job <JOB_ID> and summarize its tasks, schedule, and notebook paths.
```

```text
Use list_job_runs for job <JOB_ID> and show me the last 5 runs.
```

```text
Use get_job_run for run <RUN_ID> and summarize the task states.
```

**What to check**

- job metadata and run metadata are structured
- `get_job_run` includes task-level information such as `task_run_id`

---

## 5. Pipeline inspection verification

```text
Use list_pipelines and summarize all DLT pipelines.
```

```text
Use list_pipelines with name_filter="sales" and show me matching pipelines.
```

```text
Use get_pipeline for pipeline <PIPELINE_ID> and summarize its libraries, target schema, and latest updates.
```

```text
Use list_pipeline_updates for pipeline <PIPELINE_ID> and show me the last 5 updates.
```

```text
Use get_pipeline_update for pipeline <PIPELINE_ID> update <UPDATE_ID> and summarize the recent events and errors.
```

**What to check**

- pipeline state and update history are readable
- event/error payloads are surfaced clearly

---

## 6. Compute management verification

```text
Use list_compute and summarize currently running clusters.
```

```text
Use manage_cluster action="status" for cluster <CLUSTER_ID>.
```

```text
Use manage_cluster action="start" for cluster <TERMINATED_CLUSTER_ID>.
```

**What to check**

- cluster status is clear
- start requests return a pending/start message rather than failing silently

---

## 7. `execute_code` verification

### Serverless snippet

```text
Use execute_code on serverless to run this Python snippet: print(1 + 1)
```

### Cluster snippet

```text
Use execute_code with compute_type="cluster" on cluster <CLUSTER_ID> to run this Python snippet: print("hello from cluster")
```

### Cluster REPL reuse

```text
Use execute_code with compute_type="cluster" on cluster <CLUSTER_ID> to run this Python snippet: x = 41; print(x)
Then reuse the returned context_id and run: print(x + 1)
```

### Scala snippet

```text
Use execute_code with compute_type="cluster" on cluster <CLUSTER_ID> to run this Scala snippet: println(42)
```

**What to check**

- `execute_code` prefers snippet/repl behavior
- cluster runs return `context_id`
- serverless runs return `run_id`
- Scala routes to cluster, not serverless

---

## 8. `execute_notebook` verification

### Run an existing notebook on serverless

```text
Use execute_notebook to run the existing notebook /Workspace/Users/me/<NOTEBOOK_PATH> on serverless.
```

### Run an existing notebook on a cluster

```text
Use execute_notebook to run the existing notebook /Workspace/Users/me/<NOTEBOOK_PATH> on cluster <CLUSTER_ID>.
```

### Upload and run a notebook from inline code

```text
Use execute_notebook to create or overwrite /Workspace/Users/me/mcp-test-notebook with this Python notebook content and run it on serverless:

# Databricks notebook source
print("hello notebook")
dbutils.notebook.exit("done")
```

### Upload and run from a local file

```text
Use execute_notebook to upload notebooks/<LOCAL_NOTEBOOK_FILE> to /Workspace/Users/me/mcp-file-test and run it on cluster <CLUSTER_ID>.
```

### Run with notebook parameters

```text
Use execute_notebook to run /Workspace/Users/me/<NOTEBOOK_PATH> on serverless with notebook parameters env=dev and limit=10.
```

**What to check**

- `execute_notebook` returns a `run_id` on both serverless and cluster-backed runs
- cluster-backed notebook execution does not use `context_id`
- existing notebooks can be rerun without requiring inline code
- notebook parameters are accepted

---

## 9. Notebook run follow-up verification

Run one of the `execute_notebook` prompts above, then use the returned `run_id` in the prompts below.

### Run metadata

```text
Use get_job_run for run <RUN_ID> and summarize the task states.
```

### Text/log output

```text
Use get_job_run_output for run <RUN_ID> and summarize the notebook flow.
```

### HTML export

```text
Use get_job_run_export for run <RUN_ID> and inspect the rendered notebook output.
```

### HTML export with all views

```text
Use get_job_run_export for run <RUN_ID> with views_to_export="ALL".
```

**What to check**

- `get_job_run_output` returns text/log output when available
- `get_job_run_export` returns exported HTML view payloads
- the agent knows to follow `execute_notebook` with these tools

---

## 10. Job control verification

### Trigger a job

```text
Use run_job for job <JOB_ID>, then inspect the new run.
```

### Cancel a run

```text
Use cancel_job_run for run <RUN_ID>.
```

**What to check**

- triggering a job returns a `run_id`
- cancellation returns a clear status payload

---

## 11. Negative-path verification

### Invalid compute path for notebook execution

```text
Use execute_notebook with compute_type="cluster" for /Workspace/Users/me/<NOTEBOOK_PATH> but do not provide a cluster_id.
```

### Invalid tool selection guidance

```text
I want to iteratively develop a notebook with rendered output. Choose the best tool and explain why you are not using execute_code.
```

### Invalid run export selection

```text
Use get_job_run_export for run <RUN_ID> with views_to_export="widgets".
```

**What to check**

- validation errors are clear and specific
- the agent prefers `execute_notebook` for notebook workflows

---

## 12. End-to-end notebook iteration scenario

```text
Create a notebook at /Workspace/Users/me/mcp-iteration-demo with execute_notebook on serverless that:
1. prints a greeting
2. queries a small Spark DataFrame
3. exits with dbutils.notebook.exit("iteration complete")

After it runs:
- use get_job_run to inspect task metadata
- use get_job_run_output to summarize the text/log flow
- use get_job_run_export to inspect the rendered notebook HTML
- tell me whether this environment is ready for notebook iteration through MCP
```

**What to check**

- the agent chooses `execute_notebook`
- the resulting `run_id` is reused across the follow-up tools
- the full notebook-iteration workflow works end to end
