import os
import unittest
from unittest.mock import MagicMock, patch

from databricks.sdk.service.sql import (
    ColumnInfo,
    ColumnInfoTypeName,
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    ResultData,
    ResultManifest,
    ResultSchema,
    ServiceError,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from databricks_mcp import server
from databricks_mcp.sql_query import (
    QueryValidationError,
    execute_safe_query,
    prepare_safe_query,
)


def _statement_response(
    *,
    state: StatementState,
    statement_id: str = "stmt-123",
    columns: list[tuple[str, ColumnInfoTypeName]] | None = None,
    rows: list[list[str | None]] | None = None,
    error_message: str | None = None,
) -> StatementResponse:
    manifest = None
    result = None
    if columns is not None:
        manifest = ResultManifest(
            schema=ResultSchema(
                column_count=len(columns),
                columns=[
                    ColumnInfo(name=name, type_name=type_name, type_text=type_name.value)
                    for name, type_name in columns
                ],
            ),
            total_row_count=len(rows or []),
            truncated=False,
        )
        result = ResultData(
            data_array=rows or [],
            row_count=len(rows or []),
            row_offset=0,
        )

    status = StatementStatus(state=state)
    if error_message:
        status.error = ServiceError(message=error_message)

    return StatementResponse(
        statement_id=statement_id,
        status=status,
        manifest=manifest,
        result=result,
    )


class PrepareSafeQueryTests(unittest.TestCase):
    def test_parameterizes_literals_and_allows_ctes(self) -> None:
        prepared = prepare_safe_query(
            """
            WITH recent AS (
                SELECT order_id
                FROM sales.orders
                WHERE customer_id = 42 AND region = 'west'
            )
            SELECT order_id
            FROM recent
            LIMIT 10
            """
        )

        self.assertTrue(prepared.statement.lstrip().upper().startswith("WITH"))
        self.assertNotIn("'west'", prepared.statement)
        self.assertNotIn(" = 42", prepared.statement)
        self.assertIn("LIMIT 10", prepared.statement)
        self.assertEqual(len(prepared.parameters), 2)

    def test_keeps_limit_literal_while_parameterizing_filters(self) -> None:
        prepared = prepare_safe_query(
            "SELECT * FROM dan_test_databricks.pball.draws WHERE draw_id = 7 LIMIT 5"
        )

        self.assertIn("WHERE draw_id = :p1", prepared.statement)
        self.assertIn("LIMIT 5", prepared.statement)
        self.assertEqual(len(prepared.parameters), 1)
        self.assertEqual(prepared.parameters[0].value, "7")

    def test_keeps_fetch_literal_while_parameterizing_filters(self) -> None:
        prepared = prepare_safe_query(
            "SELECT * FROM dan_test_databricks.pball.draws WHERE created_at = 1619710755 FETCH FIRST 3 ROWS ONLY"
        )

        self.assertIn("WHERE created_at = :p1", prepared.statement)
        self.assertIn("LIMIT 3", prepared.statement)
        self.assertEqual(len(prepared.parameters), 1)
        self.assertEqual(prepared.parameters[0].value, "1619710755")

    def test_rejects_semicolons_with_recommendations(self) -> None:
        with self.assertRaises(QueryValidationError) as ctx:
            prepare_safe_query("SELECT * FROM sales.orders; DROP TABLE sales.audit_log")

        self.assertIn("semicolon", str(ctx.exception).lower())
        self.assertTrue(ctx.exception.recommendations)

    def test_rejects_queries_without_parseable_statement(self) -> None:
        with self.assertRaises(QueryValidationError) as ctx:
            prepare_safe_query("-- no statement here")

        self.assertIn("no sql statement was found", str(ctx.exception).lower())
        self.assertNotIn("semicolon", str(ctx.exception).lower())

    def test_rejects_non_select_statements(self) -> None:
        with self.assertRaises(QueryValidationError):
            prepare_safe_query("DELETE FROM sales.orders WHERE order_id = 42")

    def test_parameterizes_boolean_and_null_literals(self) -> None:
        prepared = prepare_safe_query(
            "SELECT * FROM sales.orders WHERE is_active = TRUE AND is_deleted = FALSE AND archived_at IS NULL"
        )

        self.assertEqual(
            [parameter.value for parameter in prepared.parameters],
            ["TRUE", "FALSE", None],
        )
        self.assertEqual(
            [parameter.type for parameter in prepared.parameters],
            ["BOOLEAN", "BOOLEAN", None],
        )


class ExecuteSafeQueryTests(unittest.TestCase):
    def test_executes_statement_and_returns_json_rows(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _statement_response(
            state=StatementState.SUCCEEDED,
            columns=[
                ("order_id", ColumnInfoTypeName.INT),
                ("region", ColumnInfoTypeName.STRING),
            ],
            rows=[["42", "west"]],
        )

        result = execute_safe_query(
            client=client,
            warehouse_id="warehouse-123",
            query="SELECT order_id, region FROM sales.orders WHERE region = 'west'",
            catalog="main",
            schema="sales",
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(
            result["rows"],
            [{"order_id": "42", "region": "west"}],
        )
        self.assertEqual(result["columns"][0]["name"], "order_id")
        self.assertEqual(result["statement_id"], "stmt-123")

        _, kwargs = client.statement_execution.execute_statement.call_args
        self.assertEqual(kwargs["warehouse_id"], "warehouse-123")
        self.assertEqual(kwargs["catalog"], "main")
        self.assertEqual(kwargs["schema"], "sales")
        self.assertEqual(kwargs["disposition"], Disposition.INLINE)
        self.assertEqual(kwargs["format"], Format.JSON_ARRAY)
        self.assertEqual(
            kwargs["on_wait_timeout"],
            ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        )
        self.assertEqual(kwargs["wait_timeout"], "10s")
        self.assertTrue(kwargs["parameters"])

    def test_polls_until_the_statement_finishes(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _statement_response(
            state=StatementState.RUNNING,
        )
        client.statement_execution.get_statement.side_effect = [
            _statement_response(state=StatementState.RUNNING),
            _statement_response(
                state=StatementState.SUCCEEDED,
                columns=[("answer", ColumnInfoTypeName.INT)],
                rows=[["1"]],
            ),
        ]

        monotonic_values = iter([0.0, 1.0, 2.0, 3.0])
        with patch("databricks_mcp.sql_query.time.monotonic", side_effect=lambda: next(monotonic_values)):
            with patch("databricks_mcp.sql_query.time.sleep", return_value=None):
                result = execute_safe_query(
                    client=client,
                    warehouse_id="warehouse-123",
                    query="SELECT 1 AS answer",
                )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["rows"], [{"answer": "1"}])
        self.assertEqual(client.statement_execution.get_statement.call_count, 2)

    def test_polls_immediately_before_sleeping(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _statement_response(
            state=StatementState.RUNNING,
        )
        client.statement_execution.get_statement.return_value = _statement_response(
            state=StatementState.SUCCEEDED,
            columns=[("answer", ColumnInfoTypeName.INT)],
            rows=[["1"]],
        )

        monotonic_values = iter([0.0])
        with patch("databricks_mcp.sql_query.time.monotonic", side_effect=lambda: next(monotonic_values)):
            with patch("databricks_mcp.sql_query.time.sleep", return_value=None) as sleep:
                result = execute_safe_query(
                    client=client,
                    warehouse_id="warehouse-123",
                    query="SELECT 1 AS answer",
                )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(client.statement_execution.get_statement.call_count, 1)
        sleep.assert_not_called()

    def test_cancels_statement_after_two_minutes_of_polling(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _statement_response(
            state=StatementState.RUNNING,
        )
        client.statement_execution.get_statement.return_value = _statement_response(
            state=StatementState.RUNNING,
        )

        monotonic_values = iter([0.0, 1.0, 121.0, 122.0])
        with patch("databricks_mcp.sql_query.time.monotonic", side_effect=lambda: next(monotonic_values)):
            with patch("databricks_mcp.sql_query.time.sleep", return_value=None):
                result = execute_safe_query(
                    client=client,
                    warehouse_id="warehouse-123",
                    query="SELECT 1 AS answer",
                )

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "timeout")
        self.assertIn("finer-grained", result["message"])
        self.assertIn("2 minutes", result["message"])
        client.statement_execution.cancel_execution.assert_called_once_with("stmt-123")

    def test_uses_configured_poll_timeout_in_timeout_message(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _statement_response(
            state=StatementState.RUNNING,
        )
        client.statement_execution.get_statement.return_value = _statement_response(
            state=StatementState.RUNNING,
        )

        monotonic_values = iter([0.0, 1.0, 31.0, 32.0])
        with patch("databricks_mcp.sql_query.time.monotonic", side_effect=lambda: next(monotonic_values)):
            with patch("databricks_mcp.sql_query.time.sleep", return_value=None):
                result = execute_safe_query(
                    client=client,
                    warehouse_id="warehouse-123",
                    query="SELECT 1 AS answer",
                    poll_timeout_seconds=30,
                )

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "timeout")
        self.assertIn("30 seconds", result["message"])

    def test_returns_statement_execution_errors_as_json(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _statement_response(
            state=StatementState.FAILED,
            error_message="TABLE_OR_VIEW_NOT_FOUND: sales.orders",
        )

        result = execute_safe_query(
            client=client,
            warehouse_id="warehouse-123",
            query="SELECT * FROM sales.orders",
        )

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "statement_execution_error")
        self.assertIn("TABLE_OR_VIEW_NOT_FOUND", result["message"])


class QuerySqlToolTests(unittest.TestCase):
    def test_get_client_uses_profile_specific_workspace_config(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_PROFILE_ANALYTICS_HOST": "https://analytics.example.com",
            },
            clear=True,
        ):
            with patch("databricks_mcp.server.WorkspaceClient") as workspace_client:
                server._get_client("analytics")

        workspace_client.assert_called_once_with(
            host="https://analytics.example.com",
            profile="analytics",
        )

    def test_profile_name_normalization_uses_expected_env_keys(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_PROFILE_PROD_WEST_HOST": "https://prod-west.example.com",
                "DATABRICKS_PROFILE_PROD_WEST_WAREHOUSE_ID": "warehouse-prod-west",
            },
            clear=True,
        ):
            with patch("databricks_mcp.server._get_client", return_value=MagicMock()) as get_client:
                with patch("databricks_mcp.server.execute_safe_query", return_value={"status": "ok"}) as execute:
                    result = server.query_sql("SELECT 1", profile="prod-west")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(execute.call_args.kwargs["warehouse_id"], "warehouse-prod-west")
        get_client.assert_called_once_with("prod-west")

    def test_returns_configuration_errors_as_json(self) -> None:
        with patch.dict(os.environ, {"DATABRICKS_HOST": "https://example.com"}, clear=True):
            result = server.query_sql("SELECT 1")

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "configuration_error")
        self.assertIn("DATABRICKS_WAREHOUSE_ID", result["message"])

    def test_uses_environment_poll_timeout_override(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_HOST": "https://example.com",
                "DATABRICKS_WAREHOUSE_ID": "warehouse-123",
                "DATABRICKS_SQL_POLL_TIMEOUT_SECONDS": "45",
            },
            clear=True,
        ):
            with patch("databricks_mcp.server._get_client", return_value=MagicMock()):
                with patch("databricks_mcp.server.execute_safe_query", return_value={"status": "ok"}) as execute:
                    result = server.query_sql("SELECT 1")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(execute.call_args.kwargs["poll_timeout_seconds"], 45)

    def test_uses_profile_specific_workspace_configuration(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_PROFILE_ANALYTICS_HOST": "https://analytics.example.com",
                "DATABRICKS_PROFILE_ANALYTICS_WAREHOUSE_ID": "warehouse-analytics",
                "DATABRICKS_PROFILE_ANALYTICS_SQL_POLL_TIMEOUT_SECONDS": "45",
            },
            clear=True,
        ):
            with patch("databricks_mcp.server._get_client", return_value=MagicMock()) as get_client:
                with patch("databricks_mcp.server.execute_safe_query", return_value={"status": "ok"}) as execute:
                    result = server.query_sql("SELECT 1", profile="analytics")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(execute.call_args.kwargs["warehouse_id"], "warehouse-analytics")
        self.assertEqual(execute.call_args.kwargs["poll_timeout_seconds"], 45)
        get_client.assert_called_once_with("analytics")

    def test_per_request_poll_timeout_override_takes_precedence(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_PROFILE_ANALYTICS_HOST": "https://analytics.example.com",
                "DATABRICKS_PROFILE_ANALYTICS_WAREHOUSE_ID": "warehouse-analytics",
                "DATABRICKS_PROFILE_ANALYTICS_SQL_POLL_TIMEOUT_SECONDS": "45",
            },
            clear=True,
        ):
            with patch("databricks_mcp.server._get_client", return_value=MagicMock()):
                with patch("databricks_mcp.server.execute_safe_query", return_value={"status": "ok"}) as execute:
                    result = server.query_sql(
                        "SELECT 1",
                        profile="analytics",
                        poll_timeout_seconds=30,
                    )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(execute.call_args.kwargs["poll_timeout_seconds"], 30)

    def test_rejects_non_positive_per_request_poll_timeout_override(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_HOST": "https://example.com",
                "DATABRICKS_WAREHOUSE_ID": "warehouse-123",
            },
            clear=True,
        ):
            result = server.query_sql("SELECT 1", poll_timeout_seconds=0)

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "configuration_error")
        self.assertIn("poll_timeout_seconds", result["message"])

    def test_profile_uses_default_poll_timeout_when_profile_timeout_is_missing(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_PROFILE_ANALYTICS_HOST": "https://analytics.example.com",
                "DATABRICKS_PROFILE_ANALYTICS_WAREHOUSE_ID": "warehouse-analytics",
                "DATABRICKS_SQL_POLL_TIMEOUT_SECONDS": "50",
            },
            clear=True,
        ):
            with patch("databricks_mcp.server._get_client", return_value=MagicMock()):
                with patch("databricks_mcp.server.execute_safe_query", return_value={"status": "ok"}) as execute:
                    result = server.query_sql("SELECT 1", profile="analytics")

        self.assertEqual(result["status"], "ok")
        self.assertEqual(execute.call_args.kwargs["poll_timeout_seconds"], 50)

    def test_rejects_invalid_environment_poll_timeout(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_HOST": "https://example.com",
                "DATABRICKS_WAREHOUSE_ID": "warehouse-123",
                "DATABRICKS_SQL_POLL_TIMEOUT_SECONDS": "abc",
            },
            clear=True,
        ):
            result = server.query_sql("SELECT 1")

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "configuration_error")
        self.assertIn("DATABRICKS_SQL_POLL_TIMEOUT_SECONDS", result["message"])

    def test_requires_profile_host_when_profile_is_selected(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_PROFILE_ANALYTICS_WAREHOUSE_ID": "warehouse-analytics",
            },
            clear=True,
        ):
            result = server.query_sql("SELECT 1", profile="analytics")

        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error_type"], "configuration_error")
        self.assertIn("DATABRICKS_PROFILE_ANALYTICS_HOST", result["message"])


if __name__ == "__main__":
    unittest.main()
