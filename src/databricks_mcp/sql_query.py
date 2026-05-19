from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
    StatementParameterListItem,
    StatementResponse,
    StatementState,
)

_DATABRICKS_DIALECT = "databricks"
_WAIT_TIMEOUT = "10s"
_POLL_INTERVAL_SECONDS = 2
_DEFAULT_POLL_TIMEOUT_SECONDS = 120


@dataclass(frozen=True, slots=True)
class PreparedQuery:
    statement: str
    parameters: list[StatementParameterListItem]


class QueryValidationError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        recommendations: list[str] | None = None,
        error_type: str = "validation_error",
    ) -> None:
        super().__init__(message)
        self.recommendations = recommendations or []
        self.error_type = error_type

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": "error",
            "error_type": self.error_type,
            "message": str(self),
        }
        if self.recommendations:
            payload["recommendations"] = self.recommendations
        return payload


def prepare_safe_query(query: str) -> PreparedQuery:
    cleaned_query = _strip_markdown_code_fence(query).strip()
    if not cleaned_query:
        raise QueryValidationError(
            "A non-empty SQL query is required.",
            recommendations=[
                "Submit a single SELECT query or a WITH ... SELECT query.",
            ],
        )

    try:
        statements = sqlglot.parse(cleaned_query, read=_DATABRICKS_DIALECT)
    except ParseError as exc:
        raise QueryValidationError(
            "The SQL query could not be parsed into a valid SELECT statement.",
            recommendations=[
                "Submit a single SELECT query or a WITH ... SELECT query.",
                "Move free-form values into simple literals or parameter markers.",
            ],
        ) from exc

    if not statements:
        raise QueryValidationError(
            "No SQL statement was found. Provide a single SELECT query with actual SQL text.",
            recommendations=[
                "Submit exactly one SELECT statement without a trailing semicolon.",
                "If you included only comments, add the SELECT statement after the comments.",
            ],
        )

    if len(statements) != 1 or cleaned_query.rstrip().endswith(";"):
        raise QueryValidationError(
            "Semicolons and multi-statement SQL are not allowed because they can hide SQL injection attempts.",
            error_type="unsafe_query",
            recommendations=[
                "Submit exactly one SELECT statement without a trailing semicolon.",
                "Run multiple steps as separate approved queries instead of chaining statements.",
                "Prefer parameter markers for user-supplied values, for example WHERE id = :customer_id.",
            ],
        )

    expression = statements[0]
    if expression is None:
        raise QueryValidationError(
            "No SQL statement was found. Provide a single SELECT query with actual SQL text.",
            recommendations=[
                "Submit exactly one SELECT statement without a trailing semicolon.",
                "If you included only comments, add the SELECT statement after the comments.",
            ],
        )

    if not isinstance(expression, exp.Query):
        raise QueryValidationError(
            "Only SELECT statements are allowed. WITH clauses are supported when they resolve to a SELECT query.",
            error_type="unsafe_query",
            recommendations=[
                "Rewrite the request as a single SELECT statement.",
                "Remove UPDATE, INSERT, DELETE, DROP, CREATE, ALTER, MERGE, and other non-SELECT operations.",
            ],
        )

    parameterizer = _LiteralParameterizer()
    sanitized_expression = parameterizer.parameterize(expression)

    return PreparedQuery(
        statement=sanitized_expression.sql(dialect=_DATABRICKS_DIALECT),
        parameters=parameterizer.parameters,
    )


def execute_safe_query(
    *,
    client: Any,
    warehouse_id: str,
    query: str,
    catalog: str | None = None,
    schema: str | None = None,
    poll_timeout_seconds: int = _DEFAULT_POLL_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    prepared = prepare_safe_query(query)
    statement_execution = client.statement_execution
    response = statement_execution.execute_statement(
        statement=prepared.statement,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        wait_timeout=_WAIT_TIMEOUT,
        parameters=prepared.parameters,
    )

    state = _get_state(response)
    if state in {StatementState.PENDING, StatementState.RUNNING}:
        response = _poll_for_completion(
            statement_execution=statement_execution,
            statement_id=response.statement_id,
            poll_timeout_seconds=poll_timeout_seconds,
        )
        if isinstance(response, dict):
            return response
        state = _get_state(response)

    if state != StatementState.SUCCEEDED:
        return _statement_error_response(response, prepared.statement)

    return _success_response(response, prepared.statement)


def _poll_for_completion(
    *,
    statement_execution: Any,
    statement_id: str | None,
    poll_timeout_seconds: int,
) -> StatementResponse | dict[str, Any]:
    if not statement_id:
        return {
            "status": "error",
            "error_type": "statement_execution_error",
            "message": "The statement is still running but Databricks did not return a statement_id for polling.",
        }

    deadline = time.monotonic() + poll_timeout_seconds
    while True:
        response = statement_execution.get_statement(statement_id)
        if _get_state(response) not in {StatementState.PENDING, StatementState.RUNNING}:
            return response

        if time.monotonic() >= deadline:
            statement_execution.cancel_execution(statement_id)
            return {
                "status": "error",
                "error_type": "timeout",
                "statement_id": statement_id,
                "message": (
                    f"The query execution took too long and was canceled after {_format_poll_timeout(poll_timeout_seconds)} of polling. "
                    "Add a tighter LIMIT clause or provide finer-grained parameter values before retrying."
                ),
            }

        time.sleep(_POLL_INTERVAL_SECONDS)


def _success_response(response: StatementResponse, sanitized_statement: str) -> dict[str, Any]:
    columns = []
    column_names = []
    if response.manifest and response.manifest.schema and response.manifest.schema.columns:
        for column in response.manifest.schema.columns:
            type_name = column.type_text or (column.type_name.value if column.type_name else None)
            columns.append({"name": column.name, "type": type_name})
            column_names.append(column.name or "")

    data_array = response.result.data_array if response.result and response.result.data_array else []
    rows = [dict(zip(column_names, row, strict=False)) for row in data_array]

    return {
        "status": "ok",
        "statement_id": response.statement_id,
        "statement": sanitized_statement,
        "columns": columns,
        "rows": rows,
        "row_count": response.result.row_count if response.result else 0,
        "truncated": response.manifest.truncated if response.manifest else False,
    }


def _statement_error_response(
    response: StatementResponse,
    sanitized_statement: str,
) -> dict[str, Any]:
    state = _get_state(response)
    error_message = None
    if response.status and response.status.error:
        error_message = response.status.error.message

    return {
        "status": "error",
        "error_type": "statement_execution_error",
        "statement_id": response.statement_id,
        "state": state.value if state else None,
        "statement": sanitized_statement,
        "message": error_message or f"Databricks returned statement state {state.value if state else 'UNKNOWN'}.",
    }


def _get_state(response: StatementResponse) -> StatementState | None:
    if response.status:
        return response.status.state
    return None


def _strip_markdown_code_fence(query: str) -> str:
    match = re.fullmatch(r"\s*```(?:sql)?\s*(.*?)\s*```\s*", query, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1)
    return query


class _LiteralParameterizer:
    def __init__(self) -> None:
        self._index = 0
        self.parameters: list[StatementParameterListItem] = []

    def parameterize(self, expression: exp.Expression) -> exp.Expression:
        return expression.transform(self._replace)

    def _replace(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
            if _should_preserve_numeric_literal(node):
                return node
            return self._placeholder(value=f"-{node.this.this}", value_type=_numeric_type(node.this.this))

        if isinstance(node, exp.Literal):
            value = node.this
            if not node.is_string and _should_preserve_numeric_literal(node):
                return node
            value_type = "STRING" if node.is_string else _numeric_type(value)
            return self._placeholder(value=value, value_type=value_type)

        if isinstance(node, exp.Boolean):
            return self._placeholder(value="TRUE" if node.this else "FALSE", value_type="BOOLEAN")

        if isinstance(node, exp.Null):
            return self._placeholder(value=None, value_type=None)

        return node

    def _placeholder(self, *, value: str | None, value_type: str | None) -> exp.Expression:
        self._index += 1
        name = f"p{self._index}"
        self.parameters.append(
            StatementParameterListItem(
                name=name,
                type=value_type,
                value=value,
            )
        )
        return exp.Placeholder(this=name)


def _numeric_type(value: str) -> str:
    if re.search(r"[.eE]", value):
        return "DOUBLE"
    return "BIGINT"


def _format_poll_timeout(seconds: int) -> str:
    if seconds % 60 == 0:
        minutes = seconds // 60
        return f"{minutes} minute" if minutes == 1 else f"{minutes} minutes"
    return f"{seconds} seconds"


def _should_preserve_numeric_literal(node: exp.Expression) -> bool:
    return node.find_ancestor(exp.Limit, exp.Offset, exp.Fetch) is not None
