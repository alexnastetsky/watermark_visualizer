"""Run SQL via the Databricks Statement Execution API and return pandas DataFrames."""
from __future__ import annotations

import time

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def execute_sql(client: WorkspaceClient, warehouse_id: str, sql: str, timeout_s: int = 60) -> pd.DataFrame:
    """Run a SQL statement and return its result as a DataFrame.

    Uses synchronous polling — fine for small DESCRIBE HISTORY / aggregation queries.
    """
    statement = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="0s",  # async; we'll poll
    )

    deadline = time.monotonic() + timeout_s
    while True:
        status = client.statement_execution.get_statement(statement.statement_id)
        state = status.status.state
        if state in (StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            break
        if time.monotonic() > deadline:
            client.statement_execution.cancel_execution(statement.statement_id)
            raise TimeoutError(f"SQL did not complete within {timeout_s}s: {sql[:120]}")
        time.sleep(0.5)

    if state != StatementState.SUCCEEDED:
        err = getattr(status.status, "error", None)
        msg = err.message if err and hasattr(err, "message") else str(state)
        raise RuntimeError(f"SQL failed ({state}): {msg}\n{sql[:200]}")

    schema = status.manifest.schema
    columns = [c.name for c in schema.columns] if schema and schema.columns else []
    rows: list[list] = []
    if status.result and status.result.data_array:
        rows.extend(status.result.data_array)

    # Fetch additional chunks if there are any
    next_chunk = status.result.next_chunk_index if status.result else None
    while next_chunk is not None:
        chunk = client.statement_execution.get_statement_result_chunk_n(statement.statement_id, next_chunk)
        if chunk.data_array:
            rows.extend(chunk.data_array)
        next_chunk = chunk.next_chunk_index

    return pd.DataFrame(rows, columns=columns)
