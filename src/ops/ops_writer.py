"""Helpers to write pipeline operational artifacts into BigQuery ops tables."""

from __future__ import annotations

from typing import Iterable

try:
    from google.cloud import bigquery
except ImportError:  # pragma: no cover - optional at import time
    bigquery = None

try:
    from src.utils.io import load_to_bigquery
except ImportError:
    from utils.io import load_to_bigquery


DEFAULT_DATASET = "ops"


def _write_rows(table: str, rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    prepared = list(rows)
    if not prepared:
        return
    load_to_bigquery(prepared, dataset=dataset, table=table)


def write_pipeline_runs(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="pipeline_runs", rows=rows, dataset=dataset)


def write_dq_results(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="dq_results", rows=rows, dataset=dataset)


def write_deadletter_summary(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="deadletter_summary", rows=rows, dataset=dataset)


def write_schema_registry(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="schema_registry", rows=rows, dataset=dataset)


def write_schema_registry_first_seen(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    prepared = [row for row in rows if row.get("schema_hash") and row.get("schema_json") and row.get("first_seen_run_id")]
    if not prepared:
        return

    if bigquery is None:
        write_schema_registry(prepared, dataset=dataset)
        return

    client = bigquery.Client()
    table_id = f"{client.project}.{dataset}.schema_registry"
    requested_hashes = sorted({row["schema_hash"] for row in prepared})

    query = f"SELECT schema_hash FROM `{table_id}` WHERE schema_hash IN UNNEST(@hashes)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("hashes", "STRING", requested_hashes),
        ]
    )

    try:
        existing_hashes = {row["schema_hash"] for row in client.query(query, job_config=job_config).result()}
    except Exception:
        existing_hashes = set()

    insert_rows = [row for row in prepared if row["schema_hash"] not in existing_hashes]
    if insert_rows:
        write_schema_registry(insert_rows, dataset=dataset)


def write_dq_rule_registry(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="dq_rule_registry", rows=rows, dataset=dataset)


def write_agent_sessions(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="agent_sessions", rows=rows, dataset=dataset)


def write_agent_tool_calls(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="agent_tool_calls", rows=rows, dataset=dataset)


def write_agent_responses(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="agent_responses", rows=rows, dataset=dataset)


def write_agent_proposals(rows: Iterable[dict], dataset: str = DEFAULT_DATASET) -> None:
    _write_rows(table="agent_proposals", rows=rows, dataset=dataset)
