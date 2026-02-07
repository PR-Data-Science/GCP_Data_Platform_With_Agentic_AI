from __future__ import annotations

from google.cloud import bigquery


def load_to_bigquery(rows: list[dict], dataset: str, table: str) -> None:
    client = bigquery.Client()
    table_id = f"{client.project}.{dataset}.{table}"
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
