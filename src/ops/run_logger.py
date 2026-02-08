"""Pipeline runner entrypoint."""

from __future__ import annotations

from ingestion.api_to_gcs import ApiSource, load_all_api
from ingestion.batch_to_gcs import BatchSource, load_all_batch
from utils.io import load_to_bigquery, normalize_rows


def run_pipeline(
    api_sources: list[ApiSource],
    batch_sources: list[BatchSource],
    dataset: str,
    table: str,
) -> None:
    raw_rows = []
    raw_rows.extend(load_all_api(api_sources))
    raw_rows.extend(load_all_batch(batch_sources))
    curated_rows = normalize_rows(raw_rows)
    load_to_bigquery(curated_rows, dataset, table)
