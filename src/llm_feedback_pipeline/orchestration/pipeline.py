from __future__ import annotations

from llm_feedback_pipeline.ingestion.sources import Source, load_all
from llm_feedback_pipeline.transform.cleaning import normalize_rows
from llm_feedback_pipeline.load.bigquery import load_to_bigquery


def run_pipeline(sources: list[Source], dataset: str, table: str) -> None:
    raw_rows = load_all(sources)
    curated_rows = normalize_rows(raw_rows)
    load_to_bigquery(curated_rows, dataset, table)
