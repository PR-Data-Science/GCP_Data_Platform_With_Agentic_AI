from __future__ import annotations

import yaml
from llm_feedback_pipeline.ingestion.sources import Source
from llm_feedback_pipeline.orchestration.pipeline import run_pipeline


def main() -> None:
    with open("config/settings.yaml", "r", encoding="utf-8") as handle:
        settings = yaml.safe_load(handle)

    sources = [Source(**source) for source in settings["ingestion"]["sources"]]
    dataset = settings["bigquery"]["dataset"]
    table = settings["bigquery"]["table"]

    run_pipeline(sources, dataset, table)


if __name__ == "__main__":
    main()
