"""Batch ingestion to JSONL and GCS."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator
from uuid import uuid4

from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

from src.ingestion.normalize_row import normalize_payload
from src.ingestion.readers import iter_csv, iter_json_array
from src.ops.ops_writer import write_pipeline_runs
from src.utils.hashing import record_hash_from_payload, schema_hash_from_keys
from src.utils.io import load_yaml, today_yyyy_mm_dd, utc_now_iso


def _read_rows(source_type: str, path: str) -> Iterator[dict]:
    if source_type == "json":
        return iter_json_array(path)
    if source_type == "csv":
        return iter_csv(path)
    raise ValueError(f"Unsupported source type: {source_type}")


def _get_raw_bucket(config: dict) -> str:
    try:
        return config["gcp"]["gcs"]["raw_bucket"]
    except KeyError as exc:
        raise KeyError("Missing config key: gcp.gcs.raw_bucket") from exc


def _get_project_id(config: dict) -> str:
    try:
        return config["gcp"]["project_id"]
    except KeyError as exc:
        raise KeyError("Missing config key: gcp.project_id") from exc


def _ensure_input_exists(path: str) -> None:
    if not Path(path).is_file():
        raise FileNotFoundError(f"Input file not found: {path}")


def _write_jsonl(path: Path, records: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


def _upload_to_gcs(local_path: Path, bucket_name: str, blob_path: str, project_id: str) -> str:
    try:
        client = storage.Client(project=project_id)
    except DefaultCredentialsError as exc:
        raise DefaultCredentialsError(
            "Missing GCP credentials. Run `gcloud auth application-default login`."
        ) from exc
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(local_path))
    return f"gs://{bucket_name}/{blob_path}"


def _upload_text_to_gcs(text: str, bucket_name: str, blob_path: str, project_id: str) -> str:
    try:
        client = storage.Client(project=project_id)
    except DefaultCredentialsError as exc:
        raise DefaultCredentialsError(
            "Missing GCP credentials. Run `gcloud auth application-default login`."
        ) from exc
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(text, content_type="application/json")
    return f"gs://{bucket_name}/{blob_path}"


def _derive_batch_id(payload: dict, input_path: str) -> str:
    batch_id = payload.get("batch_id")
    if batch_id:
        return str(batch_id)
    return Path(input_path).stem


def run_ingestion(
    config_path: str,
    input_path: str,
    source_name: str,
    source_type: str,
    pod_name: str,
    pod_type: str,
    task_type: str,
    env: str = "dev",
    code_version: str = "unknown",
    ops_dataset: str = "ops",
) -> None:
    started_at = datetime.now(timezone.utc)
    _ensure_input_exists(input_path)
    config = load_yaml(config_path)
    raw_bucket = _get_raw_bucket(config)
    project_id = _get_project_id(config)

    run_id = str(uuid4())
    ingest_ts = utc_now_iso()
    input_name = Path(input_path).name

    rows = _read_rows(source_type, input_path)

    jsonl_records: list[dict] = []
    schema_hash = None
    batch_id = None

    for idx, row in enumerate(rows, start=1):
        payload = normalize_payload(row, source_type)
        if schema_hash is None:
            schema_hash = schema_hash_from_keys(sorted(payload.keys()))
        record_hash = record_hash_from_payload(payload)
        if batch_id is None:
            batch_id = _derive_batch_id(payload, input_path)

        meta = {
            "run_id": run_id,
            "ingest_ts": ingest_ts,
            "source_type": source_type,
            "source_name": source_name,
            "source_file": input_name,
            "source_uri": input_path,
            "pod_name": pod_name,
            "pod_type": pod_type,
            "task_type": task_type,
            "schema_hash": schema_hash,
            "record_hash": record_hash,
            "row_number": idx,
        }
        jsonl_records.append({"meta": meta, "payload": payload})

    if batch_id is None:
        batch_id = _derive_batch_id({}, input_path)

    output_dir = Path("tmp") / "raw_jsonl"
    output_path = output_dir / "part-00000.jsonl"
    record_count = _write_jsonl(output_path, jsonl_records)

    gcs_path = (
        f"raw/{source_name}/dt={today_yyyy_mm_dd()}/run_id={run_id}"
        f"/batch_id={batch_id}/part-00000.jsonl"
    )
    gcs_uri = _upload_to_gcs(output_path, raw_bucket, gcs_path, project_id)

    finished_at = datetime.now(timezone.utc)
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    manifest_blob_path = f"manifests/raw/dt={today_yyyy_mm_dd()}/run_id={run_id}/manifest.json"
    manifest_payload = {
        "run_id": run_id,
        "stage": "raw",
        "env": env,
        "source_type": source_type,
        "input_paths": [input_path],
        "output_paths": [gcs_uri],
        "schema_hash": schema_hash,
        "record_count_in": record_count,
        "record_count_out": record_count,
        "deadletter_count": 0,
        "dataproc_batch_id": None,
        "start_ts": started_at.isoformat().replace("+00:00", "Z"),
        "end_ts": finished_at.isoformat().replace("+00:00", "Z"),
        "duration_ms": duration_ms,
        "code_version": code_version,
        "error_category": None,
        "error_code": None,
    }
    manifest_uri = _upload_text_to_gcs(
        text=json.dumps(manifest_payload, ensure_ascii=False),
        bucket_name=raw_bucket,
        blob_path=manifest_blob_path,
        project_id=project_id,
    )

    pipeline_run_row = {
        "run_id": run_id,
        "stage": "raw",
        "status": "SUCCEEDED",
        "start_ts": started_at.isoformat().replace("+00:00", "Z"),
        "end_ts": finished_at.isoformat().replace("+00:00", "Z"),
        "duration_ms": duration_ms,
        "input_count": record_count,
        "output_count": record_count,
        "deadletter_count": 0,
        "schema_hash": schema_hash,
        "dataproc_batch_id": None,
        "manifest_path": manifest_uri,
        "error_category": None,
        "error_code": None,
        "error_summary": None,
        "code_version": code_version,
        "input_paths": [input_path],
        "output_paths": [gcs_uri],
        "partition_keys": [f"dt={today_yyyy_mm_dd()}", f"run_id={run_id}"],
    }
    try:
        write_pipeline_runs([pipeline_run_row], dataset=ops_dataset)
    except Exception as exc:  # best-effort ops write to avoid blocking ingestion path
        print(f"warn_ops_pipeline_runs_write_failed={exc}")

    print(f"run_id={run_id}")
    print(f"batch_id={batch_id}")
    print(f"record_count={record_count}")
    print(f"final_gcs_uri={gcs_uri}")
    print(f"manifest_uri={manifest_uri}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest batch drop to GCS JSONL.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--source-type", required=True, choices=["csv", "json"])
    parser.add_argument("--pod-name", required=True)
    parser.add_argument("--pod-type", required=True)
    parser.add_argument("--task-type", required=True)
    parser.add_argument("--env", default="dev")
    parser.add_argument("--code-version", default="unknown")
    parser.add_argument("--ops-dataset", default="ops")
    args = parser.parse_args()

    run_ingestion(
        config_path=args.config,
        input_path=args.input,
        source_name=args.source_name,
        source_type=args.source_type,
        pod_name=args.pod_name,
        pod_type=args.pod_type,
        task_type=args.task_type,
        env=args.env,
        code_version=args.code_version,
        ops_dataset=args.ops_dataset,
    )


if __name__ == "__main__":
    main()
