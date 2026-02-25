from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime
from typing import Any
from uuid import uuid4

import pendulum
from airflow.decorators import dag, get_current_context, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "llm_feedback_full_e2e_composer"
CONFIG_VAR = "llm_feedback_composer_config"


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _to_json_text(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sample_execution_json(prompt_id: str, query_text: str, step_index: int) -> dict[str, Any]:
    return {
        "prompt_id": prompt_id,
        "query_text": query_text,
        "steps": [
            {
                "step_index": max(0, step_index - 1),
                "step_type": "search",
                "tool_name": "search",
                "query": f"Search for context for {prompt_id}",
                "tool_output": "Retrieved relevant snippets.",
                "partial_answer": "Drafting answer.",
            },
            {
                "step_index": step_index,
                "step_type": "tool",
                "tool_name": "calculator",
                "query": "Compute final score",
                "tool_output": "Score=4.2",
                "partial_answer": "Refining answer.",
            },
        ],
        "final_answer": f"Recommended next steps for {prompt_id}",
        "model_version": "gemini-1.5-pro",
    }


def _sample_rating(base: float) -> dict[str, Any]:
    return {
        "primary_intent": round(base, 2),
        "information_gain": round(base + 0.1, 2),
        "reasoning": round(base + 0.2, 2),
        "understanding": round(base + 0.1, 2),
        "implementation": round(base + 0.15, 2),
        "trajectory_robustness": round(base + 0.05, 2),
        "overall_label": "LLM_RATED_GOOD" if base >= 3.5 else "LLM_RATED_OK",
    }


def _sample_auto_rater(base: float) -> dict[str, Any]:
    return {
        "scores": _sample_rating(base),
        "violations": [],
        "confidence": 0.87,
        "rubric_version": "v1",
        "model_version": "auto-rater-v1",
        "final_reviewer": "auto",
    }


@task
def load_config() -> dict[str, Any]:
    config = Variable.get(CONFIG_VAR, default_var=None, deserialize_json=True)
    if not config:
        raise AirflowException(f"Missing Airflow Variable '{CONFIG_VAR}'")

    required = [
        "project_id",
        "region",
        "service_account",
        "raw_bucket",
        "bronze_bucket",
        "silver_bucket",
        "gold_bucket",
    ]
    missing = [key for key in required if not config.get(key)]
    if missing:
        raise AirflowException(f"Missing required config keys in '{CONFIG_VAR}': {', '.join(missing)}")

    config.setdefault("gcp_conn_id", "google_cloud_default")
    config.setdefault("env", "dev")
    config.setdefault("source_name", "llm_feedback_eval")
    config.setdefault("datasource_prefix", "datasource")
    config.setdefault("raw_prefix", "raw")
    config.setdefault("bronze_prefix", "bronze")
    config.setdefault("silver_prefix", "silver")
    config.setdefault("gold_prefix", "gold")
    config.setdefault("pod_name", "Magi_Code_Python")
    config.setdefault("pod_type", "vertical")
    config.setdefault("task_type", "EAC_NEXT_STEPS_SIMPLIFIED")
    config.setdefault("auto_batch_id", "python_training_version1_LLMrated_batch")
    config.setdefault("human_batch_id", "python_training_version1_HUMANrated_batch")
    config.setdefault("record_count_per_batch", 12)
    config.setdefault(
        "dataproc_properties",
        {
            "spark.dynamicAllocation.enabled": "false",
            "spark.executor.instances": "2",
            "spark.executor.cores": "4",
            "spark.driver.cores": "4",
        },
    )
    return config


@task
def resolve_ingest_date() -> str:
    context = get_current_context()
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("ingest_date"):
        return dag_run.conf["ingest_date"]
    return context["data_interval_end"].in_timezone("UTC").format("YYYY-MM-DD")


@task
def generate_source_batches(config: dict[str, Any], ingest_date: str) -> list[dict[str, Any]]:
    hook = GCSHook(gcp_conn_id=config["gcp_conn_id"])

    source_prefix = str(config["datasource_prefix"]).strip("/")
    source_name = config["source_name"]
    task_type = config["task_type"]
    pod_name = config["pod_name"]
    pod_type = config["pod_type"]
    count = int(config["record_count_per_batch"])

    json_records: list[dict[str, Any]] = []
    csv_rows: list[dict[str, Any]] = []

    for idx in range(count):
        prompt_id = f"prompt_{1000 + idx}"
        set_id = f"set_{1 + (idx // 3):03d}"
        query_text = f"Provide next steps for EAC scenario {1000 + idx}."
        step_index = idx % 6

        execution_json = _sample_execution_json(prompt_id, query_text, step_index)
        curator_1 = _sample_rating(3.6)
        curator_2 = _sample_rating(3.7)
        reviewer_1 = _sample_rating(3.8)
        reviewer_2 = _sample_rating(3.9)
        auto_rater = _sample_auto_rater(3.75)

        common_payload = {
            "pod_name": pod_name,
            "pod_type": pod_type,
            "task_type": task_type,
            "set_id": set_id,
            "prompt_id": prompt_id,
            "query_text": query_text,
            "step_index": step_index,
            "evaluated_step_index": step_index,
            "step_type": "tool" if idx % 2 == 0 else "search",
            "execution_json": execution_json,
            "violations": [],
            "curator_1_rating": curator_1,
            "curator_2_rating": curator_2,
            "reviewer_curator_1_rating": reviewer_1,
            "reviewer_curator_2_rating": reviewer_2,
            "auto_rater": auto_rater,
        }

        json_records.append(
            {
                **common_payload,
                "batch_id": config["auto_batch_id"],
                "batch_name": config["auto_batch_id"],
            }
        )

        csv_rows.append(
            {
                **common_payload,
                "batch_id": config["human_batch_id"],
                "batch_name": config["human_batch_id"],
                "execution_json": _to_json_text(common_payload["execution_json"]),
                "violations": _to_json_text(common_payload["violations"]),
                "curator_1_rating": _to_json_text(common_payload["curator_1_rating"]),
                "curator_2_rating": _to_json_text(common_payload["curator_2_rating"]),
                "reviewer_curator_1_rating": _to_json_text(common_payload["reviewer_curator_1_rating"]),
                "reviewer_curator_2_rating": _to_json_text(common_payload["reviewer_curator_2_rating"]),
                "auto_rater": _to_json_text(common_payload["auto_rater"]),
            }
        )

    json_blob_name = (
        f"{source_prefix}/{source_name}/source_type=json/dt={ingest_date}/"
        f"batch_id={config['auto_batch_id']}/{config['auto_batch_id']}.json"
    )
    json_text = _to_json_text(json_records)
    hook.upload(bucket_name=config["raw_bucket"], object_name=json_blob_name, data=json_text)

    csv_blob_name = (
        f"{source_prefix}/{source_name}/source_type=csv/dt={ingest_date}/"
        f"batch_id={config['human_batch_id']}/{config['human_batch_id']}.csv"
    )
    csv_buf = io.StringIO()
    writer = csv.DictWriter(csv_buf, fieldnames=list(csv_rows[0].keys()))
    writer.writeheader()
    writer.writerows(csv_rows)
    hook.upload(bucket_name=config["raw_bucket"], object_name=csv_blob_name, data=csv_buf.getvalue())

    return [
        {
            "source_type": "json",
            "batch_id": config["auto_batch_id"],
            "source_object": json_blob_name,
            "source_uri": f"gs://{config['raw_bucket']}/{json_blob_name}",
        },
        {
            "source_type": "csv",
            "batch_id": config["human_batch_id"],
            "source_object": csv_blob_name,
            "source_uri": f"gs://{config['raw_bucket']}/{csv_blob_name}",
        },
    ]


@task
def ingest_generated_batches(
    config: dict[str, Any],
    ingest_date: str,
    source_batches: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    hook = GCSHook(gcp_conn_id=config["gcp_conn_id"])

    raw_prefix = str(config["raw_prefix"]).strip("/")
    source_name = config["source_name"]

    def parse_csv(content: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        reader = csv.DictReader(io.StringIO(content))
        json_like_fields = {
            "execution_json",
            "violations",
            "curator_1_rating",
            "curator_2_rating",
            "reviewer_curator_1_rating",
            "reviewer_curator_2_rating",
            "auto_rater",
        }
        for row in reader:
            out: dict[str, Any] = {}
            for key, value in row.items():
                if key in json_like_fields and value:
                    try:
                        out[key] = json.loads(value)
                    except json.JSONDecodeError:
                        out[key] = value
                elif key in {"step_index", "evaluated_step_index"} and value is not None:
                    out[key] = int(value)
                else:
                    out[key] = value
            rows.append(out)
        return rows

    outputs: list[dict[str, Any]] = []

    for source in source_batches:
        source_text = hook.download(bucket_name=config["raw_bucket"], object_name=source["source_object"]).decode("utf-8")

        if source["source_type"] == "json":
            records = json.loads(source_text)
        else:
            records = parse_csv(source_text)

        run_id = str(uuid4())
        ingest_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        jsonl_lines: list[str] = []

        for idx, payload in enumerate(records, start=1):
            schema_hash = _sha256_text("|".join(sorted(payload.keys())))
            record_hash = _sha256_text(_to_json_text(payload))
            meta = {
                "run_id": run_id,
                "ingest_ts": ingest_ts,
                "source_type": source["source_type"],
                "source_name": source_name,
                "source_file": source["source_object"].split("/")[-1],
                "source_uri": source["source_uri"],
                "pod_name": config["pod_name"],
                "pod_type": config["pod_type"],
                "task_type": config["task_type"],
                "schema_hash": schema_hash,
                "record_hash": record_hash,
                "row_number": idx,
            }
            jsonl_lines.append(_to_json_text({"meta": meta, "payload": payload}))

        raw_object = (
            f"{raw_prefix}/{source_name}/dt={ingest_date}/run_id={run_id}/"
            f"batch_id={source['batch_id']}/part-00000.jsonl"
        )
        hook.upload(
            bucket_name=config["raw_bucket"],
            object_name=raw_object,
            data="\n".join(jsonl_lines) + "\n",
        )

        outputs.append(
            {
                "run_id": run_id,
                "batch_id": source["batch_id"],
                "source_type": source["source_type"],
                "raw_uri": f"gs://{config['raw_bucket']}/{raw_object}",
            }
        )

    return outputs


@task
def build_dataproc_submit_plan(
    config: dict[str, Any],
    ingest_date: str,
    ingested_runs: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    bronze_prefix = str(config["bronze_prefix"]).strip("/")
    silver_prefix = str(config["silver_prefix"]).strip("/")
    gold_prefix = str(config["gold_prefix"]).strip("/")

    bronze_job_uri = config.get("bronze_job_uri") or f"gs://{config['raw_bucket']}/jobs/bronze_ingest_dataproc.py"
    silver_job_uri = config.get("silver_job_uri") or f"gs://{config['bronze_bucket']}/jobs/silver_transform_dataproc.py"
    gold_job_uri = config.get("gold_job_uri") or f"gs://{config['silver_bucket']}/jobs/gold_transform_dataproc.py"

    common_runtime = {
        "version": config.get("runtime_version", "2.2"),
        "properties": config["dataproc_properties"],
    }

    bronze_submit: list[dict[str, Any]] = []
    silver_submit: list[dict[str, Any]] = []
    gold_submit: list[dict[str, Any]] = []

    for run in ingested_runs:
        run_id = run["run_id"]

        bronze_submit.append(
            {
                "project_id": config["project_id"],
                "region": config["region"],
                "batch_id": f"bronze-{uuid4().hex[:16]}",
                "gcp_conn_id": config["gcp_conn_id"],
                "batch": {
                    "pyspark_batch": {
                        "main_python_file_uri": bronze_job_uri,
                        "args": [
                            f"--env={config['env']}",
                            f"--raw_bucket={config['raw_bucket']}",
                            f"--bronze_bucket={config['bronze_bucket']}",
                            f"--raw_prefix={str(config['raw_prefix']).strip('/')}/",
                            f"--bronze_prefix={bronze_prefix}/",
                            "--mode=append",
                            f"--run_id={run_id}",
                            f"--ingest_date={ingest_date}",
                        ],
                    },
                    "environment_config": {
                        "execution_config": {
                            "service_account": config["service_account"],
                        }
                    },
                    "runtime_config": common_runtime,
                },
            }
        )

        silver_submit.append(
            {
                "project_id": config["project_id"],
                "region": config["region"],
                "batch_id": f"silver-{uuid4().hex[:16]}",
                "gcp_conn_id": config["gcp_conn_id"],
                "batch": {
                    "pyspark_batch": {
                        "main_python_file_uri": silver_job_uri,
                        "args": [
                            f"--env={config['env']}",
                            f"--bronze_bucket={config['bronze_bucket']}",
                            f"--silver_bucket={config['silver_bucket']}",
                            f"--bronze_prefix={bronze_prefix}/",
                            f"--silver_prefix={silver_prefix}/",
                            "--mode=append",
                            f"--run_id={run_id}",
                            f"--ingest_date={ingest_date}",
                        ],
                    },
                    "environment_config": {
                        "execution_config": {
                            "service_account": config["service_account"],
                        }
                    },
                    "runtime_config": common_runtime,
                },
            }
        )

        gold_submit.append(
            {
                "project_id": config["project_id"],
                "region": config["region"],
                "batch_id": f"gold-{uuid4().hex[:16]}",
                "gcp_conn_id": config["gcp_conn_id"],
                "batch": {
                    "pyspark_batch": {
                        "main_python_file_uri": gold_job_uri,
                        "args": [
                            f"--env={config['env']}",
                            f"--silver_bucket={config['silver_bucket']}",
                            f"--gold_bucket={config['gold_bucket']}",
                            f"--silver_prefix={silver_prefix}/",
                            f"--gold_prefix={gold_prefix}/",
                            f"--run_id={run_id}",
                            f"--ingest_date={ingest_date}",
                        ],
                    },
                    "environment_config": {
                        "execution_config": {
                            "service_account": config["service_account"],
                        }
                    },
                    "runtime_config": common_runtime,
                },
            }
        )

    return {
        "bronze_submit": bronze_submit,
        "silver_submit": silver_submit,
        "gold_submit": gold_submit,
    }


@dag(
    dag_id=DAG_ID,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["composer", "dataproc", "llm-feedback", "e2e"],
)
def llm_feedback_full_e2e_composer() -> None:
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done", trigger_rule=TriggerRule.NONE_FAILED)

    config = load_config()
    ingest_date = resolve_ingest_date()
    source_batches = generate_source_batches(config=config, ingest_date=ingest_date)
    ingested_runs = ingest_generated_batches(config=config, ingest_date=ingest_date, source_batches=source_batches)
    submit_plan = build_dataproc_submit_plan(config=config, ingest_date=ingest_date, ingested_runs=ingested_runs)

    bronze_submit = DataprocCreateBatchOperator.partial(task_id="bronze_submit").expand_kwargs(submit_plan["bronze_submit"])
    silver_submit = DataprocCreateBatchOperator.partial(task_id="silver_submit").expand_kwargs(submit_plan["silver_submit"])
    gold_submit = DataprocCreateBatchOperator.partial(task_id="gold_submit").expand_kwargs(submit_plan["gold_submit"])

    start >> config >> ingest_date >> source_batches >> ingested_runs >> submit_plan
    submit_plan >> bronze_submit >> silver_submit >> gold_submit >> done


dag = llm_feedback_full_e2e_composer()
