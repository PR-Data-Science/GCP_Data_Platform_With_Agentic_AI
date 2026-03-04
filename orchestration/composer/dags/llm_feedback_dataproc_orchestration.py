from __future__ import annotations

import re
from typing import Any
from uuid import uuid4

import pendulum
from airflow.decorators import dag, task
try:
    from airflow.decorators import get_current_context
except ImportError:
    from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "llm_feedback_dataproc_orchestration"
CONFIG_VAR = "llm_feedback_composer_config"


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
    config.setdefault("source_name", "llm_feedback_eval")
    config.setdefault("raw_prefix", "raw")
    config.setdefault("bronze_prefix", "bronze")
    config.setdefault("silver_prefix", "silver")
    config.setdefault("gold_prefix", "gold")
    config.setdefault("code_version", "unknown")
    config.setdefault("ops_dataset", "ops")
    config.setdefault("force_reprocess", False)
    config.setdefault("dataproc_properties", {
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.instances": "2",
        "spark.executor.cores": "4",
        "spark.driver.cores": "4",
    })

    return config


@task
def resolve_ingest_date() -> str:
    context = get_current_context()
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("ingest_date"):
        return dag_run.conf["ingest_date"]

    return context["data_interval_end"].in_timezone("UTC").format("YYYY-MM-DD")


@task
def resolve_force_reprocess(config: dict[str, Any]) -> bool:
    context = get_current_context()
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf and "force_reprocess" in dag_run.conf:
        value = dag_run.conf.get("force_reprocess")
    else:
        value = config.get("force_reprocess", False)

    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


@task
def discover_run_ids(config: dict[str, Any], ingest_date: str) -> list[str]:
    hook = GCSHook(gcp_conn_id=config["gcp_conn_id"])
    raw_prefix = str(config["raw_prefix"]).strip("/")
    source_name = config["source_name"]
    prefix = f"{raw_prefix}/{source_name}/dt={ingest_date}/"

    objects = hook.list(bucket_name=config["raw_bucket"], prefix=prefix)
    if not objects:
        return []

    run_ids: set[str] = set()
    for obj in objects:
        match = re.search(r"run_id=([^/]+)/", obj)
        if match:
            run_ids.add(match.group(1))

    return sorted(run_ids)


@task
def build_stage_plan(
    config: dict[str, Any],
    ingest_date: str,
    run_ids: list[str],
    force_reprocess: bool,
) -> dict[str, list[dict[str, Any]]]:
    hook = GCSHook(gcp_conn_id=config["gcp_conn_id"])

    bronze_prefix = str(config["bronze_prefix"]).strip("/")
    silver_prefix = str(config["silver_prefix"]).strip("/")
    gold_prefix = str(config["gold_prefix"]).strip("/")

    bronze_job_uri = config.get("bronze_job_uri") or f"gs://{config['raw_bucket']}/jobs/bronze_ingest_dataproc.py"
    silver_job_uri = config.get("silver_job_uri") or f"gs://{config['bronze_bucket']}/jobs/silver_transform_dataproc.py"
    gold_job_uri = config.get("gold_job_uri") or f"gs://{config['silver_bucket']}/jobs/gold_transform_dataproc.py"

    all_stages: list[str] = []
    silver_only: list[str] = []
    gold_only: list[str] = []

    if force_reprocess:
        all_stages = list(run_ids)
    else:
        for run_id in run_ids:
            bronze_manifest = f"manifests/bronze/dt={ingest_date}/run_id={run_id}/manifest.json"
            silver_manifest = f"manifests/silver/dt={ingest_date}/run_id={run_id}/manifest.json"
            gold_manifest = f"manifests/gold/dt={ingest_date}/run_id={run_id}/manifest.json"

            has_bronze = hook.exists(bucket_name=config["bronze_bucket"], object_name=bronze_manifest)
            has_silver = hook.exists(bucket_name=config["silver_bucket"], object_name=silver_manifest)
            has_gold = hook.exists(bucket_name=config["gold_bucket"], object_name=gold_manifest)

            if has_gold:
                continue
            if has_silver:
                gold_only.append(run_id)
            elif has_bronze:
                silver_only.append(run_id)
            else:
                all_stages.append(run_id)

    common_runtime = {
        "version": config.get("runtime_version", "2.2"),
        "properties": config["dataproc_properties"],
    }

    def bronze_batch(run_id: str) -> dict[str, Any]:
        args = [
            f"--env={config.get('env', 'dev')}",
            f"--raw_bucket={config['raw_bucket']}",
            f"--bronze_bucket={config['bronze_bucket']}",
            f"--raw_prefix={bronze_prefix}/",
            f"--bronze_prefix={bronze_prefix}/",
            "--mode=append",
            f"--code_version={config['code_version']}",
            f"--ops_dataset={config['ops_dataset']}",
            f"--run_id={run_id}",
            f"--ingest_date={ingest_date}",
        ]
        if force_reprocess:
            args.append("--force")
        return {
            "pyspark_batch": {
                "main_python_file_uri": bronze_job_uri,
                "args": args,
            },
            "environment_config": {
                "execution_config": {
                    "service_account": config["service_account"],
                }
            },
            "runtime_config": common_runtime,
        }

    def silver_batch(run_id: str) -> dict[str, Any]:
        args = [
            f"--env={config.get('env', 'dev')}",
            f"--bronze_bucket={config['bronze_bucket']}",
            f"--silver_bucket={config['silver_bucket']}",
            f"--bronze_prefix={bronze_prefix}/",
            f"--silver_prefix={silver_prefix}/",
            "--mode=append",
            f"--code_version={config['code_version']}",
            f"--ops_dataset={config['ops_dataset']}",
            f"--run_id={run_id}",
            f"--ingest_date={ingest_date}",
        ]
        if force_reprocess:
            args.append("--force")
        return {
            "pyspark_batch": {
                "main_python_file_uri": silver_job_uri,
                "args": args,
            },
            "environment_config": {
                "execution_config": {
                    "service_account": config["service_account"],
                }
            },
            "runtime_config": common_runtime,
        }

    def gold_batch(run_id: str) -> dict[str, Any]:
        args = [
            f"--env={config.get('env', 'dev')}",
            f"--silver_bucket={config['silver_bucket']}",
            f"--gold_bucket={config['gold_bucket']}",
            f"--silver_prefix={silver_prefix}/",
            f"--gold_prefix={gold_prefix}/",
            f"--code_version={config['code_version']}",
            f"--ops_dataset={config['ops_dataset']}",
            f"--run_id={run_id}",
            f"--ingest_date={ingest_date}",
        ]
        if force_reprocess:
            args.append("--force")
        return {
            "pyspark_batch": {
                "main_python_file_uri": gold_job_uri,
                "args": args,
            },
            "environment_config": {
                "execution_config": {
                    "service_account": config["service_account"],
                }
            },
            "runtime_config": common_runtime,
        }

    def submit_kwargs(stage: str, batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for batch in batches:
            result.append(
                {
                    "project_id": config["project_id"],
                    "region": config["region"],
                    "batch": batch,
                    "batch_id": f"{stage}-{uuid4().hex[:16]}",
                    "gcp_conn_id": config["gcp_conn_id"],
                }
            )
        return result

    bronze_batches = [bronze_batch(run_id) for run_id in all_stages]
    silver_from_bronze_batches = [silver_batch(run_id) for run_id in all_stages]
    gold_from_bronze_batches = [gold_batch(run_id) for run_id in all_stages]

    silver_only_batches = [silver_batch(run_id) for run_id in silver_only]
    gold_from_silver_only_batches = [gold_batch(run_id) for run_id in silver_only]

    gold_only_batches = [gold_batch(run_id) for run_id in gold_only]

    return {
        "bronze_submit": submit_kwargs("bronze", bronze_batches),
        "silver_from_bronze_submit": submit_kwargs("silver", silver_from_bronze_batches),
        "silver_only_submit": submit_kwargs("silver", silver_only_batches),
        "gold_from_bronze_submit": submit_kwargs("gold", gold_from_bronze_batches),
        "gold_from_silver_only_submit": submit_kwargs("gold", gold_from_silver_only_batches),
        "gold_only_submit": submit_kwargs("gold", gold_only_batches),
    }


@task
def get_submit_list(plan: dict[str, list[dict[str, Any]]], stage_key: str) -> list[dict[str, Any]]:
    return plan.get(stage_key, [])


@dag(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["composer", "dataproc", "llm-feedback"],
)
def llm_feedback_dataproc_orchestration() -> None:
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done", trigger_rule=TriggerRule.NONE_FAILED)

    config = load_config()
    ingest_date = resolve_ingest_date()
    force_reprocess = resolve_force_reprocess(config=config)
    run_ids = discover_run_ids(config=config, ingest_date=ingest_date)
    plan = build_stage_plan(
        config=config,
        ingest_date=ingest_date,
        run_ids=run_ids,
        force_reprocess=force_reprocess,
    )

    bronze_submit_kwargs = get_submit_list(plan=plan, stage_key="bronze_submit")
    silver_from_bronze_submit_kwargs = get_submit_list(plan=plan, stage_key="silver_from_bronze_submit")
    silver_only_submit_kwargs = get_submit_list(plan=plan, stage_key="silver_only_submit")
    gold_from_bronze_submit_kwargs = get_submit_list(plan=plan, stage_key="gold_from_bronze_submit")
    gold_from_silver_only_submit_kwargs = get_submit_list(plan=plan, stage_key="gold_from_silver_only_submit")
    gold_only_submit_kwargs = get_submit_list(plan=plan, stage_key="gold_only_submit")

    bronze_submit = DataprocCreateBatchOperator.partial(task_id="bronze_submit").expand_kwargs(bronze_submit_kwargs)

    silver_from_bronze_submit = DataprocCreateBatchOperator.partial(
        task_id="silver_from_bronze_submit"
    ).expand_kwargs(silver_from_bronze_submit_kwargs)

    silver_only_submit = DataprocCreateBatchOperator.partial(task_id="silver_only_submit").expand_kwargs(
        silver_only_submit_kwargs
    )

    gold_from_bronze_submit = DataprocCreateBatchOperator.partial(task_id="gold_from_bronze_submit").expand_kwargs(
        gold_from_bronze_submit_kwargs
    )

    gold_from_silver_only_submit = DataprocCreateBatchOperator.partial(
        task_id="gold_from_silver_only_submit"
    ).expand_kwargs(gold_from_silver_only_submit_kwargs)

    gold_only_submit = DataprocCreateBatchOperator.partial(task_id="gold_only_submit").expand_kwargs(
        gold_only_submit_kwargs
    )

    start >> config >> ingest_date >> run_ids >> plan
    config >> force_reprocess >> plan

    plan >> bronze_submit >> silver_from_bronze_submit >> gold_from_bronze_submit >> done
    plan >> silver_only_submit >> gold_from_silver_only_submit >> done
    plan >> gold_only_submit >> done


dag = llm_feedback_dataproc_orchestration()
