"""Dataproc Serverless Gold transforms from Silver parquet."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from typing import Dict, Optional

from src.ops.ops_writer import write_pipeline_runs, write_schema_registry_first_seen
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

METRICS = [
    "final_primary_intent",
    "final_information_gain",
    "final_reasoning",
    "final_understanding",
    "final_implementation",
    "final_trajectory_robustness",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold transform job for Dataproc Serverless.")
    parser.add_argument("--env", required=True, help="Runtime environment name (e.g. dev, prod).")
    parser.add_argument("--silver_bucket", default=None, help="GCS bucket containing silver data.")
    parser.add_argument("--gold_bucket", default=None, help="GCS bucket for gold output data.")
    parser.add_argument(
        "--gcs_bucket",
        default=None,
        help="Deprecated fallback bucket used for both silver and gold when specific buckets are not provided.",
    )
    parser.add_argument("--silver_prefix", default="silver/", help="Silver prefix inside silver bucket.")
    parser.add_argument("--gold_prefix", default="gold/", help="Gold prefix inside gold bucket.")
    parser.add_argument("--run_id", default=None, help="Optional run_id filter.")
    parser.add_argument("--ingest_date", default=None, help="Optional ingest_date (YYYY-MM-DD).")
    parser.add_argument("--mode", choices=["append"], default="append", help="Write mode.")
    parser.add_argument("--publish_bigquery", action="store_true", help="Publish Gold tables to BigQuery.")
    parser.add_argument("--bq_project", default=None, help="Target BigQuery project id.")
    parser.add_argument("--bq_dataset", default=None, help="Target BigQuery dataset.")
    parser.add_argument("--force", action="store_true", help="Force reprocess even if stage manifest exists.")
    parser.add_argument("--code_version", default="unknown", help="Code version pointer (for example git SHA).")
    parser.add_argument("--ops_dataset", default="ops", help="BigQuery dataset for ops control plane tables.")
    return parser.parse_args()


def normalize_prefix(prefix: str) -> str:
    cleaned = prefix.strip().strip("/")
    return f"{cleaned}/" if cleaned else ""


def path_exists(spark: SparkSession, uri: str) -> bool:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)
    return fs.exists(path)


def write_manifest(spark: SparkSession, manifest_path: str, payload: Dict[str, object]) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(manifest_path)
    fs = path.getFileSystem(hadoop_conf)

    stream = fs.create(path, True)
    try:
        stream.write(bytearray(f"{json.dumps(payload)}\n".encode("utf-8")))
    finally:
        stream.close()


def resolve_single_value(df: DataFrame, column: str, provided: Optional[str]) -> str:
    if provided:
        return provided
    values = [row[column] for row in df.select(column).distinct().limit(2).collect() if row[column]]
    if len(values) == 1:
        return values[0]
    if not values:
        raise ValueError(f"No non-null values found for {column}.")
    raise ValueError(f"Multiple values found for {column}. Provide --{column} to scope the run.")


def read_table(spark: SparkSession, root: str, table_name: str) -> DataFrame:
    return spark.read.parquet(f"{root.rstrip('/')}/{table_name}")


def build_training_supervised_examples(feedback_step: DataFrame, violations: DataFrame) -> DataFrame:
    latest_w = Window.partitionBy("run_id", "prompt_id").orderBy(F.col("evaluated_step_index").desc_nulls_last())
    violation_counts = (
        violations.groupBy("run_id", "prompt_id")
        .agg(F.count(F.lit(1)).alias("violation_count"))
    )

    quality_expr = sum(F.coalesce(F.col(metric), F.lit(0.0)) for metric in METRICS) / F.lit(float(len(METRICS)))

    return (
        feedback_step
        .withColumn("rn", F.row_number().over(latest_w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .join(violation_counts, on=["run_id", "prompt_id"], how="left")
        .withColumn("violation_count", F.coalesce(F.col("violation_count"), F.lit(0)))
        .withColumn("avg_quality_score", F.round(quality_expr, 4))
        .select(
            "ingest_date",
            "run_id",
            "prompt_id",
            "task_type",
            "model_version",
            "query_text",
            "final_answer",
            *METRICS,
            "final_overall_label",
            "avg_quality_score",
            "violation_count",
            F.current_timestamp().alias("gold_processed_ts"),
        )
    )


def build_model_eval_step_metrics(feedback_step: DataFrame) -> DataFrame:
    return feedback_step.select(
        "ingest_date",
        "run_id",
        "prompt_id",
        "evaluated_step_index",
        "task_type",
        "step_type",
        "model_version",
        *METRICS,
        "final_overall_label",
        F.when(F.col("final_overall_label") == "LLM_RATED_BAD", F.lit(1)).otherwise(F.lit(0)).alias("is_bad"),
        F.current_timestamp().alias("gold_processed_ts"),
    )


def build_model_eval_failure_breakdown(feedback_step: DataFrame, violations: DataFrame) -> DataFrame:
    label_failures = (
        feedback_step.filter(F.col("final_overall_label") == "LLM_RATED_BAD")
        .groupBy("ingest_date", "run_id", "task_type", "model_version")
        .agg(F.count(F.lit(1)).alias("failure_count"))
        .withColumn("failure_type", F.lit("overall_label"))
        .withColumn("failure_value", F.lit("LLM_RATED_BAD"))
    )

    violation_failures = (
        violations.join(
            feedback_step.select("run_id", "prompt_id", "evaluated_step_index", "task_type", "model_version"),
            on=["run_id", "prompt_id", "evaluated_step_index"],
            how="left",
        )
        .groupBy("ingest_date", "run_id", "task_type", "model_version", "violation")
        .agg(F.count(F.lit(1)).alias("failure_count"))
        .withColumn("failure_type", F.lit("violation"))
        .withColumnRenamed("violation", "failure_value")
    )

    return (
        label_failures.select("ingest_date", "run_id", "task_type", "model_version", "failure_type", "failure_value", "failure_count")
        .unionByName(
            violation_failures.select("ingest_date", "run_id", "task_type", "model_version", "failure_type", "failure_value", "failure_count"),
            allowMissingColumns=True,
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
    )


def build_model_comparison_daily(feedback_step: DataFrame) -> DataFrame:
    aggregates = [
        F.count(F.lit(1)).alias("step_count"),
        F.avg(F.when(F.col("final_overall_label") == "LLM_RATED_BAD", F.lit(1.0)).otherwise(F.lit(0.0))).alias("bad_rate"),
    ]
    for metric in METRICS:
        aggregates.append(F.avg(F.col(metric)).alias(f"avg_{metric.replace('final_', '')}"))

    return (
        feedback_step.groupBy("ingest_date", "task_type", "model_version")
        .agg(*aggregates)
        .withColumn("gold_processed_ts", F.current_timestamp())
    )


def build_rater_agreement(ratings_long: DataFrame, feedback_step: DataFrame) -> DataFrame:
    key_cols = ["ingest_date", "run_id", "prompt_id", "evaluated_step_index", "metric_name"]

    human = (
        ratings_long.filter(F.col("rater_type").isin("reviewer_curator_1", "reviewer_curator_2", "curator_1", "curator_2"))
        .groupBy(*key_cols)
        .agg(F.avg("metric_score").alias("human_metric_score"))
    )
    auto = (
        ratings_long.filter(F.col("rater_type") == "auto_rater")
        .select(*key_cols, F.col("metric_score").alias("auto_metric_score"))
    )

    detail = (
        human.join(auto, on=key_cols, how="inner")
        .join(
            feedback_step.select("ingest_date", "run_id", "prompt_id", "evaluated_step_index", "task_type", "model_version"),
            on=["ingest_date", "run_id", "prompt_id", "evaluated_step_index"],
            how="left",
        )
        .withColumn("abs_diff", F.abs(F.col("human_metric_score") - F.col("auto_metric_score")))
    )

    return (
        detail.groupBy("ingest_date", "run_id", "task_type", "model_version", "metric_name")
        .agg(
            F.count(F.lit(1)).alias("comparison_count"),
            F.avg("abs_diff").alias("avg_abs_diff"),
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
    )


def write_table(df: DataFrame, root: str, table_name: str) -> None:
    if df.rdd.isEmpty():
        return
    (
        df.write.mode("append")
        .partitionBy("ingest_date", "run_id")
        .parquet(f"{root.rstrip('/')}/{table_name}")
    )


def maybe_publish_bigquery(args: argparse.Namespace, tables: Dict[str, DataFrame], temp_gcs_bucket: str) -> None:
    if not args.publish_bigquery:
        return
    if not args.bq_project or not args.bq_dataset:
        raise ValueError("--bq_project and --bq_dataset are required when --publish_bigquery is enabled.")

    for table_name, df in tables.items():
        if df.rdd.isEmpty():
            continue
        target = f"{args.bq_project}:{args.bq_dataset}.gold_{table_name}"
        (
            df.write.format("bigquery")
            .mode("append")
            .option("table", target)
            .option("temporaryGcsBucket", temp_gcs_bucket)
            .save()
        )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    started_at = datetime.utcnow()

    silver_bucket = args.silver_bucket or args.gcs_bucket
    gold_bucket = args.gold_bucket or args.gcs_bucket
    if not silver_bucket or not gold_bucket:
        raise ValueError("Provide --silver_bucket and --gold_bucket, or use --gcs_bucket as fallback.")

    spark = SparkSession.builder.appName("gold_transform_dataproc").getOrCreate()

    silver_root = f"gs://{silver_bucket}/{normalize_prefix(args.silver_prefix).rstrip('/')}"
    gold_root = f"gs://{gold_bucket}/{normalize_prefix(args.gold_prefix).rstrip('/')}"

    feedback_step = read_table(spark, silver_root, "feedback_step")
    ratings_long = read_table(spark, silver_root, "ratings_long")
    violations = read_table(spark, silver_root, "violations")

    if args.ingest_date:
        feedback_step = feedback_step.filter(F.col("ingest_date") == args.ingest_date)
        ratings_long = ratings_long.filter(F.col("ingest_date") == args.ingest_date)
        violations = violations.filter(F.col("ingest_date") == args.ingest_date)
    if args.run_id:
        feedback_step = feedback_step.filter(F.col("run_id") == args.run_id)
        ratings_long = ratings_long.filter(F.col("run_id") == args.run_id)
        violations = violations.filter(F.col("run_id") == args.run_id)

    if feedback_step.limit(1).count() == 0:
        raise ValueError("No silver feedback_step rows found for the provided filters.")

    ingest_date_value = resolve_single_value(feedback_step, "ingest_date", args.ingest_date)
    run_id_value = resolve_single_value(feedback_step, "run_id", args.run_id)

    manifest_path = f"gs://{gold_bucket}/manifests/gold/dt={ingest_date_value}/run_id={run_id_value}/manifest.json"
    if path_exists(spark, manifest_path) and not args.force:
        logging.info(
            "Gold stage manifest exists for run_id=%s ingest_date=%s; skipping stage (use --force to reprocess).",
            run_id_value,
            ingest_date_value,
        )
        spark.stop()
        return

    training_supervised_examples = build_training_supervised_examples(feedback_step, violations)
    model_eval_step_metrics = build_model_eval_step_metrics(feedback_step)
    model_eval_failure_breakdown = build_model_eval_failure_breakdown(feedback_step, violations)
    model_comparison_daily = build_model_comparison_daily(feedback_step)
    rater_agreement = build_rater_agreement(ratings_long, feedback_step)

    tables = {
        "training_supervised_examples": training_supervised_examples,
        "model_eval_step_metrics": model_eval_step_metrics,
        "model_eval_failure_breakdown": model_eval_failure_breakdown,
        "model_comparison_daily": model_comparison_daily,
        "rater_agreement": rater_agreement,
    }

    for table_name, df in tables.items():
        write_table(df, gold_root, table_name)

    maybe_publish_bigquery(args, tables, gold_bucket)

    input_count = feedback_step.count()
    counts = {table_name: df.count() for table_name, df in tables.items()}
    finished_at = datetime.utcnow()
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    schema_hash_value = feedback_step.select("schema_hash").first()["schema_hash"] if "schema_hash" in feedback_step.columns else None

    try:
        if schema_hash_value:
            write_schema_registry_first_seen(
                [
                    {
                        "schema_hash": schema_hash_value,
                        "schema_json": training_supervised_examples.schema.json(),
                        "first_seen_run_id": run_id_value,
                        "source_type": "gold",
                    }
                ],
                dataset=args.ops_dataset,
            )
    except Exception as exc:
        logging.warning("Failed to write gold schema snapshot to ops.schema_registry: %s", exc)

    manifest = {
        "run_id": run_id_value,
        "stage": "gold",
        "env": args.env,
        "source_type": "mixed",
        "input_paths": [silver_root],
        "output_paths": [gold_root],
        "schema_hash": schema_hash_value,
        "record_count_in": input_count,
        "record_count_out": counts.get("training_supervised_examples", 0),
        "deadletter_count": 0,
        "dataproc_batch_id": None,
        "start_ts": started_at.isoformat() + "Z",
        "end_ts": finished_at.isoformat() + "Z",
        "duration_ms": duration_ms,
        "code_version": args.code_version,
        "error_category": None,
        "error_code": None,
        "ingest_date": ingest_date_value,
        "input_path": silver_root,
        "output_path": gold_root,
        "row_counts": counts,
        "published_to_bigquery": args.publish_bigquery,
        "job_start_ts": started_at.isoformat() + "Z",
        "job_end_ts": finished_at.isoformat() + "Z",
    }
    write_manifest(spark, manifest_path, manifest)
    logging.info("Gold manifest written to %s", manifest_path)

    gold_run_row = {
        "run_id": run_id_value,
        "stage": "gold",
        "status": "SUCCEEDED",
        "start_ts": manifest["start_ts"],
        "end_ts": manifest["end_ts"],
        "duration_ms": manifest["duration_ms"],
        "input_count": manifest["record_count_in"],
        "output_count": manifest["record_count_out"],
        "deadletter_count": 0,
        "schema_hash": schema_hash_value,
        "dataproc_batch_id": None,
        "manifest_path": manifest_path,
        "error_category": None,
        "error_code": None,
        "error_summary": None,
        "code_version": args.code_version,
        "input_paths": [silver_root],
        "output_paths": [gold_root],
        "partition_keys": [f"ingest_date={ingest_date_value}", f"run_id={run_id_value}"],
    }

    try:
        write_pipeline_runs([gold_run_row], dataset=args.ops_dataset)
    except Exception as exc:
        logging.warning("Failed to write gold stage ops row: %s", exc)

    if args.publish_bigquery:
        publish_manifest_path = (
            f"gs://{gold_bucket}/manifests/publish/dt={ingest_date_value}/run_id={run_id_value}/manifest.json"
        )
        publish_manifest = {
            "run_id": run_id_value,
            "stage": "publish",
            "env": args.env,
            "source_type": "mixed",
            "input_paths": [gold_root],
            "output_paths": [f"{args.bq_project}:{args.bq_dataset}.gold_*"],
            "schema_hash": schema_hash_value,
            "record_count_in": sum(counts.values()),
            "record_count_out": sum(counts.values()),
            "deadletter_count": 0,
            "dataproc_batch_id": None,
            "start_ts": started_at.isoformat() + "Z",
            "end_ts": finished_at.isoformat() + "Z",
            "duration_ms": duration_ms,
            "code_version": args.code_version,
            "error_category": None,
            "error_code": None,
        }
        write_manifest(spark, publish_manifest_path, publish_manifest)
        logging.info("Publish manifest written to %s", publish_manifest_path)

        publish_run_row = {
            "run_id": run_id_value,
            "stage": "publish",
            "status": "SUCCEEDED",
            "start_ts": publish_manifest["start_ts"],
            "end_ts": publish_manifest["end_ts"],
            "duration_ms": publish_manifest["duration_ms"],
            "input_count": publish_manifest["record_count_in"],
            "output_count": publish_manifest["record_count_out"],
            "deadletter_count": 0,
            "schema_hash": schema_hash_value,
            "dataproc_batch_id": None,
            "manifest_path": publish_manifest_path,
            "error_category": None,
            "error_code": None,
            "error_summary": None,
            "code_version": args.code_version,
            "input_paths": [gold_root],
            "output_paths": [f"{args.bq_project}:{args.bq_dataset}.gold_*"],
            "partition_keys": [f"ingest_date={ingest_date_value}", f"run_id={run_id_value}"],
        }
        try:
            write_pipeline_runs([publish_run_row], dataset=args.ops_dataset)
        except Exception as exc:
            logging.warning("Failed to write publish stage ops row: %s", exc)

    spark.stop()


if __name__ == "__main__":
    main()
