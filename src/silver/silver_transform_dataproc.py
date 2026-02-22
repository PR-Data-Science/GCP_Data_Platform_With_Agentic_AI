"""Dataproc Serverless Silver transforms from Bronze parquet."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from typing import Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

RATING_METRICS = [
    "primary_intent",
    "information_gain",
    "reasoning",
    "understanding",
    "implementation",
    "trajectory_robustness",
]

ALLOWED_LABELS = ["LLM_RATED_BAD", "LLM_RATED_OK", "LLM_RATED_GOOD"]

RATING_SCHEMA = T.StructType(
    [
        T.StructField("primary_intent", T.DoubleType(), True),
        T.StructField("information_gain", T.DoubleType(), True),
        T.StructField("reasoning", T.DoubleType(), True),
        T.StructField("understanding", T.DoubleType(), True),
        T.StructField("implementation", T.DoubleType(), True),
        T.StructField("trajectory_robustness", T.DoubleType(), True),
        T.StructField("overall_label", T.StringType(), True),
    ]
)

AUTO_RATER_SCHEMA = T.StructType(
    [
        T.StructField("scores", RATING_SCHEMA, True),
        T.StructField("violations", T.ArrayType(T.StringType()), True),
        T.StructField("confidence", T.DoubleType(), True),
        T.StructField("rubric_version", T.StringType(), True),
        T.StructField("model_version", T.StringType(), True),
        T.StructField("final_reviewer", T.StringType(), True),
    ]
)

STEP_SCHEMA = T.StructType(
    [
        T.StructField("step_index", T.IntegerType(), True),
        T.StructField("step_type", T.StringType(), True),
        T.StructField("tool_name", T.StringType(), True),
        T.StructField("query", T.StringType(), True),
        T.StructField("tool_output", T.StringType(), True),
        T.StructField("partial_answer", T.StringType(), True),
    ]
)

EXECUTION_SCHEMA = T.StructType(
    [
        T.StructField("prompt_id", T.StringType(), True),
        T.StructField("query_text", T.StringType(), True),
        T.StructField("steps", T.ArrayType(STEP_SCHEMA), True),
        T.StructField("final_answer", T.StringType(), True),
        T.StructField("model_version", T.StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver transform job for Dataproc Serverless.")
    parser.add_argument("--env", required=True, help="Runtime environment name (e.g. dev, prod).")
    parser.add_argument("--bronze_bucket", default=None, help="GCS bucket containing bronze data.")
    parser.add_argument("--silver_bucket", default=None, help="GCS bucket for silver output data.")
    parser.add_argument(
        "--gcs_bucket",
        default=None,
        help="Deprecated fallback bucket used for both bronze and silver when specific buckets are not provided.",
    )
    parser.add_argument("--bronze_prefix", default="bronze/", help="Bronze prefix inside bronze bucket.")
    parser.add_argument("--silver_prefix", default="silver/", help="Silver prefix inside silver bucket.")
    parser.add_argument("--run_id", default=None, help="Optional run_id filter.")
    parser.add_argument("--ingest_date", default=None, help="Optional ingest_date (YYYY-MM-DD).")
    parser.add_argument("--batch_name", default=None, help="Optional batch_name / batch_id filter.")
    parser.add_argument("--mode", choices=["append"], default="append", help="Write mode.")
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


def trim_string_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def ensure_array_string(df: DataFrame, col_name: str) -> DataFrame:
    if col_name not in df.columns:
        return df.withColumn(col_name, F.array().cast(T.ArrayType(T.StringType())))

    col_type = df.schema[col_name].dataType
    target_type = T.ArrayType(T.StringType())

    if isinstance(col_type, T.ArrayType):
        return df.withColumn(col_name, F.transform(F.col(col_name), lambda item: item.cast(T.StringType())))

    if isinstance(col_type, T.StringType):
        parsed = F.from_json(F.col(col_name), target_type)
        as_single = F.when(F.col(col_name).isNull() | (F.length(F.trim(F.col(col_name))) == 0), F.array().cast(target_type)).otherwise(
            F.array(F.col(col_name))
        )
        return df.withColumn(col_name, F.coalesce(parsed, as_single))

    parsed_other = F.from_json(F.to_json(F.col(col_name)), target_type)
    return df.withColumn(col_name, F.coalesce(parsed_other, F.array().cast(target_type)))


def ensure_struct(df: DataFrame, col_name: str, schema: T.StructType) -> DataFrame:
    if col_name not in df.columns:
        return df.withColumn(col_name, F.lit(None).cast(schema))

    col_type = df.schema[col_name].dataType
    if isinstance(col_type, T.StructType):
        return df.withColumn(col_name, F.from_json(F.to_json(F.col(col_name)), schema))
    if isinstance(col_type, T.StringType):
        return df.withColumn(col_name, F.from_json(F.col(col_name), schema))
    return df.withColumn(col_name, F.from_json(F.to_json(F.col(col_name)), schema))


def with_final_scores(df: DataFrame) -> DataFrame:
    for metric in RATING_METRICS:
        c1 = F.coalesce(F.col(f"reviewer_curator_1_rating.{metric}"), F.col(f"curator_1_rating.{metric}"))
        c2 = F.coalesce(F.col(f"reviewer_curator_2_rating.{metric}"), F.col(f"curator_2_rating.{metric}"))

        final_metric = (
            F.when(c1.isNull() & c2.isNull(), F.lit(None).cast(T.DoubleType()))
            .when(c1.isNull(), c2.cast(T.DoubleType()))
            .when(c2.isNull(), c1.cast(T.DoubleType()))
            .otherwise(F.round((c1.cast(T.DoubleType()) + c2.cast(T.DoubleType())) / F.lit(2.0), 4))
        )

        df = df.withColumn(f"final_{metric}", final_metric)

    df = df.withColumn(
        "final_overall_label",
        F.coalesce(
            F.col("reviewer_curator_1_rating.overall_label"),
            F.col("reviewer_curator_2_rating.overall_label"),
            F.col("curator_1_rating.overall_label"),
            F.col("curator_2_rating.overall_label"),
            F.col("auto_rater.scores.overall_label"),
        ),
    )
    return df


def apply_dq(df: DataFrame) -> DataFrame:
    reasons = [
        F.when(F.col("run_id").isNull() | (F.length(F.trim(F.col("run_id"))) == 0), F.lit("missing_run_id")),
        F.when(F.col("record_hash").isNull() | (F.length(F.trim(F.col("record_hash"))) == 0), F.lit("missing_record_hash")),
        F.when(F.col("prompt_id").isNull() | (F.length(F.trim(F.col("prompt_id"))) == 0), F.lit("missing_prompt_id")),
        F.when(F.col("query_text").isNull() | (F.length(F.trim(F.col("query_text"))) == 0), F.lit("missing_query_text")),
        F.when(F.col("evaluated_step_index").isNull(), F.lit("missing_evaluated_step_index")),
        F.when(F.col("evaluated_step_index") < 0, F.lit("invalid_evaluated_step_index")),
        F.when(
            F.col("final_overall_label").isNotNull() & (~F.col("final_overall_label").isin(ALLOWED_LABELS)),
            F.lit("invalid_final_overall_label"),
        ),
    ]

    for metric in RATING_METRICS:
        reasons.append(
            F.when(
                F.col(f"final_{metric}").isNotNull()
                & ((F.col(f"final_{metric}") < 0.0) | (F.col(f"final_{metric}") > 5.0)),
                F.lit(f"out_of_range_{metric}"),
            )
        )

    dq_reasons = F.filter(F.array(*reasons), lambda reason: reason.isNotNull())
    return df.withColumn("dq_reasons", dq_reasons).withColumn("dq_pass", F.size(dq_reasons) == 0)


def build_feedback_step(df: DataFrame) -> DataFrame:
    return df.select(
        "ingest_date",
        "run_id",
        "record_hash",
        "schema_hash",
        "source_type",
        "raw_path",
        "batch_id",
        "batch_name",
        "pod_name",
        "pod_type",
        "task_type",
        "set_id",
        "prompt_id",
        "query_text",
        "step_index",
        "evaluated_step_index",
        "step_type",
        F.col("execution_json.model_version").alias("model_version"),
        F.col("execution_json.final_answer").alias("final_answer"),
        F.col("auto_rater.model_version").alias("auto_rater_model_version"),
        F.col("auto_rater.rubric_version").alias("rubric_version"),
        F.col("auto_rater.confidence").alias("auto_rater_confidence"),
        *[F.col(f"final_{metric}") for metric in RATING_METRICS],
        "final_overall_label",
        "dq_pass",
        "dq_reasons",
        F.current_timestamp().alias("silver_processed_ts"),
    )


def build_ratings_long(df: DataFrame) -> DataFrame:
    key_cols = [
        "ingest_date",
        "run_id",
        "record_hash",
        "prompt_id",
        "evaluated_step_index",
        "step_type",
        "task_type",
    ]

    def _rows_for(rating_col: str, rater_type: str) -> DataFrame:
        metric_structs = [
            F.struct(F.lit(metric).alias("metric_name"), F.col(f"{rating_col}.{metric}").cast(T.DoubleType()).alias("metric_score"))
            for metric in RATING_METRICS
        ]
        return (
            df.select(
                *key_cols,
                F.lit(rater_type).alias("rater_type"),
                F.col(f"{rating_col}.overall_label").alias("overall_label"),
                F.array(*metric_structs).alias("metrics"),
            )
            .withColumn("metric", F.explode("metrics"))
            .select(
                *key_cols,
                "rater_type",
                "overall_label",
                F.col("metric.metric_name").alias("metric_name"),
                F.col("metric.metric_score").alias("metric_score"),
            )
            .filter(F.col("metric_score").isNotNull() | F.col("overall_label").isNotNull())
        )

    frames = [
        _rows_for("curator_1_rating", "curator_1"),
        _rows_for("curator_2_rating", "curator_2"),
        _rows_for("reviewer_curator_1_rating", "reviewer_curator_1"),
        _rows_for("reviewer_curator_2_rating", "reviewer_curator_2"),
        _rows_for("auto_rater.scores", "auto_rater"),
    ]

    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame, allowMissingColumns=True)

    return out.withColumn("silver_processed_ts", F.current_timestamp())


def build_execution_steps(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("exec_step", F.explode_outer(F.col("execution_json.steps")))
        .select(
            "ingest_date",
            "run_id",
            "record_hash",
            "prompt_id",
            "evaluated_step_index",
            "step_type",
            F.col("execution_json.model_version").alias("model_version"),
            F.col("exec_step.step_index").alias("execution_step_index"),
            F.col("exec_step.step_type").alias("execution_step_type"),
            F.col("exec_step.tool_name").alias("tool_name"),
            F.col("exec_step.query").alias("tool_query"),
            F.col("exec_step.tool_output").alias("tool_output"),
            F.col("exec_step.partial_answer").alias("partial_answer"),
            F.current_timestamp().alias("silver_processed_ts"),
        )
        .filter(F.col("execution_step_index").isNotNull())
    )


def build_violations(df: DataFrame) -> DataFrame:
    top = (
        df.withColumn("violation", F.explode_outer(F.col("violations")))
        .select(
            "ingest_date",
            "run_id",
            "record_hash",
            "prompt_id",
            "evaluated_step_index",
            F.lit("record").alias("violation_source"),
            F.col("violation").cast(T.StringType()).alias("violation"),
        )
    )

    auto = (
        df.withColumn("violation", F.explode_outer(F.col("auto_rater.violations")))
        .select(
            "ingest_date",
            "run_id",
            "record_hash",
            "prompt_id",
            "evaluated_step_index",
            F.lit("auto_rater").alias("violation_source"),
            F.col("violation").cast(T.StringType()).alias("violation"),
        )
    )

    return (
        top.unionByName(auto)
        .filter(F.col("violation").isNotNull() & (F.length(F.trim(F.col("violation"))) > 0))
        .withColumn("silver_processed_ts", F.current_timestamp())
    )


def write_table(df: DataFrame, root: str, table_name: str) -> None:
    if df.rdd.isEmpty():
        return
    (
        df.write.mode("append")
        .partitionBy("ingest_date")
        .parquet(f"{root.rstrip('/')}/{table_name}")
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bronze_bucket = args.bronze_bucket or args.gcs_bucket
    silver_bucket = args.silver_bucket or args.gcs_bucket
    if not bronze_bucket or not silver_bucket:
        raise ValueError("Provide --bronze_bucket and --silver_bucket, or use --gcs_bucket as fallback.")

    spark = SparkSession.builder.appName("silver_transform_dataproc").getOrCreate()

    bronze_root = f"gs://{bronze_bucket}/{normalize_prefix(args.bronze_prefix)}"
    silver_root = f"gs://{silver_bucket}/{normalize_prefix(args.silver_prefix).rstrip('/')}"

    logging.info("Reading bronze parquet from %s", bronze_root)
    df = spark.read.parquet(bronze_root)

    if args.run_id:
        df = df.filter(F.col("run_id") == args.run_id)
    if args.ingest_date:
        df = df.filter(F.col("ingest_date") == args.ingest_date)
    if args.batch_name:
        df = df.filter((F.col("batch_name") == args.batch_name) | (F.col("batch_id") == args.batch_name))

    if df.limit(1).count() == 0:
        raise ValueError("No bronze rows found for the provided filters.")

    if "ingest_date" not in df.columns:
        raise ValueError("Bronze input must include ingest_date partition column.")

    ingest_date_value = resolve_single_value(df, "ingest_date", args.ingest_date)
    run_id_value = resolve_single_value(df, "run_id", args.run_id)

    manifest_path = f"{silver_root}/ingest_date={ingest_date_value}/_manifests/run_id={run_id_value}.json"
    if path_exists(spark, manifest_path):
        raise ValueError(
            f"run_id {run_id_value} already processed for ingest_date={ingest_date_value}; "
            "aborting to prevent duplicate append writes."
        )

    df = df.dropDuplicates(["run_id", "record_hash"])
    df = trim_string_columns(df)

    df = ensure_struct(df, "curator_1_rating", RATING_SCHEMA)
    df = ensure_struct(df, "curator_2_rating", RATING_SCHEMA)
    df = ensure_struct(df, "reviewer_curator_1_rating", RATING_SCHEMA)
    df = ensure_struct(df, "reviewer_curator_2_rating", RATING_SCHEMA)
    df = ensure_struct(df, "auto_rater", AUTO_RATER_SCHEMA)
    df = ensure_struct(df, "execution_json", EXECUTION_SCHEMA)
    df = ensure_array_string(df, "violations")

    df = df.withColumn("step_index", F.col("step_index").cast(T.IntegerType()))
    df = df.withColumn("evaluated_step_index", F.col("evaluated_step_index").cast(T.IntegerType()))
    df = with_final_scores(df)
    df = apply_dq(df)

    deadletter_df = (
        df.filter(~F.col("dq_pass"))
        .select(
            "ingest_date",
            "run_id",
            "record_hash",
            "raw_path",
            "prompt_id",
            "evaluated_step_index",
            "dq_reasons",
            F.to_json(F.struct(*[F.col(c) for c in df.columns if c not in {"dq_reasons", "dq_pass"}])).alias("raw_record_json"),
            F.current_timestamp().alias("silver_processed_ts"),
        )
    )

    clean_df = df.filter(F.col("dq_pass"))

    feedback_step_df = build_feedback_step(clean_df)
    ratings_long_df = build_ratings_long(clean_df)
    execution_steps_df = build_execution_steps(clean_df)
    violations_df = build_violations(clean_df)

    write_table(feedback_step_df, silver_root, "feedback_step")
    write_table(ratings_long_df, silver_root, "ratings_long")
    write_table(execution_steps_df, silver_root, "execution_steps")
    write_table(violations_df, silver_root, "violations")
    write_table(deadletter_df, silver_root, "deadletter")

    counts = {
        "feedback_step": feedback_step_df.count(),
        "ratings_long": ratings_long_df.count(),
        "execution_steps": execution_steps_df.count(),
        "violations": violations_df.count(),
        "deadletter": deadletter_df.count(),
    }

    manifest = {
        "run_id": run_id_value,
        "ingest_date": ingest_date_value,
        "input_path": bronze_root,
        "output_path": silver_root,
        "row_counts": counts,
        "job_start_ts": datetime.now().isoformat(),
        "job_end_ts": datetime.now().isoformat(),
    }
    write_manifest(spark, manifest_path, manifest)
    logging.info("Silver manifest written to %s", manifest_path)

    spark.stop()


if __name__ == "__main__":
    main()
