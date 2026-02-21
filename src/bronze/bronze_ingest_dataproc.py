"""Dataproc-friendly Bronze ingestion for raw JSONL on GCS."""

from __future__ import annotations

import argparse
import logging
import json
from datetime import datetime
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

MANDATORY_METADATA_COLS = {
    "run_id",
    "ingest_ts",
    "schema_hash",
    "record_hash",
    "source_type",
    "raw_path",
}


def _existing_or_null(df: DataFrame, name: str) -> F.Column:
    return F.col(name) if name in df.columns else F.lit(None)


def _meta_or_null(df: DataFrame, field: str) -> F.Column:
    if "meta" not in df.columns:
        return F.lit(None)
    meta_type = df.schema["meta"].dataType
    if not isinstance(meta_type, T.StructType):
        return F.lit(None)
    if field not in {f.name for f in meta_type.fields}:
        return F.lit(None)
    return F.col(f"meta.{field}")


def normalize_raw_schema(df: DataFrame) -> DataFrame:
    if "payload" in df.columns and isinstance(df.schema["payload"].dataType, T.StructType):
        payload_fields = [f.name for f in df.schema["payload"].dataType.fields]
        payload_projections = [F.col(f"payload.{name}").alias(name) for name in payload_fields]
        passthrough = [F.col(c) for c in df.columns if c != "payload"]
        df = df.select(*passthrough, *payload_projections)

    if "raw_path" in df.columns:
        df = df.withColumn("raw_path", F.coalesce(F.col("raw_path"), F.input_file_name()))
    else:
        df = df.withColumn("raw_path", F.input_file_name())

    df = df.withColumn(
        "run_id",
        F.coalesce(
            _existing_or_null(df, "run_id"),
            _meta_or_null(df, "run_id"),
            F.regexp_extract(F.col("raw_path"), r"run_id=([^/]+)", 1),
        ),
    )
    df = df.withColumn(
        "ingest_ts",
        F.coalesce(_existing_or_null(df, "ingest_ts"), _meta_or_null(df, "ingest_ts"), F.current_timestamp()),
    )
    df = df.withColumn(
        "source_type",
        F.coalesce(_existing_or_null(df, "source_type"), _meta_or_null(df, "source_type"), F.lit("unknown")),
    )

    hash_data_cols = [
        c for c in df.columns if c not in {"meta", "schema_hash", "record_hash", "raw_path", "ingest_ts", "source_type"}
    ]
    if not hash_data_cols:
        hash_data_cols = ["run_id"]

    row_hash_expr = F.sha2(F.to_json(F.struct(*[F.col(c) for c in hash_data_cols])), 256)
    schema_hash_fallback = F.sha2(F.lit(",".join(sorted(hash_data_cols))), 256)

    df = df.withColumn(
        "schema_hash",
        F.coalesce(_existing_or_null(df, "schema_hash"), _meta_or_null(df, "schema_hash"), schema_hash_fallback),
    )
    df = df.withColumn(
        "record_hash",
        F.coalesce(_existing_or_null(df, "record_hash"), _meta_or_null(df, "record_hash"), row_hash_expr),
    )

    if "meta" in df.columns:
        df = df.drop("meta")

    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingest job for Dataproc Serverless.")
    parser.add_argument("--env", required=True, help="Runtime environment name (e.g. dev, prod).")
    parser.add_argument("--raw_bucket", default=None, help="GCS bucket containing raw data.")
    parser.add_argument("--bronze_bucket", default=None, help="GCS bucket for bronze output data.")
    parser.add_argument(
        "--gcs_bucket",
        default=None,
        help="Deprecated fallback bucket used for both raw and bronze when specific buckets are not provided.",
    )
    parser.add_argument("--raw_prefix", default="raw/", help="Raw prefix inside the bucket.")
    parser.add_argument("--bronze_prefix", default="bronze/", help="Bronze prefix inside the bucket.")
    parser.add_argument("--run_id", default=None, help="Optional run_id filter.")
    parser.add_argument("--ingest_date", default=None, help="Optional ingest_date (YYYY-MM-DD).")
    parser.add_argument("--batch_name", default=None, help="Optional batch_name filter.")
    parser.add_argument(
        "--mode",
        choices=["append", "overwrite-run"],
        default="append",
        help="Write mode. append is supported; overwrite-run is retained for compatibility but disabled.",
    )
    return parser.parse_args()


def normalize_prefix(prefix: str) -> str:
    cleaned = prefix.strip().strip("/")
    return f"{cleaned}/" if cleaned else ""


def build_raw_root(
    bucket: str,
    raw_prefix: str,
    batch_name: Optional[str],
    ingest_date: Optional[str],
    run_id: Optional[str],
) -> str:
    raw_prefix = normalize_prefix(raw_prefix)
    return f"gs://{bucket}/{raw_prefix}"


def extract_ingest_date_from_path(path_col: F.Column) -> F.Column:
    ingest = F.regexp_extract(path_col, r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
    dt = F.regexp_extract(path_col, r"dt=(\d{4}-\d{2}-\d{2})", 1)
    return (
        F.when(F.length(ingest) > 0, ingest)
        .when(F.length(dt) > 0, dt)
        .otherwise(F.lit(None))
    )


def trim_string_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))
    return df


def ensure_required_columns(df: DataFrame, required: Iterable[str]) -> None:
    missing = set(required).difference(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns: {missing_list}")


def resolve_single_value(df: DataFrame, column: str, provided: Optional[str]) -> str:
    if provided:
        return provided
    values = [row[column] for row in df.select(column).distinct().limit(2).collect() if row[column]]
    if len(values) == 1:
        return values[0]
    if not values:
        raise ValueError(f"No non-null values found for {column}.")
    raise ValueError(f"Multiple values found for {column}. Provide --{column} to scope the run.")


def write_manifest(
    spark: SparkSession,
    output_path: str,
    run_id: str,
    row_count: int,
    schema_hash: str,
) -> None:
    manifest = {
        "run_id": run_id,
        "output_path": output_path,
        "row_count": row_count,
        "schema_hash": schema_hash,
        "job_start_ts": datetime.now().isoformat(),
        "job_end_ts": datetime.now().isoformat(),
    }
    manifest_path = f"{output_path.rstrip('/')}/_manifests/run_id={run_id}.json"
    payload = f"{json.dumps(manifest)}\n"

    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(manifest_path)
    fs = path.getFileSystem(hadoop_conf)

    stream = fs.create(path, True)
    try:
        stream.write(bytearray(payload.encode("utf-8")))
    finally:
        stream.close()

    logging.info(f"Manifest written to: {manifest_path}")


def path_exists(spark: SparkSession, uri: str) -> bool:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)
    return fs.exists(path)


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    raw_bucket = args.raw_bucket or args.gcs_bucket
    bronze_bucket = args.bronze_bucket or args.gcs_bucket
    if not raw_bucket or not bronze_bucket:
        raise ValueError("Provide --raw_bucket and --bronze_bucket, or use --gcs_bucket as a fallback.")

    spark = SparkSession.builder.appName("bronze_ingest_dataproc").getOrCreate()

    input_glob = build_raw_root(
        bucket=raw_bucket,
        raw_prefix=args.raw_prefix,
        batch_name=args.batch_name,
        ingest_date=args.ingest_date,
        run_id=args.run_id,
    )
    logging.info("Reading raw JSONL recursively from %s", input_glob)

    df = spark.read.option("recursiveFileLookup", "true").json(input_glob)
    df = normalize_raw_schema(df)
    ensure_required_columns(df, MANDATORY_METADATA_COLS)

    if args.run_id:
        df = df.filter(F.col("run_id") == args.run_id)
    if args.batch_name:
        df = df.filter(
            F.col("raw_path").contains(f"batch_name={args.batch_name}/")
            | F.col("raw_path").contains(f"batch_id={args.batch_name}/")
        )

    ingest_date_col = extract_ingest_date_from_path(F.col("raw_path"))
    if args.ingest_date:
        df = df.withColumn("ingest_date", F.lit(args.ingest_date))
    else:
        df = df.withColumn("ingest_date", ingest_date_col)

    df = df.withColumn("ingest_ts", F.to_timestamp(F.col("ingest_ts")))
    df = df.withColumn("bronze_ingest_ts", F.current_timestamp())
    df = trim_string_columns(df)

    if df.filter(F.col("ingest_date").isNull()).limit(1).count() > 0:
        raise ValueError("ingest_date could not be derived from raw_path. Provide --ingest_date.")

    run_id_value = resolve_single_value(df, "run_id", args.run_id)
    ingest_date_value = resolve_single_value(df, "ingest_date", args.ingest_date)

    row_count = df.count()
    if args.mode == "overwrite-run":
        raise ValueError(
            "overwrite-run is not supported with current partitioning strategy. Use --mode=append."
        )

    df_out = df.dropDuplicates(["run_id", "record_hash"])

    dedupe_count = df_out.count()

    bronze_root = f"gs://{bronze_bucket}/{normalize_prefix(args.bronze_prefix).rstrip('/')}"
    output_path = f"{bronze_root}/ingest_date={ingest_date_value}/"
    run_manifest_path = f"{output_path.rstrip('/')}/_manifests/run_id={run_id_value}.json"

    if path_exists(spark, run_manifest_path):
        raise ValueError(
            f"run_id {run_id_value} already processed for ingest_date={ingest_date_value}; "
            "aborting to prevent duplicate append writes."
        )

    partition_cols = ["ingest_date"]
    if "source_type" in df_out.columns:
        partition_cols.append("source_type")

    write_mode = "append"

    logging.info("Writing bronze data to %s", bronze_root)
    (
        df_out.write
        .mode(write_mode)
        .partitionBy(*partition_cols)
        .parquet(bronze_root)
    )

    logging.info("Rows read: %s", row_count)
    logging.info("Rows after dedupe: %s", dedupe_count)
    logging.info("Rows written: %s", dedupe_count)
    logging.info("Output path: %s", output_path)

    # Write manifest JSON
    write_manifest(
        spark,
        output_path,
        run_id_value,
        dedupe_count,
        df_out.select("schema_hash").first()["schema_hash"],
    )

    spark.stop()


if __name__ == "__main__":
    main()
