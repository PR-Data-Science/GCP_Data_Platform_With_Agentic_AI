"""Dataproc-friendly Bronze ingestion for raw JSONL on GCS."""

from __future__ import annotations

import argparse
import logging
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingest job for Dataproc Serverless.")
    parser.add_argument("--env", required=True, help="Runtime environment name (e.g. dev, prod).")
    parser.add_argument("--gcs_bucket", required=True, help="GCS bucket containing raw and bronze data.")
    parser.add_argument("--raw_prefix", default="raw/", help="Raw prefix inside the bucket.")
    parser.add_argument("--bronze_prefix", default="bronze/", help="Bronze prefix inside the bucket.")
    parser.add_argument("--run_id", default=None, help="Optional run_id filter.")
    parser.add_argument("--ingest_date", default=None, help="Optional ingest_date (YYYY-MM-DD).")
    parser.add_argument("--batch_name", default=None, help="Optional batch_name filter.")
    parser.add_argument(
        "--mode",
        choices=["append", "overwrite-run"],
        default="append",
        help="Write mode: append or overwrite only the selected run_id.",
    )
    return parser.parse_args()


def normalize_prefix(prefix: str) -> str:
    cleaned = prefix.strip().strip("/")
    return f"{cleaned}/" if cleaned else ""


def build_raw_glob(
    bucket: str,
    raw_prefix: str,
    batch_name: Optional[str],
    ingest_date: Optional[str],
    run_id: Optional[str],
) -> str:
    raw_prefix = normalize_prefix(raw_prefix)

    def segment(name: str, value: Optional[str]) -> str:
        return f"{name}={value}" if value else f"{name}=*"

    return (
        f"gs://{bucket}/{raw_prefix}"
        f"{segment('batch_name', batch_name)}/"
        f"{segment('ingest_date', ingest_date)}/"
        f"{segment('run_id', run_id)}/*.jsonl"
    )


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


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    spark = SparkSession.builder.appName("bronze_ingest_dataproc").getOrCreate()

    input_glob = build_raw_glob(
        bucket=args.gcs_bucket,
        raw_prefix=args.raw_prefix,
        batch_name=args.batch_name,
        ingest_date=args.ingest_date,
        run_id=args.run_id,
    )
    logging.info("Reading raw JSONL from %s", input_glob)

    df = spark.read.json(input_glob)
    ensure_required_columns(df, MANDATORY_METADATA_COLS)

    if args.run_id:
        df = df.filter(F.col("run_id") == args.run_id)
    if args.batch_name:
        df = df.filter(F.col("raw_path").contains(f"batch_name={args.batch_name}/"))

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
    if args.mode == "append":
        df_out = df.dropDuplicates(["run_id", "record_hash"])
    else:
        if not run_id_value:
            raise ValueError("overwrite-run requires --run_id.")
        df_out = df

    dedupe_count = df_out.count()

    bronze_root = f"gs://{args.gcs_bucket}/{normalize_prefix(args.bronze_prefix).rstrip('/')}"
    partition_cols = ["ingest_date", "run_id"]
    if "source_type" in df_out.columns:
        partition_cols.append("source_type")

    if args.mode == "overwrite-run":
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        write_mode = "overwrite"
    else:
        write_mode = "append"

    logging.info("Writing bronze data to %s", bronze_root)
    (
        df_out.write
        .mode(write_mode)
        .partitionBy(*partition_cols)
        .parquet(bronze_root)
    )

    output_path = f"{bronze_root}/ingest_date={ingest_date_value}/run_id={run_id_value}/"

    logging.info("Rows read: %s", row_count)
    logging.info("Rows after dedupe: %s", dedupe_count)
    logging.info("Rows written: %s", dedupe_count)
    logging.info("Output path: %s", output_path)

    spark.stop()


if __name__ == "__main__":
    main()
