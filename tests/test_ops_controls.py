from __future__ import annotations

from types import SimpleNamespace

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ops import error_taxonomy, ops_writer
from src.silver.silver_transform_dataproc import dq_reason_to_rule_id, dq_rule_id_to_severity


def test_error_taxonomy_permission_mapping() -> None:
    info = error_taxonomy.from_exception(RuntimeError("Access denied: missing permission to write"))
    assert info.category == "IAM_PERMISSION"
    assert info.code == "RUNTIMEERROR"


def test_error_taxonomy_fallback_runtime() -> None:
    info = error_taxonomy.from_exception(Exception("unexpected issue"))
    assert info.category == "RUNTIME"
    assert info.code == "EXCEPTION"


def test_write_schema_registry_first_seen_filters_existing(monkeypatch) -> None:
    captured: list[dict] = []

    class FakeQueryJob:
        def result(self):
            return [{"schema_hash": "known"}]

    class FakeClient:
        project = "unit-test-project"

        def query(self, _query: str, job_config=None):
            assert job_config is not None
            return FakeQueryJob()

    fake_bigquery = SimpleNamespace(
        Client=lambda: FakeClient(),
        QueryJobConfig=lambda query_parameters: {"query_parameters": query_parameters},
        ArrayQueryParameter=lambda name, typ, values: {"name": name, "typ": typ, "values": values},
    )

    monkeypatch.setattr(ops_writer, "bigquery", fake_bigquery)
    monkeypatch.setattr(ops_writer, "write_schema_registry", lambda rows, dataset="ops": captured.extend(list(rows)))

    ops_writer.write_schema_registry_first_seen(
        [
            {"schema_hash": "known", "schema_json": "{}", "first_seen_run_id": "r1", "source_type": "silver"},
            {"schema_hash": "new", "schema_json": "{}", "first_seen_run_id": "r2", "source_type": "gold"},
        ],
        dataset="ops",
    )

    assert len(captured) == 1
    assert captured[0]["schema_hash"] == "new"


def test_write_schema_registry_first_seen_without_bigquery(monkeypatch) -> None:
    captured: list[dict] = []

    monkeypatch.setattr(ops_writer, "bigquery", None)
    monkeypatch.setattr(ops_writer, "write_schema_registry", lambda rows, dataset="ops": captured.extend(list(rows)))

    ops_writer.write_schema_registry_first_seen(
        [{"schema_hash": "s1", "schema_json": "{}", "first_seen_run_id": "r1", "source_type": "bronze"}],
        dataset="ops",
    )

    assert len(captured) == 1
    assert captured[0]["schema_hash"] == "s1"


def test_dq_reason_and_severity_mapping() -> None:
    spark = SparkSession.builder.master("local[1]").appName("dq-map-tests").getOrCreate()
    try:
        df = spark.createDataFrame(
            [
                {"reason": "missing_run_id"},
                {"reason": "invalid_final_overall_label"},
                {"reason": "out_of_range_primary_intent"},
                {"reason": "unknown_reason"},
            ]
        )
        out = (
            df.withColumn("rule_id", dq_reason_to_rule_id(F.col("reason")))
            .withColumn("severity", dq_rule_id_to_severity(F.col("rule_id")))
            .collect()
        )

        lookup = {row["reason"]: (row["rule_id"], row["severity"]) for row in out}
        assert lookup["missing_run_id"] == ("DQ_RUN_ID_REQUIRED", "CRITICAL")
        assert lookup["invalid_final_overall_label"] == ("DQ_FINAL_OVERALL_LABEL_ALLOWED", "HIGH")
        assert lookup["out_of_range_primary_intent"] == ("DQ_METRIC_RANGE_0_5", "HIGH")
        assert lookup["unknown_reason"] == ("DQ_UNKNOWN", "MEDIUM")
    finally:
        spark.stop()
