from __future__ import annotations

import json

from pyspark.sql import SparkSession

from src.silver.silver_transform_dataproc import (
    apply_dq,
    build_execution_steps,
    build_feedback_step,
    build_ratings_long,
    build_violations,
    ensure_array_string,
    ensure_struct,
    with_final_scores,
    AUTO_RATER_SCHEMA,
    EXECUTION_SCHEMA,
    RATING_SCHEMA,
)


def _spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("silver-tests").getOrCreate()


def test_with_final_scores_uses_reviewer_and_average() -> None:
    spark = _spark()
    try:
        rows = [
            {
                "run_id": "r1",
                "record_hash": "h1",
                "prompt_id": "p1",
                "query_text": "q",
                "evaluated_step_index": 0,
                "curator_1_rating": json.dumps({"primary_intent": 2, "overall_label": "LLM_RATED_BAD"}),
                "curator_2_rating": json.dumps({"primary_intent": 4, "overall_label": "LLM_RATED_OK"}),
                "reviewer_curator_1_rating": json.dumps({"primary_intent": 5, "overall_label": "LLM_RATED_GOOD"}),
                "reviewer_curator_2_rating": "{}",
                "auto_rater": "{}",
                "ingest_date": "2026-02-08",
            }
        ]
        df = spark.createDataFrame(rows)
        df = ensure_struct(df, "curator_1_rating", RATING_SCHEMA)
        df = ensure_struct(df, "curator_2_rating", RATING_SCHEMA)
        df = ensure_struct(df, "reviewer_curator_1_rating", RATING_SCHEMA)
        df = ensure_struct(df, "reviewer_curator_2_rating", RATING_SCHEMA)
        df = ensure_struct(df, "auto_rater", AUTO_RATER_SCHEMA)
        out = with_final_scores(df).collect()[0]

        assert out["final_primary_intent"] == 4.5
        assert out["final_overall_label"] == "LLM_RATED_GOOD"
    finally:
        spark.stop()


def test_apply_dq_flags_missing_fields_and_out_of_range() -> None:
    spark = _spark()
    try:
        rows = [
            {
                "run_id": "",
                    "record_hash": "",
                    "prompt_id": "",
                "query_text": "",
                "evaluated_step_index": -1,
                "final_primary_intent": 9.0,
                "final_information_gain": 3.0,
                "final_reasoning": 3.0,
                "final_understanding": 3.0,
                "final_implementation": 3.0,
                "final_trajectory_robustness": 3.0,
                "final_overall_label": "UNKNOWN",
            }
        ]
        df = spark.createDataFrame(rows)
        out = apply_dq(df).collect()[0]

        assert out["dq_pass"] is False
        reasons = set(out["dq_reasons"])
        assert "missing_run_id" in reasons
        assert "missing_record_hash" in reasons
        assert "invalid_evaluated_step_index" in reasons
        assert "out_of_range_primary_intent" in reasons
        assert "invalid_final_overall_label" in reasons
    finally:
        spark.stop()


def test_build_child_tables_from_nested_payloads() -> None:
    spark = _spark()
    try:
        rows = [
            {
                "ingest_date": "2026-02-08",
                "run_id": "r1",
                "record_hash": "h1",
                "schema_hash": "s1",
                "source_type": "json",
                "raw_path": "gs://bucket/path",
                "batch_id": "b1",
                "batch_name": "b1",
                "pod_name": "pod",
                "pod_type": "vertical",
                "task_type": "task",
                "set_id": "set_1",
                "prompt_id": "prompt_1",
                "query_text": "hello",
                "step_index": 0,
                "evaluated_step_index": 0,
                "step_type": "search",
                "execution_json": json.dumps({
                    "model_version": "m1",
                    "final_answer": "ans",
                    "steps": [
                        {"step_index": 0, "step_type": "search", "tool_name": "search", "query": "q"},
                        {"step_index": 1, "step_type": "final", "partial_answer": "done"},
                    ],
                }),
                "violations": ["POLICY"],
                "auto_rater": json.dumps({
                    "scores": {
                        "primary_intent": 3,
                        "information_gain": 4,
                        "reasoning": 5,
                        "understanding": 4,
                        "implementation": 3,
                        "trajectory_robustness": 2,
                        "overall_label": "LLM_RATED_OK",
                    },
                    "violations": ["TOXICITY"],
                    "confidence": 0.7,
                    "rubric_version": "r1",
                    "model_version": "auto-v1",
                    "final_reviewer": "auto",
                }),
                "curator_1_rating": json.dumps({
                    "primary_intent": 3,
                    "information_gain": 4,
                    "reasoning": 5,
                    "understanding": 4,
                    "implementation": 3,
                    "trajectory_robustness": 2,
                    "overall_label": "LLM_RATED_OK",
                }),
                "curator_2_rating": json.dumps({
                    "primary_intent": 4,
                    "information_gain": 4,
                    "reasoning": 4,
                    "understanding": 4,
                    "implementation": 4,
                    "trajectory_robustness": 4,
                    "overall_label": "LLM_RATED_GOOD",
                }),
                "reviewer_curator_1_rating": json.dumps({}),
                "reviewer_curator_2_rating": json.dumps({}),
                "dq_pass": True,
                "dq_reasons": ["none"],
            }
        ]
        df = spark.createDataFrame(rows)
        df = ensure_struct(df, "execution_json", EXECUTION_SCHEMA)
        df = ensure_struct(df, "auto_rater", AUTO_RATER_SCHEMA)
        df = ensure_struct(df, "curator_1_rating", RATING_SCHEMA)
        df = ensure_struct(df, "curator_2_rating", RATING_SCHEMA)
        df = ensure_struct(df, "reviewer_curator_1_rating", RATING_SCHEMA)
        df = ensure_struct(df, "reviewer_curator_2_rating", RATING_SCHEMA)
        df = ensure_array_string(df, "violations")
        df = with_final_scores(df)

        feedback = build_feedback_step(df)
        ratings = build_ratings_long(df)
        steps = build_execution_steps(df)
        violations = build_violations(df)

        assert feedback.count() == 1
        assert ratings.count() > 0
        assert steps.count() == 2
        assert violations.count() == 2
    finally:
        spark.stop()
