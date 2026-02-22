from __future__ import annotations

from pyspark.sql import SparkSession

from src.gold.gold_transform_dataproc import (
    build_model_comparison_daily,
    build_rater_agreement,
    build_training_supervised_examples,
)


def _spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("gold-tests").getOrCreate()


def test_build_training_supervised_examples() -> None:
    spark = _spark()
    try:
        feedback = spark.createDataFrame(
            [
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "task_type": "task",
                    "model_version": "m1",
                    "query_text": "q1",
                    "final_answer": "a1",
                    "evaluated_step_index": 0,
                    "final_primary_intent": 4.0,
                    "final_information_gain": 4.0,
                    "final_reasoning": 4.0,
                    "final_understanding": 4.0,
                    "final_implementation": 4.0,
                    "final_trajectory_robustness": 4.0,
                    "final_overall_label": "LLM_RATED_GOOD",
                },
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "task_type": "task",
                    "model_version": "m1",
                    "query_text": "q1",
                    "final_answer": "a1b",
                    "evaluated_step_index": 1,
                    "final_primary_intent": 5.0,
                    "final_information_gain": 5.0,
                    "final_reasoning": 5.0,
                    "final_understanding": 5.0,
                    "final_implementation": 5.0,
                    "final_trajectory_robustness": 5.0,
                    "final_overall_label": "LLM_RATED_GOOD",
                },
            ]
        )
        violations = spark.createDataFrame(
            [
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "evaluated_step_index": 0,
                    "violation_source": "record",
                    "violation": "POLICY",
                }
            ]
        )

        out = build_training_supervised_examples(feedback, violations)
        row = out.collect()[0]
        assert out.count() == 1
        assert row["final_answer"] == "a1b"
        assert row["violation_count"] == 1
        assert row["avg_quality_score"] == 5.0
    finally:
        spark.stop()


def test_build_model_comparison_daily() -> None:
    spark = _spark()
    try:
        feedback = spark.createDataFrame(
            [
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "task_type": "task",
                    "model_version": "m1",
                    "final_primary_intent": 4.0,
                    "final_information_gain": 4.0,
                    "final_reasoning": 4.0,
                    "final_understanding": 4.0,
                    "final_implementation": 4.0,
                    "final_trajectory_robustness": 4.0,
                    "final_overall_label": "LLM_RATED_OK",
                },
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p2",
                    "task_type": "task",
                    "model_version": "m1",
                    "final_primary_intent": 2.0,
                    "final_information_gain": 2.0,
                    "final_reasoning": 2.0,
                    "final_understanding": 2.0,
                    "final_implementation": 2.0,
                    "final_trajectory_robustness": 2.0,
                    "final_overall_label": "LLM_RATED_BAD",
                },
            ]
        )

        out = build_model_comparison_daily(feedback)
        row = out.collect()[0]
        assert row["step_count"] == 2
        assert row["bad_rate"] == 0.5
        assert row["avg_primary_intent"] == 3.0
    finally:
        spark.stop()


def test_build_rater_agreement() -> None:
    spark = _spark()
    try:
        ratings = spark.createDataFrame(
            [
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "evaluated_step_index": 0,
                    "metric_name": "reasoning",
                    "rater_type": "curator_1",
                    "metric_score": 4.0,
                },
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "evaluated_step_index": 0,
                    "metric_name": "reasoning",
                    "rater_type": "curator_2",
                    "metric_score": 2.0,
                },
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "evaluated_step_index": 0,
                    "metric_name": "reasoning",
                    "rater_type": "auto_rater",
                    "metric_score": 5.0,
                },
            ]
        )
        feedback = spark.createDataFrame(
            [
                {
                    "ingest_date": "2026-02-08",
                    "run_id": "r1",
                    "prompt_id": "p1",
                    "evaluated_step_index": 0,
                    "task_type": "task",
                    "model_version": "m1",
                }
            ]
        )

        out = build_rater_agreement(ratings, feedback)
        row = out.collect()[0]
        assert row["comparison_count"] == 1
        assert row["avg_abs_diff"] == 2.0
    finally:
        spark.stop()
