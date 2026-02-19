"""Generate deterministic sample batch drop files for LLM feedback pipeline."""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "drop" / "team=Magi_Code_Python" / "task=EAC_NEXT_STEPS_SIMPLIFIED" / "dt=2026-01-23"

AUTO_BATCH_ID = "python_training_version1_LLMrated_batch"
HUMAN_BATCH_ID = "python_training_version1_HUMANrated_batch"

POD_NAME = "Magi_Code_Python"
POD_TYPE = "vertical"
TASK_TYPE = "EAC_NEXT_STEPS_SIMPLIFIED"

MODEL_VERSION = "gpt-4.1-mini-2026-01-15"
AUTO_RATER_MODEL_VERSION = "auto-rater-v2"
RUBRIC_VERSION = "rubric_v1"

VIOLATION_ENUM = ["TOXICITY", "HALLUCINATION", "POLICY"]


@dataclass
class PromptRun:
    prompt_id: str
    query_text: str
    steps: List[Dict[str, Any]]
    final_answer: str


def rating_label(avg_score: float) -> str:
    if avg_score < 2.0:
        return "LLM_RATED_BAD"
    if avg_score < 3.5:
        return "LLM_RATED_OK"
    return "LLM_RATED_GOOD"


def make_rating(rng: random.Random) -> Dict[str, Any]:
    dims = {
        "primary_intent": rng.randint(0, 5),
        "information_gain": rng.randint(0, 5),
        "reasoning": rng.randint(0, 5),
        "understanding": rng.randint(0, 5),
        "implementation": rng.randint(0, 5),
        "trajectory_robustness": rng.randint(0, 5),
    }
    avg_score = sum(dims.values()) / len(dims)
    dims["overall_label"] = rating_label(avg_score)
    return dims


def tweak_rating(rating: Dict[str, Any], rng: random.Random) -> Dict[str, Any]:
    updated = dict(rating)
    dim_keys = [
        "primary_intent",
        "information_gain",
        "reasoning",
        "understanding",
        "implementation",
        "trajectory_robustness",
    ]
    dim = rng.choice(dim_keys)
    delta = rng.choice([-1, 1])
    updated[dim] = max(0, min(5, updated[dim] + delta))
    avg_score = sum(updated[k] for k in dim_keys) / len(dim_keys)
    updated["overall_label"] = rating_label(avg_score)
    return updated


def make_steps(rng: random.Random, prompt_id: str, query_text: str, step_count: int) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    for idx in range(step_count):
        if idx == step_count - 1:
            step_type = "final"
        elif idx == 0:
            step_type = "search"
        else:
            step_type = rng.choice(["tool", "search"])

        step: Dict[str, Any] = {
            "step_index": idx,
            "step_type": step_type,
        }

        if step_type == "search":
            step.update(
                {
                    "tool_name": "search",
                    "query": f"Search notes for: {query_text}",
                    "tool_output": "Found 3 relevant snippets.",
                    "partial_answer": f"Relevant hints for step {idx}.",
                }
            )
        elif step_type == "tool":
            step.update(
                {
                    "tool_name": "python",
                    "tool_input": {"code": f"print('step {idx} analysis')"},
                    "tool_output": "step analysis complete",
                    "partial_answer": f"Intermediate result {idx}.",
                }
            )
        else:
            step.update(
                {
                    "partial_answer": f"Finalizing response for {prompt_id}.",
                }
            )

        steps.append(step)

    return steps


def build_prompt_run(rng: random.Random, prompt_index: int) -> PromptRun:
    prompt_id = f"prompt_{prompt_index:04d}"
    query_text = f"Provide next steps for EAC scenario {prompt_index}."
    step_count = rng.randint(3, 8)
    steps = make_steps(rng, prompt_id, query_text, step_count)
    final_answer = (
        "Summarize findings, propose a remediation plan, and outline follow-up actions with owners."
    )
    return PromptRun(prompt_id=prompt_id, query_text=query_text, steps=steps, final_answer=final_answer)


def auto_rater_payload(rng: random.Random) -> Dict[str, Any]:
    scores = make_rating(rng)
    violations = [] if rng.random() < 0.8 else [rng.choice(VIOLATION_ENUM)]
    return {
        "scores": scores,
        "violations": violations,
        "confidence": round(rng.uniform(0.6, 0.95), 2),
        "rubric_version": RUBRIC_VERSION,
        "model_version": AUTO_RATER_MODEL_VERSION,
        "final_reviewer": "auto",
    }


def choose_row_violations(rng: random.Random) -> List[str]:
    if rng.random() < 0.85:
        return []
    return [rng.choice(VIOLATION_ENUM)]


def write_json_array(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, ensure_ascii=True, indent=2)


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_auto_batch(rng: random.Random) -> Dict[str, Any]:
    batch_rows: List[Dict[str, Any]] = []
    prompt_count = 30

    for idx in range(prompt_count):
        prompt = build_prompt_run(rng, idx)
        execution_json = {
            "prompt_id": prompt.prompt_id,
            "query_text": prompt.query_text,
            "steps": prompt.steps,
            "final_answer": prompt.final_answer,
            "model_version": MODEL_VERSION,
        }

        for step in prompt.steps:
            auto_rater = auto_rater_payload(rng)
            batch_rows.append(
                {
                    "pod_name": POD_NAME,
                    "pod_type": POD_TYPE,
                    "task_type": TASK_TYPE,
                    "batch_id": AUTO_BATCH_ID,
                    "batch_name": AUTO_BATCH_ID,
                    "set_id": None,
                    "prompt_id": prompt.prompt_id,
                    "query_text": prompt.query_text,
                    "step_index": step["step_index"],
                    "evaluated_step_index": step["step_index"],
                    "step_type": step["step_type"],
                    "execution_json": execution_json,
                    "violations": choose_row_violations(rng),
                    "auto_rater": auto_rater,
                }
            )

    output_path = (
        DATA_DIR / f"batch_id={AUTO_BATCH_ID}" / f"{AUTO_BATCH_ID}.json"
    )
    write_json_array(output_path, batch_rows)
    return {
        "path": output_path,
        "prompt_count": prompt_count,
        "row_count": len(batch_rows),
    }


def generate_human_batch(rng: random.Random) -> Dict[str, Any]:
    batch_rows: List[Dict[str, Any]] = []
    prompt_count = 10

    for idx in range(prompt_count):
        prompt = build_prompt_run(rng, idx + 1000)
        execution_json = {
            "prompt_id": prompt.prompt_id,
            "query_text": prompt.query_text,
            "steps": prompt.steps,
            "final_answer": prompt.final_answer,
            "model_version": MODEL_VERSION,
        }

        for step in prompt.steps:
            curator_1 = make_rating(rng)
            curator_2 = make_rating(rng)

            batch_rows.append(
                {
                    "pod_name": POD_NAME,
                    "pod_type": POD_TYPE,
                    "task_type": TASK_TYPE,
                    "batch_id": HUMAN_BATCH_ID,
                    "batch_name": HUMAN_BATCH_ID,
                    "set_id": None,
                    "prompt_id": prompt.prompt_id,
                    "query_text": prompt.query_text,
                    "step_index": step["step_index"],
                    "evaluated_step_index": step["step_index"],
                    "step_type": step["step_type"],
                    "execution_json": execution_json,
                    "violations": choose_row_violations(rng),
                    "curator_1_rating": curator_1,
                    "curator_2_rating": curator_2,
                    "auto_rater": None,
                }
            )

    total_rows = len(batch_rows)
    base = total_rows // 4
    remainder = total_rows % 4
    set_sizes = [base + (1 if i < remainder else 0) for i in range(4)]

    set_ids = [f"set_{i:03d}" for i in range(1, 5)]
    row_index = 0
    modified_rows = set(rng.sample(range(total_rows), k=max(1, int(total_rows * 0.3))))

    for set_id, size in zip(set_ids, set_sizes):
        for _ in range(size):
            row = batch_rows[row_index]
            row["set_id"] = set_id

            curator_1 = row["curator_1_rating"]
            curator_2 = row["curator_2_rating"]

            if row_index in modified_rows:
                row["reviewer_curator_1_rating"] = tweak_rating(curator_1, rng)
                row["reviewer_curator_2_rating"] = tweak_rating(curator_2, rng)
                row["reviewer_comments"] = "Adjusted scores after review."
            else:
                row["reviewer_curator_1_rating"] = dict(curator_1)
                row["reviewer_curator_2_rating"] = dict(curator_2)
                row["reviewer_comments"] = "" if rng.random() < 0.6 else "Looks consistent."

            row_index += 1

    csv_rows: List[Dict[str, Any]] = []
    for row in batch_rows:
        csv_rows.append(
            {
                **row,
                "execution_json": json.dumps(row["execution_json"], ensure_ascii=True),
                "curator_1_rating": json.dumps(row["curator_1_rating"], ensure_ascii=True),
                "curator_2_rating": json.dumps(row["curator_2_rating"], ensure_ascii=True),
                "reviewer_curator_1_rating": json.dumps(
                    row["reviewer_curator_1_rating"], ensure_ascii=True
                ),
                "reviewer_curator_2_rating": json.dumps(
                    row["reviewer_curator_2_rating"], ensure_ascii=True
                ),
                "violations": json.dumps(row["violations"], ensure_ascii=True),
            }
        )

    fieldnames = [
        "pod_name",
        "pod_type",
        "task_type",
        "batch_id",
        "batch_name",
        "set_id",
        "prompt_id",
        "query_text",
        "step_index",
        "evaluated_step_index",
        "step_type",
        "execution_json",
        "violations",
        "curator_1_rating",
        "curator_2_rating",
        "reviewer_curator_1_rating",
        "reviewer_curator_2_rating",
        "reviewer_comments",
        "auto_rater",
    ]

    output_path = (
        DATA_DIR / f"batch_id={HUMAN_BATCH_ID}" / f"{HUMAN_BATCH_ID}.csv"
    )
    write_csv(output_path, csv_rows, fieldnames)
    return {
        "path": output_path,
        "prompt_count": prompt_count,
        "row_count": len(batch_rows),
        "set_distribution": dict(zip(set_ids, set_sizes)),
    }


def main() -> None:
    rng = random.Random(42)

    auto_stats = generate_auto_batch(rng)
    human_stats = generate_human_batch(rng)

    print("Sample batch generation complete.")
    print(f"AUTO batch path: {auto_stats['path']}")
    print(f"AUTO prompts: {auto_stats['prompt_count']}, rows: {auto_stats['row_count']}")
    print(f"HUMAN batch path: {human_stats['path']}")
    print(f"HUMAN prompts: {human_stats['prompt_count']}, rows: {human_stats['row_count']}")
    print(f"HUMAN set distribution: {human_stats['set_distribution']}")


if __name__ == "__main__":
    main()
