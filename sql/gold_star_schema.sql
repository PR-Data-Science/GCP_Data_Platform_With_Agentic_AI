CREATE OR REPLACE TABLE `__PROJECT_ID__.__BQ_DATASET__.dim_date` AS
SELECT DISTINCT
  ingest_date AS date_key,
  EXTRACT(YEAR FROM ingest_date) AS year,
  EXTRACT(MONTH FROM ingest_date) AS month,
  EXTRACT(DAY FROM ingest_date) AS day,
  EXTRACT(DAYOFWEEK FROM ingest_date) AS day_of_week
FROM `__PROJECT_ID__.__BQ_DATASET__.gold_model_eval_step_metrics`;

CREATE OR REPLACE TABLE `__PROJECT_ID__.__BQ_DATASET__.dim_model` AS
SELECT DISTINCT
  TO_HEX(SHA256(IFNULL(model_version, 'unknown'))) AS model_key,
  IFNULL(model_version, 'unknown') AS model_version
FROM `__PROJECT_ID__.__BQ_DATASET__.gold_model_eval_step_metrics`;

CREATE OR REPLACE TABLE `__PROJECT_ID__.__BQ_DATASET__.dim_task` AS
SELECT DISTINCT
  TO_HEX(SHA256(IFNULL(task_type, 'unknown'))) AS task_key,
  IFNULL(task_type, 'unknown') AS task_type
FROM `__PROJECT_ID__.__BQ_DATASET__.gold_model_eval_step_metrics`;

CREATE OR REPLACE TABLE `__PROJECT_ID__.__BQ_DATASET__.dim_step_type` AS
SELECT DISTINCT
  TO_HEX(SHA256(IFNULL(step_type, 'unknown'))) AS step_type_key,
  IFNULL(step_type, 'unknown') AS step_type
FROM `__PROJECT_ID__.__BQ_DATASET__.gold_model_eval_step_metrics`;

CREATE OR REPLACE TABLE `__PROJECT_ID__.__BQ_DATASET__.dim_label` AS
SELECT DISTINCT
  TO_HEX(SHA256(IFNULL(final_overall_label, 'unknown'))) AS label_key,
  IFNULL(final_overall_label, 'unknown') AS final_overall_label
FROM `__PROJECT_ID__.__BQ_DATASET__.gold_model_eval_step_metrics`;

CREATE OR REPLACE TABLE `__PROJECT_ID__.__BQ_DATASET__.fact_model_eval_step` AS
SELECT
  m.ingest_date AS date_key,
  TO_HEX(SHA256(IFNULL(m.model_version, 'unknown'))) AS model_key,
  TO_HEX(SHA256(IFNULL(m.task_type, 'unknown'))) AS task_key,
  TO_HEX(SHA256(IFNULL(m.step_type, 'unknown'))) AS step_type_key,
  TO_HEX(SHA256(IFNULL(m.final_overall_label, 'unknown'))) AS label_key,
  m.run_id,
  m.prompt_id,
  m.evaluated_step_index,
  m.final_primary_intent,
  m.final_information_gain,
  m.final_reasoning,
  m.final_understanding,
  m.final_implementation,
  m.final_trajectory_robustness,
  m.is_bad,
  m.gold_processed_ts
FROM `__PROJECT_ID__.__BQ_DATASET__.gold_model_eval_step_metrics` m;

CREATE OR REPLACE TABLE `__PROJECT_ID__.__BQ_DATASET__.fact_failure_breakdown` AS
SELECT
  f.ingest_date AS date_key,
  TO_HEX(SHA256(IFNULL(f.model_version, 'unknown'))) AS model_key,
  TO_HEX(SHA256(IFNULL(f.task_type, 'unknown'))) AS task_key,
  f.run_id,
  f.failure_type,
  f.failure_value,
  f.failure_count,
  f.gold_processed_ts
FROM `__PROJECT_ID__.__BQ_DATASET__.gold_model_eval_failure_breakdown` f;
