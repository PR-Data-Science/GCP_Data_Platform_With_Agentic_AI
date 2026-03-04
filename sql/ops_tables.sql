-- BigQuery Ops Control Plane DDL
-- Usage example:
-- bq query --use_legacy_sql=false < sql/ops_tables.sql

CREATE SCHEMA IF NOT EXISTS `ops`;

CREATE TABLE IF NOT EXISTS `ops.pipeline_runs` (
  run_id STRING NOT NULL,
  stage STRING NOT NULL,
  status STRING NOT NULL,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  duration_ms INT64,
  input_count INT64,
  output_count INT64,
  deadletter_count INT64,
  schema_hash STRING,
  dataproc_batch_id STRING,
  manifest_path STRING,
  error_category STRING,
  error_code STRING,
  error_summary STRING,
  code_version STRING,
  input_paths ARRAY<STRING>,
  output_paths ARRAY<STRING>,
  partition_keys ARRAY<STRING>,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY run_id, stage;

CREATE TABLE IF NOT EXISTS `ops.dq_results` (
  run_id STRING NOT NULL,
  stage STRING NOT NULL,
  table_name STRING NOT NULL,
  rule_id STRING NOT NULL,
  severity STRING,
  failed_count INT64,
  sample_record_hashes ARRAY<STRING>,
  dq_pass BOOL,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY run_id, stage, table_name, rule_id;

CREATE TABLE IF NOT EXISTS `ops.schema_registry` (
  schema_hash STRING NOT NULL,
  schema_json STRING NOT NULL,
  first_seen_run_id STRING NOT NULL,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  source_type STRING
)
PARTITION BY DATE(created_ts)
CLUSTER BY schema_hash, source_type;

CREATE TABLE IF NOT EXISTS `ops.deadletter_summary` (
  run_id STRING NOT NULL,
  stage STRING NOT NULL,
  rule_id STRING,
  failure_reason STRING NOT NULL,
  count INT64 NOT NULL,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY run_id, stage, rule_id;

CREATE TABLE IF NOT EXISTS `ops.dq_rule_registry` (
  rule_id STRING NOT NULL,
  name STRING NOT NULL,
  description STRING,
  severity STRING,
  target_table STRING,
  logic_reference STRING,
  owner STRING,
  enabled BOOL,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY rule_id, target_table;

CREATE TABLE IF NOT EXISTS `ops.agent_sessions` (
  session_id STRING NOT NULL,
  user_id STRING,
  mode STRING,
  read_only BOOL,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY session_id, mode;

CREATE TABLE IF NOT EXISTS `ops.agent_tool_calls` (
  event_id STRING NOT NULL,
  session_id STRING NOT NULL,
  route STRING,
  tool_name STRING NOT NULL,
  tool_args_json STRING,
  evidence_refs ARRAY<STRING>,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY session_id, route, tool_name;

CREATE TABLE IF NOT EXISTS `ops.agent_responses` (
  event_id STRING NOT NULL,
  session_id STRING NOT NULL,
  route STRING,
  query_text STRING,
  response_text STRING,
  evidence_refs ARRAY<STRING>,
  read_only BOOL,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY session_id, route;

CREATE TABLE IF NOT EXISTS `ops.agent_proposals` (
  proposal_id STRING NOT NULL,
  session_id STRING NOT NULL,
  route STRING,
  title STRING,
  proposal_text STRING,
  evidence_refs ARRAY<STRING>,
  status STRING,
  created_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_ts)
CLUSTER BY proposal_id, session_id, status;
