# Contracts Versioning Policy

This folder stores versioned schema contracts used by the pipeline.

## Structure
- `contracts/silver/*.json`
- `contracts/gold/*.json`

## Evolution policy

### Allowed (non-breaking)
- Additive fields (new nullable columns)
- Additive nested fields when optional

### Breaking (block/quarantine by default)
- Type changes
- Field renames/removals
- Breaking nested structure changes

## Versioning rules
- Use file naming format: `v<major>_<table_or_model>.json`
- Increment:
  - `major` for breaking contract changes
  - optional metadata patch updates in-file for non-breaking updates

## Runtime behavior
- Pipeline computes `schema_hash` per run/stage.
- If hash is first-seen, snapshot is written to `ops.schema_registry`.
- If breaking drift is detected, route to deadletter/quarantine and fail stage by default (configurable).
