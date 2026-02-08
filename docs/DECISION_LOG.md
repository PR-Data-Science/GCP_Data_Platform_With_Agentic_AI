# Decision Log

| Date | Decision | Rationale |
| --- | --- | --- |
| 2026-02-07 | Dev-only deployments for now | Reduce complexity while iterating quickly. |
| 2026-02-07 | Schema drift handled via rescued_data + schema_registry | Preserve unexpected fields while tracking changes. |
| 2026-02-07 | Dedup uses record_hash + merge keys | Ensure stable identity and safe merges. |
