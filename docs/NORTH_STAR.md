# North Star

## 10-line North Star Flow
```
Source
→ GCS Raw
→ Bronze
→ Silver (DQ + deadletter)
→ Gold
→ BigQuery + Ops tables
```

## Part 1 vs Part 2
- Part 1: Core ingestion, DQ, and publishing pipeline.
- Part 2: Agents with Vertex AI ADK + RAG (mandatory, later phase).
