from __future__ import annotations

from src.agent_service.retrieval import EvidenceDocument, EvidenceRetriever, default_evidence_corpus


def test_retrieval_uses_tag_filter_and_lexical_fallback() -> None:
    retriever = EvidenceRetriever(default_evidence_corpus(), use_openai_embeddings=False)

    dq_docs = retriever.search("deadletter rule failures", top_k=3, required_tag="dq")
    assert len(dq_docs) >= 1
    assert all("dq" in doc.tags for doc in dq_docs)
    assert any(doc.uri == "bq://ops.deadletter_summary" for doc in dq_docs)


def test_retrieval_returns_ops_docs_for_ops_queries() -> None:
    retriever = EvidenceRetriever(default_evidence_corpus(), use_openai_embeddings=False)

    docs = retriever.search("latest pipeline status and timings", top_k=2, required_tag="ops")
    uris = [doc.uri for doc in docs]
    assert "bq://ops.pipeline_runs" in uris


def test_retrieval_with_custom_embedder() -> None:
    docs = [
        EvidenceDocument(doc_id="a", uri="bq://ops.pipeline_runs", text="pipeline status", tags=("ops",)),
        EvidenceDocument(doc_id="b", uri="bq://ops.schema_registry", text="schema lineage", tags=("ops",)),
    ]

    vector_map = {
        "pipeline status": [1.0, 0.0],
        "schema lineage": [0.0, 1.0],
        "pipeline": [0.99, 0.01],
    }

    def embed_fn(texts: list[str]) -> list[list[float]]:
        return [vector_map[text] for text in texts]

    retriever = EvidenceRetriever(docs, use_openai_embeddings=True, embed_fn=embed_fn)
    out = retriever.search("pipeline", top_k=1, required_tag="ops")
    assert out[0].uri == "bq://ops.pipeline_runs"
