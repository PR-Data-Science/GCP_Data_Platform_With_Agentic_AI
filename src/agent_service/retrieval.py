from __future__ import annotations

import math
import os
from dataclasses import dataclass
from threading import Lock
from typing import Callable


@dataclass(frozen=True)
class EvidenceDocument:
    doc_id: str
    uri: str
    text: str
    tags: tuple[str, ...]


def _tokenize(text: str) -> set[str]:
    cleaned = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return {token for token in cleaned.split() if token}


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


class EvidenceRetriever:
    def __init__(
        self,
        documents: list[EvidenceDocument],
        embedding_model: str = "text-embedding-3-small",
        use_openai_embeddings: bool | None = None,
        embed_fn: Callable[[list[str]], list[list[float]]] | None = None,
    ) -> None:
        self._documents = documents
        self._embedding_model = embedding_model
        self._lock = Lock()
        self._is_built = False
        self._doc_embeddings: dict[str, list[float]] = {}

        env_flag = os.getenv("AGENT_USE_OPENAI_EMBEDDINGS", "true").strip().lower()
        env_enabled = env_flag in {"1", "true", "yes", "y"}
        self._use_openai_embeddings = env_enabled if use_openai_embeddings is None else use_openai_embeddings

        self._openai_client = None
        self._embed_fn = embed_fn

    def _ensure_embedder(self) -> None:
        if self._embed_fn is not None:
            return
        if not self._use_openai_embeddings:
            return

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return

        try:
            from openai import OpenAI  # lazy import

            self._openai_client = OpenAI(api_key=api_key)

            def _openai_embed(texts: list[str]) -> list[list[float]]:
                response = self._openai_client.embeddings.create(model=self._embedding_model, input=texts)
                return [item.embedding for item in response.data]

            self._embed_fn = _openai_embed
        except Exception:
            self._embed_fn = None

    def build(self) -> None:
        with self._lock:
            if self._is_built:
                return
            self._ensure_embedder()
            if self._embed_fn is not None:
                try:
                    texts = [doc.text for doc in self._documents]
                    vectors = self._embed_fn(texts)
                    self._doc_embeddings = {doc.doc_id: vector for doc, vector in zip(self._documents, vectors)}
                except Exception:
                    self._doc_embeddings = {}
            self._is_built = True

    def search(self, query: str, top_k: int = 3, required_tag: str | None = None) -> list[EvidenceDocument]:
        self.build()

        candidates = [doc for doc in self._documents if required_tag is None or required_tag in doc.tags]
        if not candidates:
            return []

        query_tokens = _tokenize(query)
        query_embedding: list[float] | None = None

        if self._embed_fn is not None:
            try:
                query_embedding = self._embed_fn([query])[0]
            except Exception:
                query_embedding = None

        scored: list[tuple[float, EvidenceDocument]] = []
        for index, doc in enumerate(candidates):
            doc_tokens = _tokenize(doc.text)
            lexical_overlap = len(query_tokens & doc_tokens)
            lexical_score = lexical_overlap / max(1, len(query_tokens))

            embedding_score = 0.0
            if query_embedding is not None:
                doc_vector = self._doc_embeddings.get(doc.doc_id)
                if doc_vector is not None:
                    embedding_score = _cosine_similarity(query_embedding, doc_vector)

            final_score = (0.7 * embedding_score) + (0.3 * lexical_score)
            tie_breaker = 1.0 / (index + 1000)
            scored.append((final_score + tie_breaker, doc))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [doc for _, doc in scored[: max(1, top_k)]]


def default_evidence_corpus() -> list[EvidenceDocument]:
    return [
        EvidenceDocument(
            doc_id="ops_pipeline_runs",
            uri="bq://ops.pipeline_runs",
            text="Pipeline stage status, timings, counts, manifest path, code_version and partition lineage.",
            tags=("ops",),
        ),
        EvidenceDocument(
            doc_id="ops_schema_registry",
            uri="bq://ops.schema_registry",
            text="Schema hash snapshots and first-seen lineage for bronze silver gold outputs.",
            tags=("ops",),
        ),
        EvidenceDocument(
            doc_id="ops_dq_results",
            uri="bq://ops.dq_results",
            text="Rule-level data quality results including failed count, severity, and sample record hashes.",
            tags=("dq",),
        ),
        EvidenceDocument(
            doc_id="ops_deadletter_summary",
            uri="bq://ops.deadletter_summary",
            text="Aggregated deadletter distribution by rule and failure reason with counts.",
            tags=("dq",),
        ),
        EvidenceDocument(
            doc_id="dq_rules_registry",
            uri="gcs://dq/dq_rules.yaml",
            text="DQ rule registry with rule_id, severity, owner, and logic reference.",
            tags=("dq",),
        ),
        EvidenceDocument(
            doc_id="manifest_convention",
            uri="gcs://manifests/<stage>/dt=<date>/run_id=<run_id>/manifest.json",
            text="Manifest location convention for raw bronze silver gold publish stages.",
            tags=("ops", "dq"),
        ),
    ]
