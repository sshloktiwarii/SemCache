# SemCache - Product Requirements Document

## Core Thesis
SemCache is a high-performance, local-first HTTP proxy gateway designed to intercept, canonicalize, and semantically cache LLM API traffic (specifically OpenAI-compatible endpoints). It normalizes requests, caches exact matches via deterministic BLAKE3 hashing, and caches semantic matches via fuzzy vector similarity search using `sqlite-vec`. 

## Target Audience
AI infrastructure engineers and local AI agent developers who require a cost-control and latency-reduction plane for autonomous AI agents that frequently execute non-deterministic, repetitive loops.

## Core Features (MVP)
- **Proxy Endpoints:** Intercepts and proxies OpenAI-compatible `/v1/chat/completions` and `/v1/embeddings` API endpoints.
- **Request Canonicalization:** Parses JSON payloads and strips volatile, non-semantic fields (e.g., `temperature`, `top_p`, `user`, `seed`) to maximize cache hit rates without altering semantic intent.
- **L1 Cache (Exact Match):** Deterministic caching using BLAKE3 hashing on canonicalized requests.
- **L2 Cache (Semantic Match):** Fuzzy vector-similarity matching using `sqlite-vec`. A Cosine similarity > 0.92 (distance <= 0.08) triggers a cache hit.
- **SQLite Persistence:** Uses SQLite in WAL mode to persist cache payloads, vectors, and telemetry without external infrastructure.
- **Single-Flight Request Coalescing:** Identifies concurrent identical in-flight requests and coalesces them into a single upstream fetch, preventing API rate-limit exhaustion and catastrophic billing spikes during agent failure modes.

## Out of Scope for MVP
- Multi-tenant authentication
- Distributed Redis caching (local-first SQLite is sufficient)
- Streaming response interception (only unary JSON is supported in MVP)
