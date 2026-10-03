# Architecture Decision Record (ADR) Log

## ADR 001: Rust over Go/Node.js
**Decision:** SemCache is implemented in Rust.
**Rationale:** Rust provides memory safety guarantees without a garbage collector, ensuring predictable, sub-millisecond latency for a proxy sitting directly in the hot path. Rust's zero-cost abstractions, robust asynchronous ecosystem (`tokio`, `axum`), and highly performant native C-bindings for SQLite (`rusqlite`) make it the optimal choice for a high-throughput cache gateway.

## ADR 002: SQLite + sqlite-vec over pgvector/Qdrant
**Decision:** We utilize a local SQLite database augmented with the `sqlite-vec` extension for persistence and vector search.
**Rationale:** SemCache is designed as a local-first deployment to run adjacent to autonomous agents. Introducing external infrastructure dependencies like PostgreSQL (pgvector) or Qdrant introduces unnecessary networking overhead, operational complexity, and deployment friction. SQLite operating in WAL mode provides more than sufficient concurrency for local agent proxy workflows.

## ADR 003: Single-Flight Request Coalescing
**Decision:** Implement a Single-Flight request coalescing engine using a concurrent hash map and broadcast channels.
**Rationale:** Autonomous agents frequently enter non-deterministic loops or parallel execution paths where they rapidly dispatch identical prompts. Without coalescing, this behavior causes catastrophic rate-limit exhaustion (429s) and severe billing spikes at the upstream API provider. Single-Flight coalescing intercepts identical concurrent requests and folds them into a single upstream fetch, gracefully handling agent failure modes.
