# SemCache - Architecture Document

## Proxy Layer
SemCache acts as an HTTP proxy utilizing `axum` and `hyper` to handle inbound HTTP requests. It accepts requests destined for upstream LLM providers, primarily targeting `/v1/chat/completions` and `/v1/embeddings`.

## Canonicalization Pipeline
Inbound requests are normalized to ensure identical semantic intents are cached efficiently despite trivial syntactical differences.
1. The JSON payload is parsed.
2. Non-deterministic hyperparameters (`temperature`, `top_p`, `frequency_penalty`, `presence_penalty`, `user`, `seed`, `logit_bias`) are stripped.
3. String whitespace inside `content` arrays is trimmed, and consecutive newlines are collapsed.
4. The remaining JSON keys are sorted and the object is serialized deterministically.
5. A BLAKE3 hash is computed over the deterministic byte array to form the `canonical_hash`.

## Vector Search Core
Persistence and semantic similarity searches are powered by `rusqlite` loaded with the `sqlite-vec` extension.
- The `fuzzy_cache` table utilizes a virtual table schema tailored for vector dimensions (e.g., `text-embedding-3-small` at 1536 dimensions).
- A cosine distance query is executed against the `fuzzy_cache` to determine if an incoming prompt is semantically identical (distance <= 0.08) to a historically cached prompt.

## State Machine & Request Coalescing
To prevent identical concurrent requests from overwhelming upstream APIs, a Single-Flight Coalescing state machine is implemented.
- We utilize a `DashMap` (e.g., `DashMap<[u8; 32], tokio::sync::broadcast::Sender<Bytes>>`) to track in-flight requests keyed by their `canonical_hash`.
- If an incoming request matches a hash currently in the map, it subscribes to the broadcast channel and asynchronously `await`s the upstream response.
- The primary worker fetches the response, broadcasts the payload to all pending subscribers, and removes the entry from the map.

## Database Schema
The SQLite persistence layer is strictly structured:

```sql
-- L1 Cache: Exact Deterministic Matches
CREATE TABLE IF NOT EXISTS exact_cache (
    canonical_hash BLOB PRIMARY KEY,
    model TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- L2 Cache: Virtual Table for Vector Similarity (e.g. 1536 dims)
CREATE VIRTUAL TABLE IF NOT EXISTS fuzzy_cache USING vec0(
    embedding float[1536]
);

-- Fuzzy Cache Payload Metadata
CREATE TABLE IF NOT EXISTS fuzzy_payloads (
    rowid INTEGER PRIMARY KEY,
    model TEXT NOT NULL,
    prompt_text TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```
