# Architecture Decision Records (ADR) Log: SemCache

**Project:** SemCache (Vector-Similarity Cache Gateway)  
**Status:** Active & Enforced  
**Format:** Michael Nygard Architectural Decision Record Standard  

---

## Index of Decisions
- [ADR 001: Rust over Go/Node.js for the Proxy Hot Path](#adr-001-rust-over-gonodejs-for-the-proxy-hot-path)
- [ADR 002: Embedded SQLite with WAL & sqlite-vec over External Vector Stores](#adr-002-embedded-sqlite-with-wal--sqlite-vec-over-external-vector-stores)
- [ADR 003: Single-Flight Request Coalescing via DashMap and Tokio Broadcast](#adr-003-single-flight-request-coalescing-via-dashmap-and-tokio-broadcast)
- [ADR 004: BLAKE3 as the L1 Cryptographic Key Derivation Function](#adr-004-blake3-as-the-l1-cryptographic-key-derivation-function)
- [ADR 005: Strict Rejection of Server-Sent Events (SSE) Streaming in MVP](#adr-005-strict-rejection-of-server-sent-events-sse-streaming-in-mvp)
- [ADR 006: Asynchronous Off-Critical-Path Persistence to SQLite](#adr-006-asynchronous-off-critical-path-persistence-to-sqlite)

---

## ADR 001: Rust over Go/Node.js for the Proxy Hot Path

### Status
**Accepted** (Implemented in Phase 1)

### Context
SemCache sits directly in the ingress latency path of AI agent loops and LLM API traffic. Autonomous multi-agent workflows issue thousands of concurrent requests where microsecond-level overhead directly impacts agent loop execution time.

### Decision
We implement SemCache in **Rust** (Edition 2021) utilizing `axum 0.7`, `tokio 1.36`, and `hyper 1.2`.

### Consequences
**Positive:**
- **Predictable Latency Profile:** Zero garbage collection pauses. P99 latency remains consistent under heavy heap allocation and buffer cycling.
- **Memory Safety & Zero Cost Abstractions:** Compile-time borrow checking ensures zero data races across concurrent Tokio tasks without runtime overhead.
- **Native C ABI Interoperability:** High-performance FFI bindings to SQLite (`rusqlite`) and native C extensions (`sqlite-vec`) without intermediate bridge layers or IPC penalties.

**Negative:**
- Steeper learning curve and stricter compile-time constraints compared to Go or TypeScript.
- Longer compilation times during release builds with full optimization flags.

### Alternatives Considered
- **Go (Golang):** Excellent networking primitives, but runtime garbage collection induces latency spikes during high-throughput JSON buffer allocations.
- **Node.js / Bun:** Rapid prototyping, but high memory overhead per connection and single-threaded event loop bottlenecks during intensive cryptographic hashing and JSON AST traversals.

---

## ADR 002: Embedded SQLite with WAL & sqlite-vec over External Vector Stores

### Status
**Accepted** (Implemented in Phase 1)

### Context
Semantic and exact caching requires persistent storage and vector similarity indexing. Traditional AI architectures deploy external vector stores (pgvector, Qdrant, Milvus, Pinecone). However, SemCache's primary design goal is **local-first zero-infrastructure deployment** on developer laptops, edge devices, and CI runners.

### Decision
We use embedded **SQLite** (via `rusqlite` bundled) loaded with the **`sqlite-vec`** extension, operating in Write-Ahead Logging (`WAL`) mode.

### Consequences
**Positive:**
- **Zero External Infrastructure:** Runs as a self-contained binary with a single `.db` file. No Docker daemon, external database setup, or network management required.
- **Sub-Millisecond Disk Access:** Direct in-process memory and NVMe access via SQLite WAL shared-memory files (`-shm`) eliminates loopback TCP network hops.
- **Deterministic Portability:** The cache database can be version-controlled, copied, or backed up via standard file operations.

**Negative:**
- SQLite write concurrency is serialized to a single writer process (mitigated by WAL mode for concurrent readers and connection pooling).
- Not suited for multi-node distributed cluster replication (which is explicitly out of scope for the local-first MVP).

### Alternatives Considered
- **PostgreSQL + pgvector:** Industry standard for vector search, but requires running a heavy external server process, configuration of connection limits, and network latency.
- **Qdrant / ChromaDB Standalone:** Adds operational complexity, external dependencies, and significant RAM footprints ($> 500\text{MB}$ idle) incompatible with lightweight local agent loops.

---

## ADR 003: Single-Flight Request Coalescing via DashMap and Tokio Broadcast

### Status
**Accepted** (Implemented in Phase 1)

### Context
Autonomous agents (e.g., AutoGPT, BabyAGI, SWE-bench evaluators) frequently enter cyclic retry loops or spawn concurrent sub-agents querying identical prompts simultaneously. When an L1 cache miss occurs, sending 20 concurrent duplicate requests to OpenAI causes:
1. Catastrophic upstream rate-limit exhaustion (HTTP 429).
2. Financial waste (paying for identical completions 20 times).
3. Local connection socket exhaustion.

### Decision
We implement a **Single-Flight Request Coalescer** (`src/coalesce.rs`) using a concurrent sharded hash map (`DashMap<[u8; 32], broadcast::Sender<Bytes>>`) and RAII `LeaderGuard` cleanup tokens.

### Consequences
**Positive:**
- **Rate-Limit Preservation:** $N$ simultaneous identical prompt requests collapse into exactly 1 upstream request.
- **Lock-Free Concurrency:** `DashMap` provides fine-grained shard-level locking, preventing thread contention across differing prompt hashes.
- **Crash Safety:** The `LeaderGuard` implements `Drop`. If the Primary Leader task drops due to an upstream network timeout or client cancellation, the hash key is immediately removed from the map, preventing subscriber tasks from hanging.

**Negative:**
- Broadcast channel buffers must be carefully dimensioned (set to 16) to prevent slow subscriber lag errors.

### Alternatives Considered
- **Mutexted Standard `HashMap`:** Induces global lock contention across all worker threads on the gateway hot path.
- **Redis Pub/Sub:** Introduces an external network dependency, defeating the local-first embedded architecture.

---

## ADR 004: BLAKE3 as the L1 Cryptographic Key Derivation Function

### Status
**Accepted** (Implemented in Phase 1)

### Context
Every incoming JSON payload must be deterministically transformed into a fixed-size cache key. The hashing algorithm must be collision-resistant and capable of hashing multi-megabyte prompt contexts (e.g., 128k token context windows) in microseconds.

### Decision
We use **BLAKE3** (`blake3 1.5`) as the primary hashing engine for L1 deterministic caching.

### Consequences
**Positive:**
- **Unrivaled Throughput:** BLAKE3 utilizes tree hashing and SIMD parallelism (AVX-512, AVX2, NEON), achieving up to 6–10x the speed of SHA-256 and MD5.
- **Fixed 32-Byte Keys:** Produces a compact, fixed 256-bit binary key stored as a SQLite `BLOB PRIMARY KEY`, optimizing B-tree search traversal.
- **Cryptographic Security:** Unlike non-cryptographic hashes (xxHash, Murmur3), BLAKE3 is cryptographically secure and immune to length-extension and collision-generation attacks.

**Negative:**
- Slightly higher instruction count than non-cryptographic hashes on tiny payloads ($< 64\text{ bytes}$), but completely amortized by payload canonicalization.

### Alternatives Considered
- **SHA-256:** Cryptographically robust, but substantially slower on multi-kilobyte context windows.
- **xxHash64:** Faster on tiny strings, but non-cryptographic nature opens susceptibility to deliberate cache key collision injection attacks by hostile client payloads.

---

## ADR 005: Strict Rejection of Server-Sent Events (SSE) Streaming in MVP

### Status
**Accepted** (Implemented in Phase 1)

### Context
OpenAI chat completions support `stream: true` using Server-Sent Events (SSE). Handling streaming responses in an exact/semantic cache gateway requires:
1. Buffering and re-assembling token chunks into a coherent JSON document for persistence.
2. Synthesizing artificial chunked SSE playback on cache hits to simulate token-by-token rendering.

### Decision
For the initial MVP (v0.1.0), requests with `stream: true` are **explicitly rejected** with HTTP 400 Bad Request (`SemCacheError::StreamingNotSupported`).

### Consequences
**Positive:**
- Enforces strict unary JSON determinism for L1 exact match and L2 vector similarity.
- Eliminates memory fragmentation and buffer leaks associated with broken client streaming connections.
- Accelerates the release of the core production caching and coalescing engine.

**Negative:**
- Clients requesting real-time typing indicators in consumer chat UIs must disable streaming to leverage SemCache in this phase.

### Alternatives Considered
- **Transparent Stream Pass-Through (No Cache):** Forwarding streaming requests without caching would confuse users about cache hit rates and provide zero cost reduction.
- **Immediate Full Streaming Interception:** Deferred to Phase 3 to allow thorough implementation of an SSE chunk aggregation state machine.

---

## ADR 006: Asynchronous Off-Critical-Path Persistence to SQLite

### Status
**Accepted** (Implemented in Phase 1)

### Context
Writing cached request-response pairs to SQLite involves disk I/O. Even in WAL mode, disk writes can introduce millisecond-level jitter if executed synchronously on the client response path.

### Decision
We execute all SQLite cache writes asynchronously in a detached `tokio::task::spawn_blocking` closure **after** the response bytes have been dispatched to the client.

### Consequences
**Positive:**
- **Zero Latency Impact:** The client receives the response immediately upon upstream completion. Upstream latency is not augmented by disk write cycles.
- **Runtime Isolation:** Slow disk flushes or SQLite locks cannot stall the Tokio asynchronous event loop.

**Negative:**
- If the gateway process is violently killed (`SIGKILL`) within microseconds of completing an upstream request, the response may not be written to disk before termination. This is an acceptable trade-off for a cache.

### Alternatives Considered
- **Synchronous Inline DB Writes:** Guarantees 100% immediate cache consistency at the cost of adding 2–5ms of disk latency to every upstream miss.
