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
- [ADR 005: Strict Rejection of Server-Sent Events (SSE) Streaming in MVP (Superseded)](#adr-005-strict-rejection-of-server-sent-events-sse-streaming-in-mvp)
- [ADR 006: Asynchronous Off-Critical-Path Persistence to SQLite](#adr-006-asynchronous-off-critical-path-persistence-to-sqlite)
- [ADR 007: Mandatory Resource Attribution for CLI Tools](#adr-007-mandatory-resource-attribution-for-cli-tools)
- [ADR 008: Syntax-Preserving Non-Destructive Canonicalization](#adr-008-syntax-preserving-non-destructive-canonicalization)
- [ADR 009: Removal-First Atomic Broadcast in Single-Flight Coalescer](#adr-009-removal-first-atomic-broadcast-in-single-flight-coalescer)
- [ADR 010: Transparent Non-Blocking Streaming Bypass with Dual-Stage Timeout](#adr-010-transparent-non-blocking-streaming-bypass)
- [ADR 011: Bounded Concurrency Semaphore for SQLite Disk Writers](#adr-011-bounded-concurrency-semaphore-for-sqlite-disk-writers)
- [ADR 012: Provider-Aware Default Hyperparameter Normalization](#adr-012-provider-aware-default-hyperparameter-normalization)
- [ADR 013: State-Gated In-Flight RAII Guard Lifecycle](#adr-013-state-gated-in-flight-raii-guard-lifecycle)
- [ADR 014: SemCache v1.1 Architectural Blueprint](#adr-014-semcache-v11-architectural-blueprint)
- [ADR 015: In-Code Recursive AST Key Sorting for Feature Unification Immunity](#adr-015-in-code-recursive-ast-key-sorting-for-feature-unification-immunity)
- [ADR 016: Multi-Header Credential Salting and Unauthenticated Namespace Isolation (Superseded)](#adr-016-multi-header-credential-salting-first-match-precedence-and-authoritative-provider-isolation)
- [ADR 017: Fail-Open SQLite Storage Degradation & Write Circuit Breaker](#adr-017-fail-open-sqlite-storage-degradation--write-circuit-breaker)
- [ADR 018: Gateway Memory, Payload Size, and Concurrency Bounds](#adr-018-gateway-memory-payload-size-and-concurrency-bounds)
- [ADR 019: Multi-Header Credential Salting & Length-Prefixed Tenant Isolation](#adr-019-multi-header-credential-salting--length-prefixed-tenant-isolation)
- [ADR 020: Circuit Breaker Real-Write Errors & Automatic Success Recovery](#adr-020-circuit-breaker-real-write-errors--automatic-success-recovery)
- [ADR 021: Conservative Replay Policy (`SEMCACHE_CONSERVATIVE_REPLAY`)](#adr-021-conservative-replay-policy-semcache_conservative_replay)
- [ADR 022: Dedicated-Connection Off-Path Storage Bounding & Non-Blocking Vacuum](#adr-022-dedicated-connection-off-path-storage-bounding--non-blocking-vacuum)
- [ADR 023: Prometheus Metrics Exposition & Loopback-Only Security Gating](#adr-023-prometheus-metrics-exposition--loopback-only-security-gating)
- [ADR 024: Stream Concurrency Permit Retention & Mid-Stream Disconnect Cleanup](#adr-024-stream-concurrency-permit-retention--mid-stream-disconnect-cleanup)
- [ADR 025: Process-Local File and Directory Permissions Security](#adr-025-process-local-file-and-directory-permissions-security)
- [ADR 026: Upstream Response `Cache-Control: no-store` Compliance](#adr-026-upstream-response-cache-control-no-store-compliance)

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
**Superseded by ADR 010** (Replaced with Transparent Non-Blocking Streaming Bypass)

> [!NOTE]
> **Superseded by ADR 010:** Streaming requests (`stream: true`) are no longer rejected with HTTP 400. In accordance with ADR 010, SemCache transparently proxies SSE streams using a non-blocking dual-stage watchdog (180s TTFB for reasoning models + 30s inter-chunk timeout) with header `x-semcache-status: BYPASS_STREAM`.

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

---

## ADR 007: Multi-Tenant Cache Isolation via Authorization Header Hashing

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Generating cache keys purely from JSON request bodies creates an unintentional authorization bypass: an unauthenticated user or User B can submit prompt $P$ and retrieve a cache hit previously populated by User A, effectively stealing API access.

### Decision
We incorporate `auth_header: Option<&str>` directly into the BLAKE3 digest calculation (`|auth_tenant:<token>`).

### Consequences
**Positive:**
- 100% multi-tenant isolation. Requests authenticated with different API keys generate completely disjoint cache spaces.
- Zero risk of cross-account data leakage or billing circumvention.

**Negative:**
- Identical prompts sent across different API keys will not share cache hits (intentional security design).

---

## ADR 008: Syntax-Preserving Non-Destructive Canonicalization

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Aggressively stripping newlines and trimming internal whitespace corrupts code generation prompts (e.g. Python scripts where whitespace defines scope, YAML configurations, Markdown tables).

### Decision
We preserve all internal whitespace, newlines, and indentation intact. Only outermost string boundaries are trimmed, and generative hyperparameters (`temperature`, `top_p`, `seed`) are retained in the cache key. Only non-generative metadata (`user`) is stripped.

### Consequences
**Positive:**
- Zero syntax corruption for code synthesis, YAML, and Markdown prompts.
- Prompts with different generation dynamics (e.g. `temperature: 0.0` vs `1.0`) do not collide in cache.

---

## ADR 009: Removal-First Atomic Broadcast in Single-Flight Coalescer

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Calling `tx.send(data)` *before* `in_flight.remove(&hash)` creates a critical race window where late-arriving requests subscribe after `send` completed, causing infinite follower deadlocks. Furthermore, leader aborts caused followers to receive unhelpful channel closed errors.

### Decision
1. The leader removes the hash from `DashMap` **first**, and then broadcasts the payload.
2. The channel transmits `Result<Bytes, Arc<SemCacheError>>` so upstream errors (e.g. 429 rate limits) are actively propagated to followers without deadlocking.

### Consequences
**Positive:**
- Closes the send-then-remove race window completely.
- Followers receive immediate, accurate upstream error propagation rather than generic 500s or hangs.

---

## ADR 010: Transparent Non-Blocking Streaming Bypass

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Rejecting `stream: true` with HTTP 400 breaks compatibility with standard agent frameworks (LangChain, LlamaIndex, Cursor) that default to streaming completions.

### Decision
When `stream: true` is detected, SemCache bypasses cache lookup and single-flight coalescing, forwards the request directly upstream, and streams raw Server-Sent Event (SSE) chunks back to the client with `x-semcache-status: BYPASS_STREAM`.

### Consequences
**Positive:**
- 100% backward and forward compatibility with all streaming LLM client SDKs.
- Zero client breakage while preserving unary caching guarantees.

---

## ADR 011: Bounded Concurrency Semaphore for SQLite Disk Writers

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Wrapping `tokio::task::spawn_blocking` in `tokio::time::timeout` does not abort or cancel the underlying OS thread when the timeout triggers. Under SQLite disk lock contention, hundreds of abandoned blocking tasks accumulate, exhausting Tokio's 512-thread blocking pool and starving the server.

### Decision
1. Bound concurrent disk write tasks via an `Arc<tokio::sync::Semaphore>` in `AppState` (default: 4 concurrent writes, matching SQLite's single-writer architecture).
2. Configure `PRAGMA busy_timeout = 5000;` on all SQLite connections so OS threads back off after 5 seconds instead of blocking indefinitely.
3. If the semaphore cannot be acquired within 250ms, safely drop the disk write under backpressure and immediately evict the in-flight memory entry.

### Consequences
**Positive:**
- Tokio's blocking threadpool cannot be exhausted even under catastrophic disk I/O stalls.
- Excess cache writes are dropped gracefully without failing client HTTP responses.

---

## ADR 012: Provider-Aware Default Hyperparameter Normalization

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Hardcoding `temperature: 1.0` as the default is OpenAI-specific. Ollama defaults to `temperature: 0.8` and `top_p: 0.9`. If SemCache were pointed at an Ollama instance, an explicit `temperature: 1.0` is non-default, but would previously be stripped, causing distinct requests to collide into the same bucket.

### Decision
1. Implement a `Provider` enum (`OpenAi`, `Ollama`, `Generic`).
2. Provide explicit configuration via `SEMCACHE_DEFAULT_PROVIDER` and per-request override via `x-semcache-provider` header.
3. When provider is `Ollama`, normalize `temperature: 0.8` and `top_p: 0.9` (retaining `1.0` verbatim). When provider is `Generic`, preserve all parameters verbatim.

### Consequences
**Positive:**
- Eliminates the Ollama Trap and cross-provider cache divergence.
- Explicit operator control over default parameter normalization.

---

## ADR 013: State-Gated In-Flight RAII Guard Lifecycle

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
If `LeaderGuard::drop` unconditionally removes the entry from `InFlightMap` and broadcasts an error, dropping the guard after `evict()` has already executed could corrupt subsequent cycles or send spurious error notifications.

### Decision
Introduce a strict lifecycle enum `GuardState` (`Pending`, `Ready`, `Evicted`):
- `mark_ready_and_broadcast` transitions to `GuardState::Ready`.
- `evict` transitions to `GuardState::Evicted`.
- `Drop for LeaderGuard` only purges the map and broadcasts an error if `self.state == GuardState::Pending`.

### Consequences
**Positive:**
- RAII drop safety guarantees zero map corruption or erroneous follower abortion after successful completion.

---

## ADR 014: SemCache v1.1 Architectural Blueprint

### Status
**Proposed & Roadmapped for v1.1**

### Context & Threat Model
The v1.0 release established production-grade guarantees for single-flight coalescing, multi-tenant salting, and backpressure. However, system audit exposed three critical second-order failure modes to be addressed in the v1.1 milestone:

1. **The `serde_json` Cargo Feature Unification Vulnerability:**
   Cargo automatically unifies crate features across the entire dependency graph. If any transitive dependency in the workspace activates `serde_json/preserve_order` (e.g. a CLI parser or OpenTelemetry exporter), `serde_json::Map` silently switches from `BTreeMap` to `IndexMap`. BLAKE3 hashes change silently without compiler warnings, destroying cache hit rates.
2. **True Disk-Commit Synchronization Verification:**
   Ensuring that `CoalesceState::Ready` in RAM persists strictly until the background SQLite WAL transaction commits on NVMe/disk. If eviction runs before disk commit finishes, followers arriving in that window trigger duplicate upstream calls.
3. **Continuous 30-Minute Sustained Production Soak Suite:**
   Testing beyond burst benchmarks (which execute in hundreds of milliseconds) to sustained multi-gigabyte continuous agent loops that measure long-horizon memory stabilization, WAL checkpoint behavior under pressure, and actual disk write wear.

### Decision & Technical Blueprint for v1.1

#### 1. In-Code Recursive Canonical JSON Key Sorter
Instead of relying on `serde_json` crate configuration, implement an explicit in-code recursive key sorter that traverses the AST and sorts all object keys into a lexicographically ordered representation:
```rust
pub fn canonicalize_json_strictly(val: &Value) -> Vec<u8> {
    match val {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            // Deterministically serialize sorted pairs
        }
        Value::Array(arr) => { /* recursively format array items */ }
        _ => { /* format primitives */ }
    }
}
```
This guarantees mathematical key order determinism regardless of any Cargo workspace feature unification.

#### 2. Synchronized WAL Commit-to-Evict Handoff
Enforce that `guard.evict()` is invoked strictly within the completion callback of `write_handle.await`, guaranteeing zero time-gap between the memory `Ready` state and the SQLite L1 WAL state.

#### 3. 30-Minute Sustained Continuous Soak Suite
Construct a dedicated soak test bin (`cargo test --test long_soak -- --ignored`) that:
- Runs for 30 minutes continuously.
- Simulates realistic 8k to 32k context-window payloads.
- Periodically triggers SQLite WAL manual checkpoints (`PRAGMA wal_checkpoint(TRUNCATE)`).
- Validates flat resident memory (RSS $\Delta \approx 0$).

---

## ADR 015: In-Code Recursive AST Key Sorting for Feature Unification Immunity

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
In Cargo workspaces, features are unified across all crates. Setting `default-features = false` on `serde_json` does not prevent another dependency from enabling `preserve_order`. If enabled, `serde_json::Map` shifts from `BTreeMap` to `IndexMap`, changing key serialization order and silently destroying BLAKE3 cache hit rates.

### Decision
Implement `serialize_canonical_strict` in `src/canonical.rs` to recursively traverse the AST and sort all object keys lexicographically in code before computing the BLAKE3 digest.

### Consequences
**Positive:**
- Mathematical key-order determinism is guaranteed regardless of Cargo build graph features.
- Eliminates silent cache divergence.

---

## ADR 016: Multi-Header Credential Salting, First-Match Precedence, and Authoritative Provider Isolation

### Status
**Superseded** by [ADR 019: Multi-Header Credential Salting & Length-Prefixed Tenant Isolation](#adr-019-multi-header-credential-salting--length-prefixed-tenant-isolation)

### Context
AI client libraries use varying authentication headers: `Authorization` (OpenAI), `api-key` (Azure OpenAI), or `x-api-key` (Anthropic/LiteLLM). Furthermore, local LLMs (Ollama, vLLM) often run without authentication. If multi-header precedence is undefined, clients sending both `Authorization` and `api-key` (e.g. Azure OpenAI SDKs) could either cause cache collisions or generate silent cache misses across deployments. Additionally, allowing clients to override provider identity via `x-semcache-provider` introduces a Denial-of-Service vector where hostile clients arbitrarily partition the cache.

### Decision
1. **First-Match Credential Precedence:** The credential salt strictly evaluates headers in the following order:
   $$\text{Authorization} \succ \text{api-key} \succ \text{x-api-key}$$
   The first non-empty header encountered is used as the cryptographic salt. If a client transmits both `Authorization` and `api-key`, the cache key is salted exclusively with `Authorization`. This guarantees that Azure deployments do not partition into separate buckets when the SDK includes redundant headers.
2. **Server-Side Provider Authority:** The client header `x-semcache-provider` is **strictly ignored and removed**. The gateway's `SEMCACHE_DEFAULT_PROVIDER` configuration is authoritative. Clients cannot manipulate the provider cache partition.
3. **Unauthenticated Isolation:** For unauthenticated requests (e.g. local Ollama or vLLM), requests namespace under `SEMCACHE_TENANT_ID` (default `default_tenant`).
4. **Zero Credential Leakage:** All credential headers are sanitized from logged and stored request JSON in SQLite.

### Consequences
**Positive:**
- Multi-cloud compatibility across OpenAI, Azure, and Anthropic SDKs with zero cache fragmentation.
- Prevention of client-driven cache-partitioning DoS attacks.
- Strict isolation across distinct API keys even when prompts and hyper-parameters are identical.
- Zero credential leakage into database storage or telemetry.

---

## ADR 017: Fail-Open SQLite Storage Degradation & Write Circuit Breaker

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
SemCache is an accelerating gateway, not an authoritative datastore. If SQLite encounters disk lock timeouts, table corruption, or I/O failure, failing the client's HTTP request destroys upstream reliability. However, retaining failed writes in RAM `Ready` cache for 10 seconds under continuous disk failure (e.g. disk full, permission denied, WAL corruption) causes RAM usage to grow proportionally to $\text{rate} \times \text{retention}$, risking gateway OOM.

### Decision
1. **L1 Read Fail-Open:** If SQLite L1 read fails, log a warning and degrade gracefully to a cache miss (`None`), continuing upstream without failing the client.
2. **L1 Write Fail-Open:** If SQLite write fails or times out, log a warning and return the HTTP 200 response to the client.
3. **Consecutive Write Failure Circuit Breaker:**
   - Track consecutive write/backpressure failures via an atomic counter.
   - If consecutive failures are $< 5$, retain the response in RAM `Ready` cache for 10s to serve immediate coalesced retries.
   - If consecutive failures reach the threshold ($N \ge 5$), trip the circuit breaker: **disable `Ready` RAM retention immediately** and evict the entry upon dispatch. The gateway reverts to pure fail-open, preventing memory accumulation during persistent disk outages.
   - When any SQLite write succeeds, the failure counter resets to 0.
4. **Dropped Writes Observability:** Track all dropped and failed writes via `semcache_dropped_writes_total`.

### Consequences
**Positive:**
- Upstream proxy availability is preserved even under catastrophic database failure.
- RAM exhaustion is mathematically prevented during persistent disk outages via the $N=5$ circuit breaker.
- Full observability of disk write saturation.

---

## ADR 018: Gateway Memory, Payload Size, and Concurrency Bounds

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Unbounded request bodies, multi-hundred-megabyte responses, and unconstrained upstream dispatches expose the gateway to Out-Of-Memory (OOM) crashes and threadpool starvation. Multimodal vision models also require accommodating multi-megabyte inline base64 images without triggering HTTP 413.

### Decision
1. **Request Body Cap (32MB):** Default `SEMCACHE_MAX_REQUEST_BYTES` to 32MB (`32 * 1024 * 1024`) to natively support multimodal vision models (GPT-4o, Claude 3.5 Sonnet, Gemini 1.5 Pro) with inline base64 image attachments. Oversized requests return HTTP 413 Payload Too Large.
2. **Streaming Response Size Enforcement:** Inspect `Content-Length` and accumulate chunks while streaming upstream bytes. If a response exceeds `SEMCACHE_MAX_RESPONSE_BYTES` (default 10MB), immediately terminate buffering for cache storage. The leader streams to its client, and waiting followers receive an `UpstreamOversizedBypass` directive to fetch directly from upstream with HTTP 200 and header `x-semcache-status: BYPASS_OVERSIZED`.
3. **Bounded Ready RAM Retention:** Enforce `SEMCACHE_MAX_READY_BYTES` (default 128MB). If adding an entry to `InFlightMap` exceeds the budget, the entry is broadcast to active subscribers and immediately evicted from RAM.
4. **Upstream Semaphore Exhaustion Contract:** Gate upstream dispatch behind `SEMCACHE_MAX_UPSTREAM_CONCURRENCY` (default 256). When all permits are saturated:
   - Wait up to 5 seconds.
   - If timeout expires, return **HTTP 503 Service Unavailable** with **`Retry-After: 5`** header to both the primary worker and all waiting followers.
   - Increment `semcache_upstream_shed_total`.
   - L1 cache hits are checked before semaphore acquisition, guaranteeing that cache hits never block on or consume upstream concurrency permits.
5. **Standard `Cache-Control: no-store` Bypass:** When a request presents `Cache-Control: no-store` or `x-semcache-no-store: true`, bypass L1 cache lookup and persistence, streaming directly from upstream with header `x-semcache-status: BYPASS_NO_STORE`.
6. **Data at Rest Protection:** Automatically enforce Unix file permissions `0600` on the SQLite database file on startup.
7. **Secure Loopback Default:** Default server binding to `127.0.0.1:3000`. For containerized deployments, document mandatory `SEMCACHE_BIND=0.0.0.0:3000` with instructions to front the gateway with a secure reverse proxy (Nginx, Envoy, Cloudflare).

### Consequences
**Positive:**
- Comprehensive protection against memory exhaustion (streaming response check + 128MB Ready cap).
- Concurrency exhaustion fails fast with standard HTTP 503 + `Retry-After: 5` semantics.
- Seamless support for heavy multimodal vision prompts.
- Full support for client-controlled bypass (`Cache-Control: no-store`).

---

## ADR 019: Multi-Header Credential Salting & Length-Prefixed Tenant Isolation

### Status
**Accepted** (Supersedes ADR 016)

### Context
SemCache isolates cache partitions by hashing authentication credentials into the cache key. Plain byte concatenation of multiple headers (e.g. `Authorization: "ab"` + `api-key: "c"` vs `Authorization: "a"` + `api-key: "bc"`) can result in identical byte sequences, creating a cross-tenant collision vulnerability.

### Decision
1. **Length-Prefixed Hashing:** Each credential header name and value is length-prefixed with a 32-bit little-endian integer before absorption into the BLAKE3 hasher:
   $$\text{Hasher} \leftarrow \text{len}(K)_{\text{u32 LE}} \,\|\, K \,\|\, \text{len}(V)_{\text{u32 LE}} \,\|\, V$$
2. **Canonical Header Sorting:** All matched credential headers (`authorization`, `api-key`, `x-api-key`) are extracted, converted to lowercase, sorted lexicographically by name, and encoded with length prefixes.
3. **Unauthenticated Isolation:** If no credential headers exist, the length-prefixed `SEMCACHE_TENANT_ID` (default: `default_tenant`) is absorbed as the salt.

### Consequences
**Positive:**
- Cryptographic immunity to delimiter-injection and boundary-shifting collision attacks.
- Strict multi-tenant isolation across heterogeneous API key configurations.

---

## ADR 020: Circuit Breaker Real-Write Errors & Automatic Success Recovery

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Disk write failures (e.g., read-only filesystem, I/O corruption, out of disk space) must trigger fail-open behavior to protect gateway memory. However, transient write queue backpressure is a temporary capacity constraint, not persistent storage corruption. Furthermore, once disk storage recovers, the gateway should self-heal without manual restarts.

### Decision
1. **Real-Write Error Accounting:** Only genuine SQLite write errors increment the consecutive failure counter. Capacity-based write drops increment `semcache_dropped_writes_total` but do not trip the breaker.
2. **Circuit Trip Threshold:** If consecutive real write errors reach 5, trip the breaker: evict completed items immediately from RAM `Ready` cache rather than retaining them for 10 seconds.
3. **Automatic Reset:** Any successful SQLite write resets the consecutive failure counter to 0 and restores normal `Ready` cache retention.

### Consequences
**Positive:**
- Prevents spurious circuit trips under high-concurrency burst traffic.
- Automatic self-healing when underlying storage issues are resolved.

---

## ADR 021: Conservative Replay Policy (`SEMCACHE_CONSERVATIVE_REPLAY`)

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
Earlier designs referred to deterministic-only caching. In production LLMs, strict determinism cannot be guaranteed due to non-associative GPU floating-point operations, dynamic batching, and MoE routing. Furthermore, OpenAI documentation explicitly states that `seed` is best-effort. If a proxy caches only `temperature: 0` traffic by default, typical development and evaluation workloads achieve near-zero cache hit rates.

### Decision
1. **Renamed Configuration:** Rename the flag to `SEMCACHE_CONSERVATIVE_REPLAY` (with backward-compatible aliases `SEMCACHE_STABLE_ONLY` and `SEMCACHE_DETERMINISTIC_ONLY`).
2. **Default Policy:** Default `SEMCACHE_CONSERVATIVE_REPLAY=false`. In this mode, standard requests are cached and replayed based on canonical payload hashes, maximizing cache efficiency for AI agents, CI runs, and developer iterations.
3. **Conservative Mode:** When set to `true`, the proxy only persists and replays responses if the payload specifies `temperature == 0.0` or a valid `seed` with `temperature <= 0.0`. All other requests bypass cache storage with `x-semcache-status: BYPASS_STABLE_ONLY`.

### Consequences
**Positive:**
- Honest, technically defensible naming and documentation.
- High hit rate by default for developer and CI workflows.
- Opt-in strict conservative mode for users who require zero stochastic variance.

---

## ADR 022: Dedicated-Connection Off-Path Storage Bounding & Non-Blocking Vacuum

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
SQLite's `VACUUM` command rebuilds the database file into a temporary file and acquires an exclusive write lock. If `PRAGMA temp_store = MEMORY;` is active, the temporary database is stored in RAM, causing memory spikes and possible OOM on multi-gigabyte databases. Furthermore, executing VACUUM or size-based pruning inside request write transactions introduces multi-second latency jitter for client queries.

### Decision
1. **Disk-Backed Temp Storage:** Configure `PRAGMA temp_store = FILE;` on all database connections.
2. **Off-Path Background Maintenance:** All pruning (TTL expiration, soft-cap LRU eviction) and database maintenance run on an independent background thread with a dedicated SQLite connection, completely off the request hot path.
3. **Sensible Laptop Defaults:** Reduce default `SEMCACHE_MAX_DB_BYTES` from 10 GB to 2 GB (`2147483648` bytes).
4. **Maintenance Interval:** Pruning runs once per hour. VACUUM is only triggered if soft-cap pruning removes $> 25\%$ of database entries.

### Consequences
**Positive:**
- Zero request latency spikes or writer lock contention from database maintenance.
- Memory safety: vacuum operations use disk temporary files instead of process RAM.
- Predictable 2 GB storage footprint on developer machines.

---

## ADR 023: Prometheus Metrics Exposition & Loopback-Only Security Gating

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
The `/metrics` endpoint exposes detailed gateway telemetry, including cache hit counts, token savings, and error rates. In containerized environments (Docker/Kubernetes), binding to `0.0.0.0` exposes this endpoint to the network. Exposing cache hit patterns allows external observers to infer client prompt frequency and cache behavior.

### Decision
1. **Feature Gate:** Add `SEMCACHE_ENABLE_METRICS` environment variable.
2. **Security Default:** If the server is bound to an open network address (`0.0.0.0`), `SEMCACHE_ENABLE_METRICS` defaults to `false`. When bound to `127.0.0.1`, it defaults to `true`.
3. **404 When Disabled:** When disabled, requests to `/metrics` return HTTP 404 Not Found.

### Consequences
**Positive:**
- Prevents accidental telemetry leakage in public or container networks.
- Retains effortless local developer observability.

---

## ADR 024: Stream Concurrency Permit Retention & Mid-Stream Disconnect Cleanup

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
When streaming responses (`stream: true`), an upstream connection permit is held for the duration of the stream. If a client disconnects mid-stream or the upstream provider stalls indefinitely, orphan tasks ("ghost tasks") could hold concurrency permits, leading to permit exhaustion.

### Decision
1. **RAII Permit Lifetime:** The upstream concurrency permit is transferred to an RAII guard held by the streaming response body task. The permit is automatically released when the stream finishes or drops.
2. **Mid-Stream Watchdog:** If client TCP disconnects or if no chunk arrives within the chunk timeout (30s) or TTFB timeout (default 180s for reasoning models, 30s for standard), the streaming task aborts and drops the upstream request immediately.

### Consequences
**Positive:**
- Zero permit leakage on client disconnects.
- Active cancellation of ghost upstream requests saves API billing.

---

## ADR 025: Process-Local File and Directory Permissions Security

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
`libc::umask(0o077)` alters the global umask of the entire OS process. This introduces race conditions when running concurrent tests or when embedding SemCache as a library. SQLite creates its `-wal` and `-shm` files using the filesystem permissions of the parent database file.

### Decision
1. **Drop Global umask:** Remove all calls to `libc::umask`.
2. **Process-Local Permissions:** On startup, SemCache verifies and creates the database file with `0600` (`-rw-------`) mode using `std::os::unix::fs::OpenOptionsExt::mode(0o600)` and `std::fs::set_permissions`.
3. **Directory Permissions:** The containing parent directory is created with `0700` (`drwx------`) mode.

### Consequences
**Positive:**
- Eliminates process-wide side effects and test concurrency races.
- Database, WAL, and SHM files remain strictly accessible only to the executing user.

---

## ADR 026: Upstream Response `Cache-Control: no-store` Compliance

### Status
**Accepted** (Implemented in Hardening Phase)

### Context
RFC 9111 specifies that HTTP caches must not store any part of a response if the response contains `Cache-Control: no-store`. Upstream LLM providers or intermediary gateways may mark certain sensitive or transient completions with this directive.

### Decision
Inspect the HTTP response headers returned by the upstream provider. If `Cache-Control` contains `no-store` (case-insensitive):
1. Immediately bypass SQLite cache persistence.
2. Deliver the response to the client with `x-semcache-status: BYPASS_NO_STORE`.

### Consequences
**Positive:**
- Full compliance with RFC 9111 response caching semantics.
- Guarantees upstream privacy directives are strictly respected.



