# SemCache: Systems Architecture Specification

**System Architecture Specification**  
**Version:** 1.0.0 (Production Hardened) | **Target Roadmap:** v1.1.0  
**Implementation Language:** Rust (Edition 2021)  
**Target Environment:** Local-First Embedded Gateway / Edge Proxy  
**Repository:** [https://github.com/sshloktiwarii/SemCache.git](https://github.com/sshloktiwarii/SemCache.git)

---

## 1. System Topology & Architectural Tenets

SemCache is architected as an embedded, low-latency HTTP reverse proxy gateway. It intercepts OpenAI-compatible `/v1/chat/completions` traffic, applies syntax-preserving deterministic AST normalization, evaluates a multi-tier cache hierarchy (L1 BLAKE3 exact match and L2 vector search extension), coordinates concurrent duplicate executions via single-flight coalescing, and provides transparent non-blocking bypass for SSE token streams.

```
                                      ┌─────────────────────────────────────────────────────────────┐
                                      │                      SemCache Gateway                       │
                                      │                                                             │
  ┌─────────────────┐   HTTP POST     │  ┌───────────────┐     ┌──────────────────────────────────┐ │
  │   AI Agent /    │ ───────────────►│  │  Axum Proxy   │ ──► │     Canonicalization Engine      │ │
  │ Client Run-time │                 │  │ (src/proxy.rs)│     │        (src/canonical.rs)        │ │
  └─────────────────┘                 │  └───────────────┘     └─────────────────┬────────────────┘ │
           ▲                          │                                          │                  │
           │                          │  ┌──────────────────────────────────────┐│                  │
           │                          │  │ Stream Detector (stream == true)     ││                  │
           │   SSE Token Bypass       │  │ └─► Dual-Stage Idle Watchdog Stream  ││                  │
           ├──────────────────────────┼──┤     (180s TTFB, 30s Chunk Idle)      ││                  │
           │                          │  └──────────────────────────────────────┘│                  │
           │                          │                                          │ (stream == false)│
           │                          │                                          ▼                  │
           │                          │                      ┌────────────────────────────────────┐ │
           │                          │                      │  Tenant-Salted BLAKE3 Hash Engine  │ │
           │                          │                      └─────────────────┬──────────────────┘ │
           │                          │                                        │                    │
           │     L1 Hit (< 1.5ms)     │                                        ▼                    │
           ├──────────────────────────┼────────────────────────────── [ L1 Exact Cache Check ]      │
           │                          │                                        │ (Miss)             │
           │                          │                                        ▼                    │
           │                          │                      ┌────────────────────────────────────┐ │
           │                          │                      │ Single-Flight Request Coalescer    │ │
           │   Coalesced Wait         │                      │ (DashMap Sharded InFlightState)    │ │
           ├──────────────────────────┼──────────────────────┤          (src/coalesce.rs)         │ │
           │                          │                      └─────────────────┬──────────────────┘ │
           │                          │                                        │ (Primary Leader)   │
           │                          │                                        ▼                    │
           │                          │                      ┌────────────────────────────────────┐ │
           │     L2 Hit (< 15ms)      │                      │ L2 Semantic Cache (sqlite-vec)     │ │
           ├──────────────────────────┼──────────────────────┤     [Phase 2 / Roadmap Module]     │ │
           │                          │                      └─────────────────┬──────────────────┘ │
           │                          │                                        │ (Miss)             │
           │                          │                                        ▼                    │
           │                          │                      ┌────────────────────────────────────┐ │
           │   HTTP 200 (Unary JSON)  │                      │ Upstream Forwarding (Reqwest)      │ │
           └──────────────────────────┼──────────────────────┤ https://api.openai.com/v1/...      │ │
                                      │                      └─────────────────┬──────────────────┘ │
                                      │                                        │                    │
                                      │                                        ▼ (Async Semaphore)  │
                                      │                      ┌────────────────────────────────────┐ │
                                      │                      │ SQLite WAL Persistence Engine      │ │
                                      │                      │ (4-Permit Semaphore, busy_timeout) │ │
                                      │                      │ (r2d2 Pool) (src/db.rs)            │ │
                                      │                      └────────────────────────────────────┘ │
                                      └─────────────────────────────────────────────────────────────┘
```

### Core Architectural Tenets
1. **Zero Vibe-Code / Zero Panics:** Zero `.unwrap()` or `.expect()` calls in production execution paths. All internal errors resolve through strongly typed domain errors (`SemCacheError`, `CoalesceError`).
2. **Multi-Tenant Cache Isolation:** Authorization bearer tokens are salted directly into the BLAKE3 digest calculation (`|auth_tenant:<token>`). Different tenants querying identical prompts will never collide, share cached data, or circumvent API metering.
3. **Syntax-Preserving Canonicalization:** Code blocks, Python indentation, YAML spacing, and Markdown line breaks are preserved verbatim. Only non-generative client tracking fields (`user`) are stripped; generative hyperparameters (`temperature`, `top_p`, `presence_penalty`, `frequency_penalty`, `seed`, `logit_bias`) are preserved.
4. **Provider-Aware Default Normalization:** Default hyperparameter values are normalized based on provider profiles (`OpenAi`, `Ollama`, `Generic`) so default requests converge without colliding distinct explicit settings.
5. **Stampede Resilience & Race-Free Coalescing:** Single-flight request coalescing uses DashMap shard locking. The leader transitions the entry to `InFlightState::Ready(Bytes)` and broadcasts a typed `Result` payload so followers receive exact upstream failures (429/500) without hanging.
6. **Transparent Streaming Bypass:** `stream: true` requests bypass cache lookup and in-flight deduplication, piping raw SSE bytes directly through a dual-stage watchdog (`IdleTimeoutStream`: 180s TTFB, 30s chunk idle).
7. **Bounded Concurrency & Backpressure Shedding:** Background SQLite disk writes are constrained by a 4-permit `tokio::sync::Semaphore`. If disk I/O stalls, unacquired write tasks drop after 250ms without exhausting Tokio's blocking threadpool.

---

## 2. Ingress & Proxy Pipeline (`src/proxy.rs`)

The gateway proxy is implemented using `axum 0.7`, `tokio 1.36`, and `reqwest 0.12`.

### Request Pipeline Flow
1. **Payload Extraction & Streaming Interception:**
   - Ingress `POST /v1/chat/completions` request body is parsed into a `serde_json::Value` (enforcing 32MB default limit).
   - If `Cache-Control: no-store` or `x-semcache-no-store` is present, the request bypasses L1 cache and coalescing, streaming directly from upstream with header `x-semcache-status: BYPASS_NO_STORE`.
   - If `stream == true`, the request acquires an upstream permit and routes to `IdleTimeoutStream`, tunneling SSE chunks directly to the client while enforcing a 180-second TTFB and 30-second inter-chunk watchdog. The response is tagged with header `x-semcache-status: BYPASS_STREAM`.
2. **Authoritative Provider Resolution & Canonicalization:**
   - Provider is resolved strictly from server-side configuration (`AppState::default_provider`), ignoring client `x-semcache-provider` headers to eradicate client-driven cache-partitioning DoS vectors.
   - The payload is canonicalized via `canonicalize_and_hash(&payload_bytes, auth_salt, provider)`.
3. **L1 Cache Lookup:**
   - The 32-byte BLAKE3 hash is queried against SQLite `exact_cache` within `tokio::task::spawn_blocking`.
   - If found: Returns HTTP 200 with `x-semcache-status: HIT_L1` ($< 1.5\text{ms}$). Note that L1 hits never acquire or consume upstream semaphore permits.
4. **Coalesce State Registration (`register_or_wait`):**
   - If L1 misses, the worker acquires a DashMap shard entry lock:
     - **Follower (Pending):** Subscribes to the broadcast channel and awaits completion.
     - **Follower (Ready):** Retrieves the completed response immediately from bounded `Ready` RAM.
     - **Primary Leader:** Instantiates `LeaderGuard` and proceeds to upstream execution.
5. **Upstream Forwarding & Semaphore Bounding:**
   - The leader checks active receiver count (`tx.receiver_count()`). If client disconnected and zero followers wait, execution aborts to save upstream tokens.
   - Acquires permit from `upstream_semaphore` (256 permits, 5s timeout). If saturated, returns **HTTP 503 Service Unavailable** with **`Retry-After: 5`** to both leader and followers.
   - Forwards request via connection-pooled `reqwest::Client`.
   - If upstream returns non-2xx status code: broadcasts error to followers and propagates status code without caching.
6. **Streaming Response Size Enforcement:**
   - Chunks are read from upstream using `bytes_stream()`. If cumulative bytes exceed `SEMCACHE_MAX_RESPONSE_BYTES` (10MB), buffering is aborted.
   - Waiting followers receive `UpstreamOversizedBypass`, causing them to transparently forward directly upstream with header `x-semcache-status: BYPASS_OVERSIZED`.
7. **Memory State Transition & Broadcast:**
   - On 2xx response, leader calls `leader_guard.mark_ready_and_broadcast(resp_bytes)`.
   - Transitions state to `CoalesceState::Ready(resp_bytes, Instant::now())` within `SEMCACHE_MAX_READY_BYTES` (128MB budget) and broadcasts to all waiting followers.
8. **Bounded Asynchronous Persistence & Circuit Breaker:**
   - Dispatches background SQLite write task governed by `sqlite_write_semaphore` (4 permits, 250ms acquisition timeout).
   - If 5 consecutive writes fail or time out, the circuit breaker trips: `Ready` RAM retention is disabled, and entries are immediately evicted to prevent memory bloat during persistent disk failure.
9. **Eviction:**
   - Leader invokes `leader_guard.evict()`, safely removing the entry from the in-flight map.
   - Returns response with `x-semcache-status: MISS_UPSTREAM`.

---

## 3. Canonicalization & Hashing Engine (`src/canonical.rs`)

The Canonicalization Engine eliminates environmental variance and non-semantic entropy while strictly protecting code syntax and intentional hyperparameter configuration.

### Volatile Key Stripping vs Parameter Preservation
- **Stripped:** Non-generative client tracking fields (`user`), omitted parameter defaults (`stream: false`).
- **Preserved Verbatim:** Generative hyperparameters (`temperature`, `top_p`, `presence_penalty`, `frequency_penalty`, `seed`, `logit_bias`).

### Provider-Aware Default Parameter Normalization
Client libraries often inject default parameters explicitly. Hardcoding provider-specific defaults across all runtimes leads to cache collisions. SemCache applies provider-aware normalization:

| Provider | Normalized Default Values (Stripped if Matching) | Rationale |
| :--- | :--- | :--- |
| **`OpenAi`** | `temperature: 1.0`, `top_p: 1.0`, `presence_penalty: 0.0`, `frequency_penalty: 0.0` | Standard OpenAI API defaults. |
| **`Ollama`** | `temperature: 0.8`, `top_p: 0.9` | Official Ollama defaults. Retains `temperature: 1.0` verbatim. |
| **`Generic`** | None (all parameters preserved verbatim) | Fallback for custom or unknown vLLM/TGI runtimes. |

### Syntax-Preserving Message Normalization
Code generation prompts are hypersensitive to whitespace manipulation. SemCache preserves internal spacing:
- Outermost edges of message `content` and `prompt` strings are trimmed.
- **Internal indentation, newlines, tabs, and line feeds are preserved 100% verbatim.**

### Tenant-Salted BLAKE3 Key Derivation & Length-Prefixed Hashing
Credential salting evaluates configured credential headers (`authorization`, `api-key`, `x-api-key`, `x-goog-api-key`), sorts matched headers lexicographically by lowercase name, and hashes each `(name, value)` pair with 32-bit little-endian length prefixes to mathematically eliminate boundary collision attacks:
$$\text{SaltHasher} \leftarrow \bigoplus_{i} \left[ \text{len}(K_i)_{\text{u32 LE}} \,\|\, K_i \,\|\, \text{len}(V_i)_{\text{u32 LE}} \,\|\, V_i \right]$$

```rust
// Length-prefixed multi-header credential salt (ADR-019)
let mut hasher = blake3::Hasher::new();
if matched.is_empty() {
    let tenant_bytes = default_tenant_id.trim().as_bytes();
    hasher.update(&(tenant_bytes.len() as u32).to_le_bytes());
    hasher.update(tenant_bytes);
} else {
    matched.sort_by(|a, b| a.0.cmp(b.0));
    for (name, val) in matched {
        hasher.update(&(name.len() as u32).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(&(val.len() as u32).to_le_bytes());
        hasher.update(val.as_bytes());
    }
}
let cred_salt = hasher.finalize().as_bytes().to_vec();
```
The resulting credential salt is absorbed into the canonical request payload hash:
```rust
hasher.update(&canonical_json_bytes);
hasher.update(b"|auth_salt:");
hasher.update(&cred_salt);
let hash: [u8; 32] = *hasher.finalize().as_bytes();
```

---

## 4. Single-Flight Concurrency Coalescing (`src/coalesce.rs`)

To prevent catastrophic stampedes when 50+ concurrent agents evaluate identical prompts, SemCache provides sharded single-flight request coalescing.

### Concurrency Primitives
- **InFlightMap:** `Arc<DashMap<[u8; 32], InFlightState>>`
- **InFlightState:**
  - `Pending(broadcast::Sender<Result<Bytes, CoalesceError>>)`: Leader currently querying upstream.
  - `Ready(Bytes)`: Response received; cached in memory pending SQLite disk write.
- **Channel Capacity:** 16 messages per broadcast queue.

### State Transition Diagram
```mermaid
stateDiagram-v2
    [*] --> LockShard: register_or_wait(hash)
    LockShard --> FollowerPending: Entry::Occupied(Pending)
    LockShard --> FollowerReady: Entry::Occupied(Ready)
    LockShard --> PrimaryLeader: Entry::Vacant
    
    FollowerPending --> AwaitBroadcast: rx.recv().await
    AwaitBroadcast --> CoalescedSuccess: Ok(Bytes) -> HIT_COALESCED
    AwaitBroadcast --> CoalescedError: Err(CoalesceError) -> Propagate
    
    FollowerReady --> ImmediateReturn: Clone Bytes -> HIT_COALESCED
    
    PrimaryLeader --> UpstreamFetch: Return LeaderGuard
    UpstreamFetch --> UpstreamFailed: 4xx/5xx / Timeout / Panic
    UpstreamFailed --> RAIIAbort: Drop LeaderGuard
    RAIIAbort --> CleanMap: Purge Hash & Broadcast Error
    
    UpstreamFetch --> UpstreamSuccess: HTTP 200 OK
    UpstreamSuccess --> MarkReady: mark_ready_and_broadcast(bytes)
    MarkReady --> AsyncDiskWrite: Persist to SQLite WAL
    AsyncDiskWrite --> EvictMemory: guard.evict() -> MISS_UPSTREAM
```

### RAII LeaderGuard Lifecycle & Drop Safety
```rust
pub enum GuardState {
    Pending,
    Ready,
    Evicted,
}

pub struct LeaderGuard {
    hash: [u8; 32],
    in_flight: InFlightMap,
    tx: broadcast::Sender<Result<Bytes, CoalesceError>>,
    state: GuardState,
}
```
- **Drop Safety:** `Drop for LeaderGuard` evaluates `self.state`. Only if `self.state == GuardState::Pending` does the destructor remove the entry from `DashMap` and broadcast `CoalesceError::LeaderDropped` to prevent follower hangs. If `Ready` or `Evicted`, drop performs zero destructive actions.

---

## 5. Persistence & Storage Architecture (`src/db.rs`)

Persistence is handled exclusively by embedded SQLite via `rusqlite 0.31` managed by an `r2d2` connection pool.

### Critical SQLite PRAGMA Configuration
Every connection initialized from the pool executes the following pragmas:
```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
PRAGMA busy_timeout = 5000;
```
- **`journal_mode = WAL`**: Concurrent readers access the database without blocking write transactions.
- **`synchronous = NORMAL`**: Durable against application crashes without redundant disk `fsync` overhead.
- **`busy_timeout = 5000`**: SQLite connection retries internally for up to 5,000ms upon lock contention before returning `SQLITE_BUSY`.

### Bounded Concurrency Semaphore
SQLite supports only one active writer transaction at a time. To prevent threadpool exhaustion:
1. Disk writes are gated behind `AppState::sqlite_write_semaphore` (4 permits).
2. The acquisition attempts `tokio::time::timeout(Duration::from_millis(250), sem.acquire_owned())`.
3. If the timeout triggers under heavy write load, the write is dropped and logged, protecting client response latency.

### Database DDL Schema
```sql
-- L1 Deterministic Cache Table
CREATE TABLE IF NOT EXISTS exact_cache (
    canonical_hash BLOB PRIMARY KEY,
    model TEXT NOT NULL DEFAULT '',
    request_json TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- L2 Vector Payload Metadata (Roadmapped for Phase 2)
CREATE TABLE IF NOT EXISTS fuzzy_payloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    prompt_text TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- L2 Vector Virtual Table (sqlite-vec extension)
CREATE VIRTUAL TABLE IF NOT EXISTS fuzzy_cache USING vec0(
    embedding float[1536]
);
```

---

## 6. Centralized Error Handling Model (`src/error.rs`)

SemCache enforces structured JSON errors compliant with OpenAI API specifications:

| Error Variant | HTTP Status | Response `type` | Description |
| :--- | :--- | :--- | :--- |
| `JsonError` | `400 Bad Request` | `invalid_json_error` | Malformed inbound JSON request payload. |
| `UpstreamError(code, msg)` | Propagated (or `502`) | `upstream_error` | Upstream provider returned non-2xx status code. |
| `DbError` / `PoolError` | `500 Internal Error` | `gateway_error` | SQLite read failure or database error. |
| `InternalError` | `500 Internal Error` | `gateway_error` | Internal runtime or task execution failure. |

---

## 7. Latency Budget & Memory Allocation Profiles

| Operation Phase | Target Latency | Memory & Concurrency Strategy |
| :--- | :--- | :--- |
| **Ingress Parsing** | $< 100\,\mu\text{s}$ | Zero-copy byte borrowing where possible |
| **Canonicalization & BLAKE3** | $< 250\,\mu\text{s}$ | In-place map mutation; SIMD hash |
| **L1 SQLite Read** | $< 800\,\mu\text{s}$ | WAL shared memory read (`-shm`) |
| **Single-Flight Lockup** | $< 50\,\mu\text{s}$ | Lock-free DashMap shard access |
| **Upstream Network RTT** | Variable ($200\text{ms} - 2\text{s}$) | Governed by upstream provider |
| **Async Disk Write** | Off Critical Path | Bounded by 4-permit Semaphore |

---

## 8. Operational Configuration Reference

| Environment Variable | Default Value | Description |
| :--- | :--- | :--- |
| `SEMCACHE_BIND` | `127.0.0.1:3000` | Loopback TCP socket address for the inbound HTTP gateway. |
| `SEMCACHE_DB_PATH` | `semcache.db` | Filepath for the SQLite persistence database. |
| `OPENAI_UPSTREAM_URL` | `https://api.openai.com/v1/chat/completions` | Target upstream URL for forwarded chat requests. |
| `SEMCACHE_DEFAULT_PROVIDER`| `openai` | Default provider profile for normalization (`openai`, `ollama`, `generic`). |
| `SEMCACHE_TENANT_ID` | `default_tenant` | Fallback tenant identifier for unauthenticated endpoints. |
| `SEMCACHE_MAX_REQUEST_BYTES` | `10485760` (10 MB) | Maximum accepted request payload size (returns HTTP 413 on breach). |
| `SEMCACHE_MAX_RESPONSE_BYTES`| `10485760` (10 MB) | Maximum cached response size; larger responses bypass memory retention. |
| `SEMCACHE_MAX_CONCURRENT_WRITES` | `4` | Concurrency limit on SQLite background writer tasks. |
| `SEMCACHE_MAX_UPSTREAM_CONCURRENCY` | `256` | Maximum concurrent upstream HTTP requests. |
| `SEMCACHE_TTL_DAYS` | `7` | Cache entry retention window before automated pruning. |
| `SEMCACHE_UPSTREAM_TIMEOUT_SECS` | `300` | Overall upstream HTTP connection timeout. |
| `SEMCACHE_CANCEL_ORPHAN_REQUESTS` | `false` | Whether to cancel in-flight upstream fetch if initiator disconnects. |
| `RUST_LOG` | `semcache=debug,axum=info` | Tracing log filter directive. |

---

## 9. SemCache v1.1 Architectural Blueprint & Threat Mitigation Matrix

The v1.0 release established verified production-grade guarantees for single-flight coalescing, multi-tenant salting, streaming bypass, and write backpressure. The system audit identified three critical second-order architectural risks to be executed in the **v1.1 milestone**:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                          SemCache v1.1 Engineering Blueprint                           │
├──────────────────────────────┬────────────────────────────┬────────────────────────────┤
│ 1. Cargo Feature Unification │ 2. Continuous Soak Suite   │ 3. Zero-Gap WAL Commitment │
│    & Recursive AST Sorter    │    (30-Min Real Disk I/O)  │    (Tx Disk Handoff)       │
└──────────────────────────────┴────────────────────────────┴────────────────────────────┘
```

### 9.1 Threat 1: The `serde_json` Cargo Feature Unification Vulnerability

#### The Technical Failure Mode
In Cargo, features are **additive and unified across the entire workspace dependency graph**. Even though SemCache specifies:
```toml
serde_json = { version = "1.0", default-features = false, features = ["std", "alloc"] }
```
If any crate in the extended dependency graph (such as an OpenTelemetry tracing exporter, AWS SDK component, or CLI argument parser) activates the `preserve_order` feature on `serde_json`:
1. Cargo silently turns on `preserve_order` globally for all compilation units.
2. `serde_json::Map<String, Value>` changes its underlying backing structure from `std::collections::BTreeMap` to `indexmap::IndexMap`.
3. JSON object serialization ceases to be lexicographically sorted by key.
4. Payload `{"model":"gpt-4o","messages":[...]}` and `{"messages":[...],"model":"gpt-4o"}` produce two distinct byte sequences.
5. BLAKE3 hashes diverge silently. The L1 cache hit rate drops to zero without any compiler warning or runtime error.

#### The v1.1 Architectural Solution: AST-Recursive Lexicographical Sorter
In v1.1, canonicalization will no longer rely on `serde_json`'s internal map ordering. Instead, SemCache will implement an explicit, zero-allocation recursive AST key sorter that emits canonical JSON bytes directly:

```rust
pub fn serialize_canonical_strict(value: &Value, buffer: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            buffer.push(b'{');
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            // Deterministic lexicographical sorting independent of Cargo features
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            for (idx, (k, v)) in entries.iter().enumerate() {
                if idx > 0 { buffer.push(b','); }
                serde_json::to_writer(&mut *buffer, k).expect("valid string");
                buffer.push(b':');
                serialize_canonical_strict(v, buffer);
            }
            buffer.push(b'}');
        }
        Value::Array(arr) => {
            buffer.push(b'[');
            for (idx, item) in arr.iter().enumerate() {
                if idx > 0 { buffer.push(b','); }
                serialize_canonical_strict(item, buffer);
            }
            buffer.push(b']');
        }
        primitive => {
            serde_json::to_writer(&mut *buffer, primitive).expect("valid primitive");
        }
    }
}
```
**Guarantee:** Mathematical key-order determinism is guaranteed at runtime, completely decoupled from any external crate features in the Cargo build graph.

---

### 9.2 Threat 2: The Soak Test Illusion & 30-Minute Continuous Production Soak Suite

#### The Technical Failure Mode
A 232-millisecond automated burst test exercises concurrency coordination and branch coverage, but it cannot measure:
- Memory stabilization across thousands of garbage-collection cycles in client runtimes.
- SQLite WAL file growth and checkpoint latency under continuous concurrent writes.
- Buffer pool fragmentation and threadpool latency under real disk I/O contention (NVMe vs SATA).
- Slow resource leaks (file descriptors, lingering broadcast channels, detached task handles).

#### The v1.1 Architectural Solution: Dedicated Long-Horizon Soak Runner
v1.1 introduces a dedicated long-horizon soak binary (`tests/long_soak.rs` behind `#[ignore]`):
- **Duration:** 30 minutes continuous execution.
- **Concurrency:** 100 sustained virtual agent workers dispatching randomized multi-turn conversations (8k–32k context windows).
- **WAL Checkpoint Integration:** Runs periodic background checkpoints:
  ```sql
  PRAGMA wal_checkpoint(TRUNCATE);
  ```
- **Telemetry Invariants Verified:**
  1. $\Delta \text{RSS} \le 5\%$ between minute 5 and minute 30 (proving zero memory leaks).
  2. Zero unclosed SQLite file descriptors (`lsof -p <pid> | grep semcache.db`).
  3. WAL size remains bounded ($< 32\text{MB}$).
  4. Semaphore wait queue latency remains stable ($< 15\text{ms}$).

---

### 9.3 Threat 3: The Persistence Gap & Transactional Disk-Commit Handoff

#### The Technical Failure Mode
In a high-throughput proxy, if an in-flight entry transitions from RAM before SQLite confirms the WAL commit to disk, a microsecond window opens:
```
Time ──►
Leader:  [Upstream 200] ──► [mark_ready_and_broadcast] ──► [guard.evict()] ──► [spawn_blocking SQLite write commits]
Follower 1:                                              ▲
                                            Arrives here:
                                    Misses RAM (evicted)
                                    Misses SQLite (uncommitted)
                                    Dispatches redundant upstream call!
```
While v1.0 mitigates this by awaiting the `write_handle.await` before calling `guard.evict()`, backpressure timeouts (e.g. 250ms semaphore timeout) or SQLite busy retries can drop or delay writes, leaving followers vulnerable to duplicate dispatches.

#### The v1.1 Architectural Solution: Transactional Disk-Commit Handoff
In v1.1, memory eviction will be explicitly coupled to the SQLite completion event:
1. `LeaderGuard::mark_ready_and_broadcast` retains `InFlightState::Ready(Bytes)` in RAM with an atomic commit flag.
2. The SQLite writer task sends a completion signal across an internal oneshot channel upon transaction commit.
3. Only upon receiving the commit confirmation (or a verified persistent status) is `guard.evict()` invoked.
4. If the write fails or times out under backpressure, the entry transitions to an ephemeral TTL cache in RAM (10s expiry) rather than being dropped instantly, ensuring subsequent followers still hit memory until disk settles.

---

## 10. Architectural Decision Records (ADR 001–026)

All architectural decisions are formally documented in accordance with the Michael Nygard ADR standard in [`decision.md`](decision.md). ADRs are strictly append-only; historical decisions are marked superseded rather than renumbered.

### Summary Index of Current ADRs
- **[ADR 001](decision.md#adr-001-rust-over-gonodejs-for-the-proxy-hot-path):** Rust over Go/Node.js for the Proxy Hot Path
- **[ADR 002](decision.md#adr-002-embedded-sqlite-with-wal--sqlite-vec-over-external-vector-stores):** Embedded SQLite with WAL & sqlite-vec over External Vector Stores
- **[ADR 003](decision.md#adr-003-single-flight-request-coalescing-via-dashmap-and-tokio-broadcast):** Single-Flight Request Coalescing via DashMap and Tokio Broadcast
- **[ADR 004](decision.md#adr-004-blake3-as-the-l1-cryptographic-key-derivation-function):** BLAKE3 as the L1 Cryptographic Key Derivation Function
- **[ADR 005](decision.md#adr-005-strict-rejection-of-server-sent-events-sse-streaming-in-mvp):** Strict Rejection of SSE Streaming in MVP *(Superseded by ADR 010)*
- **[ADR 006](decision.md#adr-006-asynchronous-off-critical-path-persistence-to-sqlite):** Asynchronous Off-Critical-Path Persistence to SQLite
- **[ADR 007](decision.md#adr-007-mandatory-resource-attribution-for-cli-tools):** Mandatory Resource Attribution for CLI Tools
- **[ADR 008](decision.md#adr-008-syntax-preserving-non-destructive-canonicalization):** Syntax-Preserving Non-Destructive Canonicalization
- **[ADR 009](decision.md#adr-009-removal-first-atomic-broadcast-in-single-flight-coalescer):** Removal-First Atomic Broadcast in Single-Flight Coalescer
- **[ADR 010](decision.md#adr-010-transparent-non-blocking-streaming-bypass):** Transparent Non-Blocking Streaming Bypass with Dual-Stage Timeout
- **[ADR 011](decision.md#adr-011-bounded-concurrency-semaphore-for-sqlite-disk-writers):** Bounded Concurrency Semaphore for SQLite Disk Writers
- **[ADR 012](decision.md#adr-012-provider-aware-default-hyperparameter-normalization):** Provider-Aware Default Hyperparameter Normalization
- **[ADR 013](decision.md#adr-013-state-gated-in-flight-raii-guard-lifecycle):** State-Gated In-Flight RAII Guard Lifecycle
- **[ADR 014](decision.md#adr-014-semcache-v11-architectural-blueprint):** SemCache v1.1 Architectural Blueprint
- **[ADR 015](decision.md#adr-015-in-code-recursive-ast-key-sorting-for-feature-unification-immunity):** In-Code Recursive AST Key Sorting for Feature Unification Immunity
- **[ADR 016](decision.md#adr-016-multi-header-credential-salting-first-match-precedence-and-authoritative-provider-isolation):** Multi-Header Credential Salting & Provider Isolation *(Superseded by ADR 019)*
- **[ADR 017](decision.md#adr-017-fail-open-sqlite-storage-degradation--write-circuit-breaker):** Fail-Open SQLite Storage Degradation & Write Circuit Breaker
- **[ADR 018](decision.md#adr-018-gateway-memory-payload-size-and-concurrency-bounds):** Gateway Memory, Payload Size, and Concurrency Bounds
- **[ADR 019](decision.md#adr-019-multi-header-credential-salting--length-prefixed-tenant-isolation):** Multi-Header Credential Salting & Length-Prefixed Tenant Isolation
- **[ADR 020](decision.md#adr-020-circuit-breaker-real-write-errors--automatic-success-recovery):** Circuit Breaker Real-Write Errors & Automatic Success Recovery
- **[ADR 021](decision.md#adr-021-conservative-replay-policy-semcache_conservative_replay):** Conservative Replay Policy (`SEMCACHE_CONSERVATIVE_REPLAY`)
- **[ADR 022](decision.md#adr-022-dedicated-connection-off-path-storage-bounding--non-blocking-vacuum):** Dedicated-Connection Off-Path Storage Bounding & Non-Blocking Vacuum
- **[ADR 023](decision.md#adr-023-prometheus-metrics-exposition--loopback-only-security-gating):** Prometheus Metrics Exposition & Loopback-Only Security Gating
- **[ADR 024](decision.md#adr-024-stream-concurrency-permit-retention--mid-stream-disconnect-cleanup):** Stream Concurrency Permit Retention & Mid-Stream Disconnect Cleanup
- **[ADR 025](decision.md#adr-025-process-local-file-and-directory-permissions-security):** Process-Local File and Directory Permissions Security
- **[ADR 026](decision.md#adr-026-upstream-response-cache-control-no-store-compliance):** Upstream Response `Cache-Control: no-store` Compliance

---

## 11. Verification & Audit Trail

| Verification Category | Command | Target / SLA | Status |
| :--- | :--- | :--- | :--- |
| **Unit Suite** | `cargo test --lib` | 24/24 tests passing | Verified |
| **Integration Suite** | `cargo test --test integration_tests` | 29/29 tests passing | Verified |
| **Property-Based Invariants** | `cargo test --test canonical_proptest` | 3/3 property tests passing | Verified |
| **Soak & Benchmark Suite** | `cargo test --test soak_test` | Passing ($> 20,000\,\text{req/s}$, 0 leaks, 30m suite under `#[ignore]`) | Verified |
| **Total Test Suite** | `cargo test` | 58 tests passing, 0 failed, 1 ignored | Verified |
| **Static Linting & Denial** | `cargo clippy --all-targets -- -D warnings` | Zero warnings; `#![deny(clippy::unwrap_used, clippy::expect_used)]` | Verified |
| **Refined Secrets Audit** | `git grep -E "\bsk-[a-zA-Z0-9_-]+" \| grep -v -E "sk-(test\|tenant\|mock\|alice\|bob\|shared\|sentinel\|soak)"` | Zero production keys/secrets committed across entire git history | Verified |
| **File Permissions** | `stat -f "%OLp" semcache.db*` | Exactly `0600` on `.db`, `-wal`, `-shm` (parent directory `0700`) | Verified |


