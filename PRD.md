# SemCache: Product Requirements Document (PRD)

**Document Version:** 1.0.0 (Production Hardened) | **Target Roadmap:** v1.1.0  
**Status:** Approved & Implemented  
**Target Release:** SemCache v1.0.0  
**Author:** Principal Systems Architect  
**Repository:** [https://github.com/sshloktiwarii/SemCache.git](https://github.com/sshloktiwarii/SemCache.git)

---

## 1. Executive Summary & Problem Space

### 1.1 The Agentic Explosion & API Cost Trajectory
Autonomous AI agents, coding assistants, and multi-agent coordination loops execute non-deterministic search graphs, test-and-repair iterations, and automated tool calls. In practice, these workloads exhibit high semantic repetition:
- Agent retry loops dispatch near-identical prompts with slight temperature or formatting variations.
- Concurrent sub-agents in tree-of-thought or tournament architectures query identical foundational contexts simultaneously.
- Standard HTTP reverse proxies fail to cache these requests because LLM client SDKs inject non-semantic hyperparameters (`temperature`, `top_p`, `user`, `seed`) into the request body, and prompt strings often contain insignificant whitespace variance.

### 1.2 The Solution: SemCache
**SemCache** is an embedded, local-first HTTP proxy gateway designed to sit transparently between client AI applications and OpenAI-compatible LLM endpoints. It delivers cost reduction, latency optimization, and rate-limit preservation through:
1. **Deterministic Request Canonicalization:** Stripping non-semantic metadata and normalizing text while preserving code syntax.
2. **Multi-Tenant Cache Isolation:** Authorization bearer tokens are salted directly into cache digests.
3. **L1 Deterministic Exact-Match Cache:** Sub-millisecond retrieval via SIMD-accelerated BLAKE3 hashing.
4. **Single-Flight Request Coalescing:** Consolidating concurrent duplicate requests into a single upstream call with race-free memory handoff.
5. **Transparent Streaming Bypass:** Non-blocking passthrough of SSE tokens with dual-stage timeout watchdogs.
6. **L2 Semantic Vector Cache (Phase 2):** Vector-similarity lookup via embedded `sqlite-vec`.

```
[ AI Agent / Client ] 
        │ (POST /v1/chat/completions)
        ▼
[ SemCache Gateway (Port 3000) ]
        ├── 1. Stream Check (stream == true) ──► [ Transparent SSE Bypass: 180s/30s Watchdog ]
        ├── 2. Syntax-Preserving Canonicalization & Tenant-Salted Hashing
        ├── 3. L1 Exact Match (SQLite WAL) ────► [ Cache Hit: < 1.5ms ]
        ├── 4. Single-Flight Coalesce (DashMap) ► [ Concurrent Wait: 0 Upstream Tokens ]
        ├── 5. L2 Vector Search (sqlite-vec) ──► [ Phase 2 Roadmap ]
        └── 6. Upstream Forwarding ───────────► [ OpenAI / vLLM / Ollama ]
```

---

## 2. Personas & Target Users

| Persona | Environment | Primary Pain Point | Core Need |
| :--- | :--- | :--- | :--- |
| **Autonomous Agent Developer** | Local workstations, containerized loops | Cost explosion during recursive self-healing or multi-agent swarms. | Transparent drop-in proxy with zero cloud infrastructure overhead. |
| **AI Infrastructure Engineer** | CI/CD test runners, development sandboxes | Upstream rate-limit exhaustion (HTTP 429) on automated integration tests. | Single-flight coalescing and high-throughput local caching. |
| **Local LLM Operator** | Edge servers running Ollama/vLLM | Redundant GPU compute wasted on re-evaluating prompt prefixes. | Deterministic offloading of prompt evaluations. |

---

## 3. Core Functional Requirements (FR)

### FR-01: OpenAI-Compatible Ingress Proxy
- The gateway binds to `127.0.0.1:3000` by default (configurable via `SEMCACHE_BIND`).
- Exposes an OpenAI-compatible endpoint: `POST /v1/chat/completions`.
- Preserves incoming HTTP `Authorization` and credential headers, passing them upstream.

### FR-02: Syntax-Preserving Canonicalization Engine
- **Non-Generative Metadata Stripping:** The gateway parses incoming JSON payloads and strips client tracking metadata (`user`).
- **Default Parameter Stripping:** If `stream: false` is present, it is stripped to prevent cache divergence from omitted keys.
- **Generative Hyperparameter Preservation:** Hyperparameters that alter generation dynamics (`temperature`, `top_p`, `presence_penalty`, `frequency_penalty`, `seed`, `logit_bias`) are preserved in the cache key.
- **Provider-Aware Default Normalization:**
  - `OpenAi`: Normalizes `temperature: 1.0`, `top_p: 1.0`, `presence_penalty: 0.0`, `frequency_penalty: 0.0`.
  - `Ollama`: Normalizes `temperature: 0.8`, `top_p: 0.9` (retains `1.0` verbatim).
  - `Generic`: Preserves all parameters verbatim.
- **Syntax-Preserving Text Normalization:**
  - Content strings in `messages` and `prompt` are trimmed only at the outermost string boundaries.
  - Internal indentation, tabs, newlines, and formatting are preserved 100% verbatim to protect code blocks, YAML, and Markdown.
- **Length-Prefixed Multi-Tenant Salting (ADR-019):** Credential headers (`authorization`, `api-key`, `x-api-key`, `x-goog-api-key`) are extracted, sorted lexicographically, and encoded with length prefixes:
  $$\text{SaltHasher} \leftarrow \bigoplus_{i} \left[ \text{len}(K_i)_{\text{u32 LE}} \,\|\, K_i \,\|\, \text{len}(V_i)_{\text{u32 LE}} \,\|\, V_i \right]$$
  If unauthenticated, length-prefixed `SEMCACHE_TENANT_ID` is used. Prevents delimiter-shifting collision attacks.

### FR-03: Non-Blocking Streaming Bypass
- If an incoming payload contains `stream: true`, the gateway bypasses caching and transparently proxies the stream to the upstream LLM endpoint.
- **Dual-Stage Timeout Watchdog:** The stream is wrapped in `IdleTimeoutStream`, enforcing:
  1. A configurable Time-To-First-Byte (TTFB) timeout (`SEMCACHE_UPSTREAM_TTFB_SECS`, default 180s for reasoning models, 30s for standard) to accommodate extended thinking phases.
  2. A 30-second inter-chunk idle watchdog once token streaming commences.
- Responses are tagged with header `x-semcache-status: BYPASS_STREAM`.
- Upstream concurrency permits are held via RAII and automatically released on client disconnect, preventing ghost tasks.

### FR-04: L1 Exact-Match Deterministic Caching
- Compute a 32-byte BLAKE3 hash over the canonicalized JSON bytes with tenant salt.
- Query the SQLite `exact_cache` table by `canonical_hash`.
- If a match exists:
  - Return the cached response immediately with HTTP status 200.
  - Inject the custom HTTP header: `x-semcache-status: HIT_L1`.
  - Content-Type must be `application/json`.

### FR-05: Single-Flight Request Coalescing
- When an L1 cache miss occurs, the gateway acquires a DashMap shard lock on `InFlightMap`:
  - **Follower (Pending):** If an upstream query is in flight, subscribe to a `tokio::sync::broadcast` channel and asynchronously await the leader's response. On receipt, return the payload with header `x-semcache-status: HIT_COALESCED`.
  - **Follower (Ready):** If the response is already in memory pending disk write, immediately return a clone of the payload.
  - **Primary Leader:** Register as the leader, retain a state-gated `LeaderGuard`, and proceed to upstream execution.
- **Error Propagation & Drop Safety:**
  - If upstream fails (e.g. 429), the leader broadcasts `Err(CoalesceError)` to followers, ensuring followers never hang.
  - `LeaderGuard` implements state-gated RAII drop: only guards abandoned in `Pending` state purge the map and broadcast errors.

### FR-06: [Phase 2 / Roadmap] Semantic Vector Search (L2)
- *MVP Scope Note:* v1.0 ships with exact-match L1 and coalescing. Native L2 embedding generation via `fastembed-rs` and indexed `sqlite-vec` lookups are roadmapped for Phase 2.
- Evaluates Cosine similarity $\ge 0.92$ on 1536-dimensional embeddings.

### FR-07: Upstream Forwarding & Non-Cacheable Failure Handling
- When a request misses all cache layers, forward the payload to the configured upstream endpoint.
- **Error Propagation:** If upstream returns a non-2xx status code:
  - The error payload and status code must be returned to the client immediately.
  - The error response **MUST NOT** be persisted into the cache.

### FR-08: Dedicated Off-Path Persistence & Storage Bounding (ADR-022)
- Upon receiving a successful 2xx response from upstream:
  - Transition in-flight state to `Ready` and broadcast bytes to awaiting followers.
  - Asynchronously persist into SQLite WAL using `tokio::task::spawn_blocking` gated behind an `Arc<tokio::sync::Semaphore>` (4 concurrent permits, 250ms acquisition timeout).
  - Background database maintenance (TTL expiration, soft-cap LRU pruning below `SEMCACHE_MAX_DB_BYTES`, non-blocking VACUUM) runs once per hour on a dedicated SQLite connection completely off the request hot path.
  - SQLite temporary storage configured with `PRAGMA temp_store = FILE;` to guarantee memory safety.

### FR-09: Replay Policy & Conservative Mode (ADR-021)
- Governed by `SEMCACHE_CONSERVATIVE_REPLAY` (default: `false`).
- When `false` (default): Standard development and CI requests are cached and replayed based on canonical payload hashes, maximizing cache hit rate.
- When `true`: Only requests with `temperature == 0.0` or a valid `seed` with `temperature <= 0.0` are cached and replayed; stochastic requests bypass cache storage with `x-semcache-status: BYPASS_STABLE_ONLY`.

### FR-10: Upstream Response `Cache-Control: no-store` Compliance (ADR-026)
- If an upstream LLM response contains `Cache-Control: no-store` (case-insensitive), SemCache serves the response to the client with `x-semcache-status: BYPASS_NO_STORE` and skips SQLite persistence, strictly honoring RFC 9111.

### FR-11: Gated Prometheus Metrics Telemetry (ADR-023)
- Governed by `SEMCACHE_ENABLE_METRICS`.
- Automatically enabled when bound to loopback (`127.0.0.1`), and default disabled when bound to `0.0.0.0` to prevent unauthenticated cache usage pattern leaks in container networks. Returns HTTP 404 when disabled.

---

## 4. Non-Functional Requirements (NFR)

| ID | Category | Requirement Specification |
| :--- | :--- | :--- |
| **NFR-01** | **Latency SLA** | L1 cache hits must return with P99 latency $< 1.5\text{ms}$. Coalesced followers receive responses $< 0.5\text{ms}$ after leader completion. |
| **NFR-02** | **Memory Safety** | Zero `.unwrap()`, `.expect()`, or explicit panics in production request paths. Memory leaks strictly prevented by state-gated RAII guards and circuit breaker. |
| **NFR-03** | **Storage Engine** | Embedded SQLite with Write-Ahead Logging (`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000; PRAGMA temp_store = FILE;`). Zero external DBMS dependencies. |
| **NFR-04** | **Concurrency & Pool Sizing** | The gateway must sustain 5,000+ concurrent client connections. Reader pool is sized to 32/64 connections with 500ms checkout timeout to fail-open gracefully under disk stalls, operating against microsecond SQLite WAL memory-mapped read loops without thread starvation. |
| **NFR-05** | **Zero Allocation** | Critical hot paths must utilize zero-copy `bytes::Bytes` slicing to minimize heap allocations during HTTP and disk shuttling. |
| **NFR-06** | **Telemetry** | Every response must include diagnostic headers (`x-semcache-status`) and emit structured logs via `tracing`. |
| **NFR-07** | **Process Security** | Avoid process-wide umask modifications. Pre-create database files with `0600` permissions and containing directories with `0700` mode. |

---

## 5. Configuration & Environment Variables

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `SEMCACHE_BIND` | `127.0.0.1:3000` | Gateway listen address and port. |
| `SEMCACHE_UPSTREAM_URL` | `https://api.openai.com` | Target OpenAI-compatible LLM endpoint. |
| `SEMCACHE_DB_PATH` | `./data/semcache.db` | Local SQLite database file path. |
| `SEMCACHE_MAX_DB_BYTES` | `2147483648` (2 GB) | Soft cap on database disk storage. Prunes oldest records above limit. |
| `SEMCACHE_MAX_REQUEST_BYTES` | `33554432` (32 MB) | Ingress request payload cap (supports vision / multimodal prompts). |
| `SEMCACHE_MAX_RESPONSE_BYTES` | `10485760` (10 MB) | Response buffer cap before bypassing cache for oversized streams. |
| `SEMCACHE_MAX_READY_BYTES` | `134217728` (128 MB) | Bounded in-flight RAM buffer cap for coalesced follower dispatches. |
| `SEMCACHE_MAX_UPSTREAM_CONCURRENCY` | `256` | Maximum concurrent upstream HTTP dispatches before 503 shedding. |
| `SEMCACHE_CONSERVATIVE_REPLAY` | `false` | When true, only caches and replays `temperature: 0` or seeded traffic. |
| `SEMCACHE_ENABLE_METRICS` | `true` (loopback) / `false` (`0.0.0.0`) | Exposes Prometheus telemetry at `GET /metrics`. |
| `SEMCACHE_UPSTREAM_TTFB_SECS` | `30` (standard) / `180` (reasoning) | Maximum Time-To-First-Byte before upstream fetch aborts. |
| `SEMCACHE_DEFAULT_PROVIDER` | `openai` | Normalization profile (`openai`, `ollama`, `generic`). |
| `SEMCACHE_TENANT_ID` | `default_tenant` | Namespace partition for unauthenticated client requests. |
| `SEMCACHE_CREDENTIAL_HEADERS` | `authorization,api-key,x-api-key,x-goog-api-key` | Header names absorbed into the length-prefixed credential salt. |

---

## 6. System State Machine & Request Lifecycle

```mermaid
stateDiagram-v2
    [*] --> InboundRequest: POST /v1/chat/completions
    InboundRequest --> CheckStream: Check stream parameter
    
    CheckStream --> StreamBypass: stream == true
    StreamBypass --> ProxySSE: IdleTimeoutStream (180s TTFB, 30s chunk)
    ProxySSE --> [*]: HTTP 200 (BYPASS_STREAM)
    
    CheckStream --> Canonicalize: stream == false / omitted
    Canonicalize --> HashBlake3: Provider defaults + Multi-tenant salt
    HashBlake3 --> L1Lookup: Query exact_cache table
    
    L1Lookup --> ReturnL1Hit: Record found
    ReturnL1Hit --> [*]: HTTP 200 (HIT_L1)
    
    L1Lookup --> CheckCoalesce: L1 Miss
    CheckCoalesce --> AwaitBroadcast: In-flight channel exists (Pending)
    AwaitBroadcast --> ReturnCoalesced: Receive broadcast bytes
    ReturnCoalesced --> [*]: HTTP 200 (HIT_COALESCED)
    
    CheckCoalesce --> ImmediateMemoryHit: In-flight ready in RAM (Ready)
    ImmediateMemoryHit --> [*]: HTTP 200 (HIT_COALESCED)
    
    CheckCoalesce --> RegisterLeader: Primary worker
    RegisterLeader --> UpstreamFetch: POST https://api.openai.com
    
    UpstreamFetch --> UpstreamError: HTTP 4xx / 5xx
    UpstreamError --> DropLeader: Clean DashMap & Broadcast Error
    DropLeader --> [*]: Propagate Status Code
    
    UpstreamFetch --> UpstreamSuccess: HTTP 200 OK
    UpstreamSuccess --> MarkReadyAndBroadcast: InFlightState::Ready + Send to followers
    MarkReadyAndBroadcast --> AsyncPersist: Bounded write semaphore (4 permits)
    AsyncPersist --> EvictMemory: write_handle.await -> guard.evict()
    EvictMemory --> [*]: HTTP 200 (MISS_UPSTREAM)
```

---

## 6. Release Roadmap

### Phase 1: SemCache v1.0.0 (Production Hardened - Shipped)
- Axum HTTP gateway engine with zero unwrap / panic paths.
- Syntax-preserving canonicalization and multi-tenant token salting.
- Provider-aware default hyperparameter normalization (`OpenAi`, `Ollama`, `Generic`).
- BLAKE3 L1 deterministic caching with SQLite WAL and `PRAGMA busy_timeout = 5000;`.
- Sharded single-flight request coalescing with `Pending`/`Ready` states and state-gated RAII guards.
- Transparent streaming bypass with dual-stage timeout watchdogs (`IdleTimeoutStream`).
- Bounded 4-permit SQLite write semaphore with 250ms backpressure shedding.
- 28-test comprehensive verification suite including sustained soak harness.

### Phase 1.1: SemCache v1.1.0 Architectural Blueprint
- **AST-Recursive Canonical Key Sorter:** Eliminate dependency on `serde_json` crate map ordering, guaranteeing mathematical lexicographical key determinism immune to Cargo workspace feature unification.
- **30-Minute Continuous Production Soak Suite:** Dedicated long-horizon soak binary (`tests/long_soak.rs` behind `#[ignore]`) testing 100 sustained agents streaming multi-gigabyte context windows, evaluating WAL checkpoint performance and flat RSS stabilization.
- **Transactional Disk-Commit Handoff:** Synchronize in-flight memory eviction directly with SQLite WAL commit signals to close any potential microsecond persistence race under heavy backpressure.

### Phase 2: Semantic Vector Expansion (v2.0)
- Native runtime linking of `sqlite-vec`.
- Local embedding generation via `fastembed-rs` (ONNX runtime) to eliminate external embedding API latency.
- Automated threshold calibration for Cosine distance metrics ($\ge 0.92$).
- Full SSE chunk caching and stream reconstruction.
