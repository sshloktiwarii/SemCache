# SemCache

**High-Throughput, Exact-Match Caching & Single-Flight Coalescing Reverse Proxy for LLMs**

SemCache is an embedded, local-first HTTP reverse proxy gateway designed to sit transparently between client AI applications/agent frameworks and OpenAI-compatible LLM endpoints. It prevents duplicate upstream calls, preserves API rate limits (HTTP 429), and offloads repetitive agent prompts via deterministic canonicalization and sharded single-flight coalescing.

### 🛡️ The 3 Core Architecture Guarantees
- **Zero Infrastructure:** Self-contained single binary. No Redis, Memcached, external database, or daemon cluster required.
- **Zero-Trust Privacy:** 100% local persistence via embedded SQLite WAL with restricted permissions (`0600` file, `0700` dir). Never persists raw API keys, never emits telemetry over the WAN.
- **1-Line Drop-In:** Fully OpenAI-wire-compatible. Point any SDK or AI agent framework (Python, Node, Cursor, Claude Code, MiroFish) to SemCache by changing `baseURL`.

> [!TIP]
> **New to SemCache or prefer plain English?**  
> Check out the beginner-friendly [**Plain-English Guide (README.simple.md)**](README.simple.md) for a 60-second quickstart without systems jargon!

---

## ⚡ Architectural Comparison: Why Not Redis or Memcached?

A common question from infrastructure architects is: *"Why deploy SemCache instead of an existing Redis or Memcached cluster?"*

General-purpose key-value caches operate on raw binary/string keys and are unaware of the semantics of LLM payload formatting. Here is how SemCache fundamentally differs:

| Architectural Dimension | Generic Key-Value Cache (Redis / Memcached) | **SemCache** |
| :--- | :--- | :--- |
| **JSON Key Permutation** | ❌ **Cache Miss** (different key order yields different hash) | ✅ **Deterministic Hit** (Recursive AST canonicalization sorts JSON keys) |
| **Whitespace Invariance** | ❌ **Cache Miss** (trailing newlines/indentation break key) | ✅ **Deterministic Hit** (Syntax-preserving whitespace normalization) |
| **Thundering Herd** | ❌ **Cache Stampede** (concurrent agents all query upstream simultaneously) | ✅ **Single-Flight Coalesced** (1 upstream fetch; concurrent waiters subscribed via Tokio broadcast) |
| **Streaming Responses** | ❌ Cannot inspect or tunnel chunked Server-Sent Events | ✅ **Transparent SSE Tunnel** with 180s TTFB & 30s chunk watchdogs |
| **Infrastructure Overhead** | Dedicated service, TCP connection pool, memory eviction tuning | **Zero dependencies** (embedded in-process SQLite WAL storage) |
| **Credential Security** | Often unencrypted in shared memory; keys stored alongside data | Length-prefixed multi-header salting; zero API keys written to disk |
| **L1 Response Latency** | Network round-trip to Redis: 1.0ms – 5.0ms | In-process embedded lookup: **< 1.5ms** |

---

## Explicit Scope: Shipped vs Not Shipped

> [!IMPORTANT]
> SemCache v1.0 delivers production-grade exact-match caching and concurrency coalescing for local development, CI/CD runners, and edge deployments.

| Category | Capability | Status |
| :--- | :--- | :--- |
| **L1 Exact Cache** | SIMD BLAKE3 32-byte exact match | **SHIPPED** |
| **AST Canonicalization** | Recursive in-code key sorting (Cargo unification immune) | **SHIPPED** |
| **Single-Flight Coalescing** | Sharded `DashMap` + `tokio::sync::broadcast` | **SHIPPED** |
| **Tenant Isolation** | Multi-header credential salting (`Authorization`, `api-key`, `x-api-key`) | **SHIPPED** |
| **Provider Namespacing** | Provider-aware normalization (`OpenAi`, `Ollama`, `Generic`) | **SHIPPED** |
| **Storage Engine** | Embedded SQLite WAL with `PRAGMA busy_timeout = 5000` | **SHIPPED** |
| **Fail-Open Resilience** | SQLite read/write errors degrade caching without failing proxying | **SHIPPED** |
| **Streaming Bypass** | Transparent SSE tunnel with 180s TTFB and 30s chunk watchdogs | **SHIPPED** |
| **Operational Hardening** | Body size limits, upstream concurrency limits, `/healthz`, SIGTERM | **SHIPPED** |
| **L2 Semantic Vector Cache** | Local embeddings (`fastembed-rs`) & indexed `sqlite-vec` tables | **NOT SHIPPED (Roadmapped for Phase 2)** |
| **Stream Reconstruction** | Full SSE chunk buffering & synthetic re-streaming | **NOT SHIPPED (Roadmapped for Phase 3)** |

---

## Architecture Overview

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
         │   HTTP 200 (Unary JSON)  │                      ┌────────────────────────────────────┐ │
         └──────────────────────────┼──────────────────────┤ Upstream Forwarding (Reqwest)      │ │
                                    │                      │ https://api.openai.com/v1/...      │ │
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

---

## Diagnostic Response Headers

Every response processed by SemCache includes the `x-semcache-status` header:

| Header Value | Meaning |
| :--- | :--- |
| `HIT_L1` | Returned from embedded SQLite exact-match table ($< 1.5\text{ms}$). |
| `HIT_COALESCED` | Subscribed to a concurrent in-flight leader or retrieved from `Ready` memory. |
| `MISS_UPSTREAM` | Primary leader worker forwarded request to upstream and cached response. |
| `BYPASS_STREAM` | Streaming completion (`stream: true`) forwarded through watchdog proxy. |
| `BYPASS_NO_STORE`| Request carried `Cache-Control: no-store` or upstream sent `Cache-Control: no-store`. |
| `BYPASS_OVERSIZED`| Upstream response exceeded response size cap; bypassed cache without error. |
| `BYPASS_STABLE_ONLY`| Conservative replay active (`SEMCACHE_CONSERVATIVE_REPLAY=true`); request has temperature > 0 and no seed. |

---

## Configuration Reference

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `SEMCACHE_BIND` | `127.0.0.1:3000` | Loopback socket address. Set `0.0.0.0:3000` in Docker (see below). |
| `SEMCACHE_DB_PATH` | `./data/semcache.db` | Filepath for SQLite database (auto-permissions `0600` on file, `0700` on dir). |
| `SEMCACHE_UPSTREAM_URL` | `https://api.openai.com/v1/chat/completions` | Target upstream LLM completion endpoint. |
| `SEMCACHE_DEFAULT_PROVIDER` | `openai` | Authoritative provider profile (`openai`, `ollama`, `generic`). |
| `SEMCACHE_TENANT_ID` | `default_tenant` | Fallback tenant identifier for unauthenticated endpoints. |
| `SEMCACHE_CONSERVATIVE_REPLAY` | `false` | When true, only caches requests with `temperature == 0.0` or explicit `seed`. |
| `SEMCACHE_ENABLE_METRICS` | `true` (loopback) / `false` (`0.0.0.0`) | Exposes Prometheus telemetry at `GET /metrics`. Auto-disabled on `0.0.0.0`. |
| `SEMCACHE_UPSTREAM_TTFB_SECS` | `30` (standard) / `180` (reasoning) | Maximum Time-To-First-Byte before upstream fetch aborts. |
| `SEMCACHE_CREDENTIAL_HEADERS` | `authorization,api-key,x-api-key,x-goog-api-key` | Comma-separated list of headers to salt for tenant isolation. |
| `SEMCACHE_MAX_DB_BYTES` | `2147483648` (2 GB) | Disk storage limit; auto-prunes oldest records on background connection. |
| `SEMCACHE_MAX_REQUEST_BYTES` | `33554432` (32 MB) | Maximum request size (accommodates base64 vision images; HTTP 413). |
| `SEMCACHE_MAX_RESPONSE_BYTES`| `10485760` (10 MB) | Maximum response size; larger payloads bypass cache via streaming. |
| `SEMCACHE_MAX_READY_BYTES` | `134217728` (128 MB) | Bounded memory ceiling for in-flight `Ready` responses. |
| `SEMCACHE_MAX_CONCURRENT_WRITES` | `4` | Concurrency limit on SQLite writer tasks with 5-failure circuit breaker. |
| `SEMCACHE_MAX_UPSTREAM_CONCURRENCY` | `256` | Upstream concurrency permit ceiling (returns HTTP 503 + Retry-After: 5 on shed). |
| `SEMCACHE_TTL_DAYS` | `7` | Cache entry retention window before automatic background pruning. |
| `SEMCACHE_UPSTREAM_TIMEOUT_SECS` | `300` | Maximum overall upstream HTTP connection timeout. |

### Replay Policy Trade-Off: Conservative Replay
The setting `SEMCACHE_CONSERVATIVE_REPLAY` defaults to **`false`**.
- **Default (`false`):** Caches and replays all canonical prompt hits regardless of temperature. This delivers maximum cache hit rates for AI agent retry loops, automated test suites, and development iteration.
- **Conservative (`true`):** Strict mode. Skips caching any request that does not explicitly set `temperature: 0` or an explicit `seed` with `temperature <= 0.0`. We avoid calling this "deterministic" because GPU floating point non-associativity, MoE routing, and provider-side dynamic batching mean true determinism cannot be guaranteed even with seed.

---

## Key Architectural Decisions

- **ADR 001:** Rust over Go/Node.js for zero GC pauses on proxy hot paths.
- **ADR 002:** Embedded SQLite with WAL mode over external vector DBMS infrastructure.
- **ADR 003:** Single-flight request coalescing via DashMap and Tokio broadcast channels.
- **ADR 004:** BLAKE3 as the L1 cryptographic key derivation function.
- **ADR 005:** Strict rejection of SSE streaming *(Superseded by ADR 010)*.
- **ADR 006:** Asynchronous off-critical-path persistence to SQLite.
- **ADR 007:** Mandatory resource attribution for CLI commands.
- **ADR 008:** Syntax-preserving non-destructive AST canonicalization.
- **ADR 009:** Removal-first atomic broadcast in single-flight coalescer.
- **ADR 010:** Transparent non-blocking streaming bypass with dual-stage timeout.
- **ADR 011:** Bounded concurrency semaphore for SQLite disk writers.
- **ADR 012:** Provider-aware default hyperparameter normalization.
- **ADR 013:** State-gated in-flight RAII guard lifecycle.
- **ADR 014:** SemCache v1.1 architectural blueprint.
- **ADR 015:** In-code recursive AST key sorting immune to Cargo feature unification.
- **ADR 016:** Multi-header credential salting precedence *(Superseded by ADR 019)*.
- **ADR 017:** Fail-open SQLite storage degradation & write circuit breaker.
- **ADR 018:** Gateway memory, payload size, and concurrency bounds.
- **ADR 019:** Multi-header length-prefixed credential salting for collision-proof tenant isolation.
- **ADR 020:** Circuit breaker real-write error accounting and automatic success recovery.
- **ADR 021:** Conservative replay policy (`SEMCACHE_CONSERVATIVE_REPLAY`) with default `false`.
- **ADR 022:** Dedicated-connection off-path storage bounding & disk-backed vacuum.
- **ADR 023:** Prometheus metrics exposition & loopback-only security gating.
- **ADR 024:** Stream concurrency permit retention & mid-stream disconnect cleanup.
- **ADR 025:** Process-local `0600` file and `0700` directory security (dropped process-wide umask).
- **ADR 026:** Upstream response `Cache-Control: no-store` compliance.

---

## What's NOT Shipped (Honest Boundaries)

SemCache is focused on solving local and CI caching with maximum engineering rigor. We intentionally do not ship:
1. **Multi-Node Distributed Clustering:** SemCache is an embedded, single-node local-first proxy. It does not synchronize cache tables across nodes or run Raft/Paxos.
2. **Unvalidated Vector Distance Caching:** We do not perform fuzzy semantic vector lookups with uncalibrated similarity thresholds that return incorrect responses. Exact match L1 is 100% reliable and verified.
3. **Client-Side Credential Storage:** We never persist raw API keys or client credentials to SQLite disk or structured telemetry.
4. **Synthetic SSE Token Playback:** Streams are passed through transparently without synthetic chunk fabrication.

---

## Docker & Container Deployment

> [!WARNING]
> **Docker Binding & Network Security**
> By default, SemCache binds to `127.0.0.1:3000` (loopback). Inside a Docker container, binding to `127.0.0.1` makes the gateway unreachable from the host or other containers on the Docker bridge network.
> 
> To run in Docker, you must set:
> ```bash
> -e SEMCACHE_BIND="0.0.0.0:3000"
> ```
> When bound to `0.0.0.0:3000`, `/metrics` is **disabled by default** to prevent external telemetry leakage.
> Always deploy SemCache in a private network or front it with an authenticated reverse proxy (Nginx, Envoy, Cloudflare).

---

## Quickstart

### 1. Run with OpenAI
```bash
# Build release binary
cargo build --release

# Run gateway pointing to OpenAI
OPENAI_UPSTREAM_URL="https://api.openai.com/v1/chat/completions" \
SEMCACHE_BIND="127.0.0.1:3000" \
./target/release/semcache
```

### 2. Run with Local LLMs (Ollama)
```bash
# Proxy local Ollama running on port 11434 with zero-cost cache replay:
SEMCACHE_BIND="0.0.0.0:8080" \
OPENAI_UPSTREAM_URL="http://localhost:11434/v1/chat/completions" \
SEMCACHE_DEFAULT_PROVIDER="ollama" \
./target/release/semcache
```

### Pointing AI SDKs / Agents to SemCache

```bash
export OPENAI_BASE_URL="http://127.0.0.1:3000/v1"
```

### Health & Metrics Probes

```bash
# Health check
curl http://127.0.0.1:3000/healthz
# {"status":"ok","service":"semcache"}

# Prometheus metrics exposition (available on loopback)
curl http://127.0.0.1:3000/metrics
# # HELP semcache_requests_total Total number of chat completion requests received
# # TYPE semcache_requests_total counter
# semcache_requests_total 42
```

---

## 🧪 Low-Risk Adoption Blueprint: Staging & CI/CD

Enterprise engineering teams rarely adopt new network proxies directly into production on day 1. SemCache is designed for a friction-free evaluation pipeline:

### Phase 1: CI/CD Test Suite Acceleration
- Point test runners in GitHub Actions, GitLab CI, or CircleCI to a local SemCache binary or container.
- Automated evaluation runs and prompt regression tests replay identical prompt fixtures in **$< 1.5\text{ms}$** with **$0 token spend**.

### Phase 2: Local Developer Tooling & Agent Sandboxes
- Developers running coding agents (Cursor, Claude Code, Aider) or custom agent loops set `OPENAI_BASE_URL=http://localhost:3000/v1`.
- Prevents team members from hitting provider rate limits (`HTTP 429`) or blowing through monthly API allowances during iterative debugging.

### Phase 3: Staging Environment Shared Gateway
- Deploy SemCache as an internal sidecar or staging gateway in your internal container stack.
- Multiple staging microservices share coalesced responses and cached prompt results with zero risk to production stability.

---

## 🐝 Empirical Validation: MiroFish Multi-Agent Swarm Benchmark

SemCache underwent end-to-end integration and load testing against **MiroFish-Offline**, a high-concurrency multi-agent swarm simulation platform orchestrating dozens of autonomous AI personas debating, generating Neo4j knowledge graphs, and conducting world surveys.

- **100+ Concurrent Agent Requests:** Dispatched parallel agent debate prompts through SemCache proxying a local Ollama instance (`qwen2.5-coder:7b` accelerated on Apple Silicon Metal).
- **Sub-1.5ms L1 Hit Latency:** Re-evaluated survey questions and simulation graph nodes resolved from embedded SQLite WAL in $<1.5\text{ms}$ without re-invoking the neural network.
- **Zero Stability Regressions:** Single-flight request coalescing prevented connection starvation and GPU thermal throttling under high-burst multi-agent dispatch.

---

## Test Verification Suite

```bash
# Run full unit and integration test suite (58 tests passing)
cargo test

# Run property-based invariants (proptest)
cargo test --test canonical_proptest

# Run sustained soak suite (20k+ req/s, 0 memory leaks)
cargo test --test soak_test

# Run long-horizon 30-minute soak test
cargo test --test soak_test -- test_long_horizon_sustained_soak_30min --ignored --nocapture

# Enforce zero clippy warnings and unwrap/expect denials
cargo clippy --all-targets -- -D warnings
```

---

## License

This project is licensed under the [MIT License](LICENSE).


