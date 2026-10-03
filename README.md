# SemCache

**High-Throughput, Exact-Match Caching & Single-Flight Coalescing Reverse Proxy for LLMs**

SemCache is an embedded, local-first HTTP reverse proxy gateway designed to sit transparently between client AI applications/agent frameworks and OpenAI-compatible LLM endpoints. It prevents duplicate upstream calls, preserves API rate limits (HTTP 429), and offloads repetitive agent prompts via deterministic canonicalization and sharded single-flight coalescing.

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
| `BYPASS_NO_STORE`| Request carried `Cache-Control: no-store`; bypassed cache and coalescing. |
| `BYPASS_OVERSIZED`| Upstream response exceeded response size cap; bypassed cache without error. |

---

## Configuration Reference

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `SEMCACHE_BIND` | `127.0.0.1:3000` | Loopback socket address. Set `0.0.0.0:3000` in Docker (see below). |
| `SEMCACHE_DB_PATH` | `semcache.db` | Filepath for SQLite database (auto-permissions `0600` on Unix). |
| `OPENAI_UPSTREAM_URL` | `https://api.openai.com/v1/chat/completions` | Target upstream LLM completion endpoint. |
| `SEMCACHE_DEFAULT_PROVIDER` | `openai` | Authoritative provider profile (`openai`, `ollama`, `generic`). |
| `SEMCACHE_TENANT_ID` | `default_tenant` | Fallback tenant identifier for unauthenticated endpoints. |
| `SEMCACHE_MAX_REQUEST_BYTES` | `33554432` (32 MB) | Maximum request size (accommodates base64 vision images; HTTP 413). |
| `SEMCACHE_MAX_RESPONSE_BYTES`| `10485760` (10 MB) | Maximum response size; larger payloads bypass cache via streaming. |
| `SEMCACHE_MAX_READY_BYTES` | `134217728` (128 MB) | Bounded memory ceiling for in-flight `Ready` responses. |
| `SEMCACHE_MAX_CONCURRENT_WRITES` | `4` | Concurrency limit on SQLite writer tasks with 5-failure circuit breaker. |
| `SEMCACHE_MAX_UPSTREAM_CONCURRENCY` | `256` | Upstream concurrency permit ceiling (returns HTTP 503 + Retry-After: 5 on shed). |
| `SEMCACHE_TTL_DAYS` | `7` | Cache entry retention window before automatic pruning (`PRAGMA wal_checkpoint(TRUNCATE)`). |
| `SEMCACHE_UPSTREAM_TIMEOUT_SECS` | `300` | Maximum overall upstream HTTP connection timeout. |

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
> **CRITICAL SECURITY REQUIREMENT:** Never expose port 3000 directly to the public internet when bound to `0.0.0.0`. SemCache is an internal accelerating proxy. Always deploy it in a private subnet or front it with a secure reverse proxy (Nginx, Traefik, Envoy, Cloudflare Access) that terminates TLS and enforces ingress authentication.

---

## Quickstart

```bash
# Build release binary
cargo build --release

# Run gateway pointing to OpenAI
OPENAI_UPSTREAM_URL="https://api.openai.com/v1/chat/completions" \
SEMCACHE_BIND="127.0.0.1:3000" \
./target/release/semcache
```

### Pointing AI SDKs / Agents to SemCache

```bash
export OPENAI_BASE_URL="http://127.0.0.1:3000/v1"
```

### Health Probe

```bash
curl http://127.0.0.1:3000/healthz
# {"status":"ok","service":"semcache"}
```

---

## Test Verification Suite

```bash
# Run unit and integration tests (44 tests across all suites)
cargo test --release

# Run multi-threaded barrier stampede and governance tests
cargo test --test integration_tests --release

# Run sustained soak and stress benchmarks
cargo test --test soak_test --release

# Enforce zero clippy warnings and unwrap denials
cargo clippy --all-targets -- -D warnings
```
