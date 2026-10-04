# Changelog

All notable changes to **SemCache** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.2.0] - 2026-10-04

### Breaking Changes
- **Configuration Renaming (`SEMCACHE_CONSERVATIVE_REPLAY`):**
  - Renamed `SEMCACHE_DETERMINISTIC_ONLY` to `SEMCACHE_CONSERVATIVE_REPLAY` (with fallback aliases `SEMCACHE_STABLE_ONLY` and legacy `SEMCACHE_DETERMINISTIC_ONLY`).
  - Default remains `false` to optimize agent retry-loop cache hit rates. When set to `true`, only requests with `temperature == 0.0` or an explicit `seed` parameter are cached; stochastic requests bypass cache storage with `x-semcache-status: BYPASS_STOCHASTIC`.
- **Metrics Exposure Security Gating (`SEMCACHE_ENABLE_METRICS`):**
  - The `/metrics` endpoint is now **disabled by default** when `SEMCACHE_BIND` is set to `0.0.0.0` (external interfaces) to prevent leaking query patterns and cache utilization shapes.
  - To expose Prometheus metrics on external interfaces, explicitly set `SEMCACHE_ENABLE_METRICS=true`. Loopback bindings (`127.0.0.1`, `localhost`) continue to expose metrics by default.
- **Client Header `x-semcache-provider` Authority Removed:**
  - Clients can no longer manipulate the backend provider partition via request headers. The server's `SEMCACHE_DEFAULT_PROVIDER` configuration and target upstream URL are authoritative.
- **Inbound Request Cap Raised to 32MB:**
  - `SEMCACHE_MAX_REQUEST_BYTES` default raised from 10MB to 32MB to natively support vision models (GPT-4o, Claude 3.5 Sonnet, Gemini 1.5 Pro) with inline base64 image attachments.
- **Process-Global Security Fix (`umask` removal):**
  - Removed process-global `libc::umask(0o077)` invocation from library functions.
  - Directory and file permissions are now enforced process-locally: containing directory is created with `0700` (`rwx------`), and SQLite `.db` files are pre-created and chmod'd to `0600` (`rw-------`) on every startup. Secondary SQLite files (`-wal`, `-shm`) inherit restricted `0600` permissions from the main file.
- **Default Storage Cap Reduced to 2GB:**
  - Default `SEMCACHE_MAX_DB_BYTES` reduced from 10GB to 2GB (`2 * 1024 * 1024 * 1024`) for local developer laptop hygiene (fully configurable).

### Added
- **Multi-Header Credential Salting with Length-Prefixed Hashing:**
  - Implemented BLAKE3 length-prefixed hashing across sorted `(name, value)` credential tuples (`ADR 019`), preventing cross-tenant collisions on ambiguous headers (e.g. `authorization="ab"` + `api-key="c"` vs `authorization="a"` + `api-key="bc"`).
- **Response-Side `Cache-Control: no-store` Compliance:**
  - Upstream responses containing `Cache-Control: no-store` are returned to client with HTTP 200, broadcast to live in-flight followers, and immediately evicted without writing to SQLite (`ADR 026`).
- **Dedicated Connection Off-Path Maintenance:**
  - Background database maintenance (TTL pruning, storage cap enforcement, and SQLite `VACUUM`) now runs on a dedicated direct connection, completely off the client connection pool and write semaphore (`ADR 022`).
- **RAM Protection Pragmas:**
  - Updated SQLite connection pragma from `PRAGMA temp_store = MEMORY;` to `PRAGMA temp_store = FILE;` to eliminate RAM explosion risks during table defragmentation.
- **Configurable Upstream Timeouts:**
  - Added `SEMCACHE_UPSTREAM_TTFB_SECS` (default 180s) to accommodate deep reasoning models (o1, o3, DeepSeek-R1) on both streaming and non-streaming paths.
- **Ghost Task Mid-Stream Abort:**
  - When `SEMCACHE_CANCEL_ORPHAN_REQUESTS=true`, workers monitor client connection drops during response chunk streaming and abort upstream fetching immediately when 0 listeners remain.
- **Property-Based Invariant Tests (`proptest`):**
  - Added exhaustive property-based tests in `tests/canonical_proptest.rs` verifying JSON key-ordering invariance, whitespace normalization, and fuzz resilience.
- **Long-Horizon 30-Minute Soak Test (`#[ignore]`):**
  - Added 100-worker sustained soak test in `tests/soak_test.rs` to verify zero memory leaks and flat RSS over extended durations.
- **Pre-Commit Quality Gate:**
  - Added `scripts/pre-commit.sh` enforcing `cargo test`, `cargo clippy --all-targets -- -D warnings`, and unredacted secret audit.
- **Open Source Licensing:**
  - Added MIT License in `LICENSE`.

---

## [0.1.0] - 2026-10-04

### Added
- Initial release of SemCache local-first reverse proxy gateway.
- BLAKE3 exact match cache (L1) with embedded SQLite WAL mode.
- Single-flight request coalescer with atomic DashMap and Tokio broadcast channels.
- Transparent SSE streaming bypass with dual-stage TTFB and idle chunk timeouts.
- Fail-open SQLite storage degradation and write circuit breaker.
- Prometheus `/metrics` exposition and structured background telemetry logger.
