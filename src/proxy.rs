use axum::{
    body::Body,
    extract::{Json, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::{sleep_until, Instant, Sleep};

use crate::{
    canonical::{canonicalize_and_hash, Provider},
    coalesce::{CoalesceResult, RequestCoalescer},
    db::{get_exact_cache, insert_exact_cache, DbPool},
    error::SemCacheError,
};

#[derive(Clone)]
pub struct AppState {
    pub db: DbPool,
    pub http_client: reqwest::Client,
    pub coalescer: RequestCoalescer,
    pub upstream_url: String,
    pub sqlite_write_semaphore: Arc<Semaphore>,
    pub upstream_semaphore: Arc<Semaphore>,
    pub default_provider: Provider,
    pub default_tenant_id: String,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub cancel_orphan_requests: bool,
    pub conservative_replay: bool,
    pub upstream_ttfb_secs: u64,
    pub credential_headers: Vec<String>,
    // Observability and Telemetry counters
    pub requests_total: Arc<AtomicU64>,
    pub l1_hits_total: Arc<AtomicU64>,
    pub coalesced_hits_total: Arc<AtomicU64>,
    pub upstream_fetches_total: Arc<AtomicU64>,
    pub streaming_bypasses_total: Arc<AtomicU64>,
    pub oversized_bypasses_total: Arc<AtomicU64>,
    pub stochastic_bypasses_total: Arc<AtomicU64>,
    pub upstream_shed_total: Arc<AtomicU64>,
    pub dropped_writes_total: Arc<AtomicU64>,
    pub consecutive_write_failures: Arc<AtomicUsize>,
}

/// A stream wrapper that enforces a dual-stage timeout and holds an upstream concurrency permit.
/// 1. Long Time-To-First-Byte (TTFB) timeout (e.g. 180s) to accommodate reasoning models
///    (such as `o1-preview` or `o3-mini`) that think silently for 60-120 seconds before emitting token 1.
/// 2. Shorter inter-chunk watchdog timeout (e.g. 30s) once chunk streaming has commenced.
/// 3. Retains `_permit` until the stream concludes OR the client disconnects/aborts mid-stream,
///    preventing permit leaks while bounding active upstream streaming concurrency.
pub struct IdleTimeoutStream<S> {
    inner: S,
    ttfb_timeout: Duration,
    inter_chunk_timeout: Duration,
    has_received_first_chunk: bool,
    sleep: Pin<Box<Sleep>>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl<S> IdleTimeoutStream<S> {
    pub fn new(inner: S, ttfb_timeout: Duration, inter_chunk_timeout: Duration) -> Self {
        Self::with_permit(inner, ttfb_timeout, inter_chunk_timeout, None)
    }

    pub fn with_permit(
        inner: S,
        ttfb_timeout: Duration,
        inter_chunk_timeout: Duration,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        let sleep = Box::pin(sleep_until(Instant::now() + ttfb_timeout));
        Self {
            inner,
            ttfb_timeout,
            inter_chunk_timeout,
            has_received_first_chunk: false,
            sleep,
            _permit: permit,
        }
    }
}

impl<S, T, E> Stream for IdleTimeoutStream<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<T, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => {
                self.has_received_first_chunk = true;
                let new_deadline = Instant::now() + self.inter_chunk_timeout;
                self.sleep.as_mut().reset(new_deadline);
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(std::io::Error::other(e))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                if self.sleep.as_mut().poll(cx).is_ready() {
                    let err_msg = if self.has_received_first_chunk {
                        format!(
                            "Upstream inter-chunk timeout: {:?} elapsed with zero bytes",
                            self.inter_chunk_timeout
                        )
                    } else {
                        format!(
                            "Upstream TTFB timeout: {:?} elapsed with zero tokens received (thinking timeout)",
                            self.ttfb_timeout
                        )
                    };
                    Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        err_msg,
                    ))))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

/// Extracts a canonical multi-header credential salt.
///
/// Salting every credential header present with length-prefixed hashing prevents
/// cross-tenant collision and eliminates ambiguity (e.g. auth="ab" + api-key="c"
/// vs auth="a" + api-key="bc").
/// Each (name, value) pair is sorted and hashed with explicit length prefixes.
pub fn extract_credential_salt(
    headers: &HeaderMap,
    credential_headers: &[String],
    default_tenant_id: &str,
) -> String {
    let mut present_credentials: Vec<(String, String)> = Vec::new();

    for header_name in credential_headers {
        let name_lower = header_name.to_ascii_lowercase();
        if let Ok(name) = axum::http::HeaderName::from_bytes(name_lower.as_bytes()) {
            let mut vals: Vec<&str> = headers
                .get_all(&name)
                .iter()
                .filter_map(|v| v.to_str().ok())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            if !vals.is_empty() {
                vals.sort_unstable();
                for val in vals {
                    present_credentials.push((name_lower.clone(), val.to_string()));
                }
            }
        }
    }

    if present_credentials.is_empty() {
        return default_tenant_id.to_string();
    }

    // Sort canonically by header name, then value
    present_credentials.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    // Hash each length-prefixed (name, value) pair using BLAKE3
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"semcache_credential_salt_v2:");
    for (k, v) in present_credentials {
        hasher.update(&(k.len() as u64).to_le_bytes());
        hasher.update(k.as_bytes());
        hasher.update(&(v.len() as u64).to_le_bytes());
        hasher.update(v.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// Evaluates whether an LLM request payload is stable/conservative for replay.
///
/// A request is considered stable if an explicit `seed` parameter is provided,
/// or if `temperature` is explicitly set to `0.0`.
pub fn is_payload_stable_replay(payload: &Value) -> bool {
    if let Value::Object(map) = payload {
        if map.contains_key("seed") {
            return true;
        }
        if let Some(temp) = map.get("temperature").and_then(|v| v.as_f64()) {
            if temp.abs() < f64::EPSILON {
                return true;
            }
        }
    }
    false
}

#[inline]
pub fn is_payload_deterministic(payload: &Value) -> bool {
    is_payload_stable_replay(payload)
}

/// Health and readiness probe handler.
pub async fn handle_healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
        ],
        "{\"status\":\"ok\",\"service\":\"semcache\"}",
    )
}

/// Renders Prometheus-compatible metrics for SemCache.
pub async fn handle_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let requests = state.requests_total.load(Ordering::Relaxed);
    let l1_hits = state.l1_hits_total.load(Ordering::Relaxed);
    let coalesced_hits = state.coalesced_hits_total.load(Ordering::Relaxed);
    let upstream_fetches = state.upstream_fetches_total.load(Ordering::Relaxed);
    let streaming_bypasses = state.streaming_bypasses_total.load(Ordering::Relaxed);
    let oversized_bypasses = state.oversized_bypasses_total.load(Ordering::Relaxed);
    let stochastic_bypasses = state.stochastic_bypasses_total.load(Ordering::Relaxed);
    let upstream_shed = state.upstream_shed_total.load(Ordering::Relaxed);
    let dropped_writes = state.dropped_writes_total.load(Ordering::Relaxed);
    let write_failures = state.consecutive_write_failures.load(Ordering::Relaxed);
    let circuit_breaker_open = if write_failures >= 5 { 1 } else { 0 };
    let ready_bytes = state.coalescer.ready_bytes();
    let in_flight = state.coalescer.in_flight_count();

    let metrics_text = format!(
        "# HELP semcache_requests_total Total number of chat completion requests received\n\
         # TYPE semcache_requests_total counter\n\
         semcache_requests_total {}\n\n\
         # HELP semcache_l1_hits_total Total L1 exact cache hits served from SQLite\n\
         # TYPE semcache_l1_hits_total counter\n\
         semcache_l1_hits_total {}\n\n\
         # HELP semcache_coalesced_hits_total Total follower requests coalesced onto in-flight leader\n\
         # TYPE semcache_coalesced_hits_total counter\n\
         semcache_coalesced_hits_total {}\n\n\
         # HELP semcache_upstream_fetches_total Total requests dispatched upstream to provider\n\
         # TYPE semcache_upstream_fetches_total counter\n\
         semcache_upstream_fetches_total {}\n\n\
         # HELP semcache_streaming_bypasses_total Total requests bypassed due to stream=true\n\
         # TYPE semcache_streaming_bypasses_total counter\n\
         semcache_streaming_bypasses_total {}\n\n\
         # HELP semcache_oversized_bypasses_total Total requests bypassed due to response size limit\n\
         # TYPE semcache_oversized_bypasses_total counter\n\
         semcache_oversized_bypasses_total {}\n\n\
         # HELP semcache_stochastic_bypasses_total Total requests bypassed due to deterministic-only replay policy\n\
         # TYPE semcache_stochastic_bypasses_total counter\n\
         semcache_stochastic_bypasses_total {}\n\n\
         # HELP semcache_upstream_shed_total Total requests shed due to upstream semaphore saturation (503)\n\
         # TYPE semcache_upstream_shed_total counter\n\
         semcache_upstream_shed_total {}\n\n\
         # HELP semcache_dropped_writes_total Total SQLite writes dropped due to disk backpressure\n\
         # TYPE semcache_dropped_writes_total counter\n\
         semcache_dropped_writes_total {}\n\n\
         # HELP semcache_consecutive_write_failures Current consecutive SQLite disk write failures\n\
         # TYPE semcache_consecutive_write_failures gauge\n\
         semcache_consecutive_write_failures {}\n\n\
         # HELP semcache_circuit_breaker_open Whether the disk write circuit breaker is currently open (1) or closed (0)\n\
         # TYPE semcache_circuit_breaker_open gauge\n\
         semcache_circuit_breaker_open {}\n\n\
         # HELP semcache_ready_bytes Current bytes held in-memory across Ready states\n\
         # TYPE semcache_ready_bytes gauge\n\
         semcache_ready_bytes {}\n\n\
         # HELP semcache_in_flight_requests Current active in-flight requests in coalescer\n\
         # TYPE semcache_in_flight_requests gauge\n\
         semcache_in_flight_requests {}\n",
        requests,
        l1_hits,
        coalesced_hits,
        upstream_fetches,
        streaming_bypasses,
        oversized_bypasses,
        stochastic_bypasses,
        upstream_shed,
        dropped_writes,
        write_failures,
        circuit_breaker_open,
        ready_bytes,
        in_flight,
    );

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8"),
        ],
        metrics_text,
    )
}

/// Forwards a request directly to upstream, bypassing cache storage and coalescing.
/// Used for `Cache-Control: no-store` and for oversized responses (> max_response_bytes).
///
/// Under peak saturation where all upstream concurrency permits are held, direct re-forwarding
/// will wait up to 5s before shedding with HTTP 503 (`Retry-After: 5`), preserving upstream stability.
pub async fn forward_direct_upstream(
    state: &AppState,
    headers: &HeaderMap,
    payload_bytes: Vec<u8>,
    status_tag: &'static str,
) -> Result<Response, SemCacheError> {
    let _upstream_permit = match tokio::time::timeout(
        Duration::from_secs(5),
        state.upstream_semaphore.clone().acquire_owned(),
    )
    .await {
        Ok(Ok(permit)) => permit,
        _ => {
            state.upstream_shed_total.fetch_add(1, Ordering::Relaxed);
            return Err(SemCacheError::ConcurrencyLimitExceeded(
                "Gateway upstream concurrency saturated".into(),
            ));
        }
    };

    let mut req_builder = state
        .http_client
        .post(&state.upstream_url)
        .header(header::CONTENT_TYPE, "application/json");

    for (k, v) in headers.iter() {
        let name = k.as_str().to_ascii_lowercase();
        if state.credential_headers.iter().any(|ch| ch == &name) {
            req_builder = req_builder.header(k, v);
        }
    }

    let upstream_resp = match tokio::time::timeout(
        Duration::from_secs(state.upstream_ttfb_secs),
        req_builder.body(payload_bytes).send(),
    )
    .await
    {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => return Err(SemCacheError::from(e)),
        Err(_) => return Err(SemCacheError::UpstreamTimeout),
    };
    let status = upstream_resp.status();

    if !status.is_success() {
        let err_bytes = upstream_resp.bytes().await.unwrap_or_default();
        let err_msg = String::from_utf8_lossy(&err_bytes).into_owned();
        return Err(SemCacheError::UpstreamError(status.as_u16(), err_msg));
    }

    let resp_bytes = upstream_resp.bytes().await?;
    Ok((
        status,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::HeaderName::from_static("x-semcache-status"), status_tag),
        ],
        resp_bytes,
    )
        .into_response())
}


/// Core HTTP proxy gateway handler for LLM chat completions.
pub async fn handle_chat_completion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Response, SemCacheError> {
    let payload_bytes = serde_json::to_vec(&payload)?;

    // Increment total request counter
    state.requests_total.fetch_add(1, Ordering::Relaxed);

    // Enforce maximum inbound request body size (32MB default for multimodal vision payloads)
    if payload_bytes.len() > state.max_request_bytes {
        return Err(SemCacheError::PayloadTooLarge(format!(
            "Request payload of {} bytes exceeds configured limit of {} bytes",
            payload_bytes.len(),
            state.max_request_bytes
        )));
    }

    // Cache-Control: no-store or x-semcache-no-store bypass
    let no_store = headers
        .get(header::CACHE_CONTROL)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.contains("no-store"))
        .unwrap_or(false)
        || headers
            .get("x-semcache-no-store")
            .and_then(|h| h.to_str().ok())
            .map(|s| s == "true" || s == "1")
            .unwrap_or(false);

    if no_store {
        return forward_direct_upstream(&state, &headers, payload_bytes, "BYPASS_NO_STORE").await;
    }

    // Conservative replay policy bypass
    if state.conservative_replay && !is_payload_stable_replay(&payload) {
        state.stochastic_bypasses_total.fetch_add(1, Ordering::Relaxed);
        return forward_direct_upstream(&state, &headers, payload_bytes, "BYPASS_STOCHASTIC").await;
    }

    // Salt across all credential headers present to guarantee strict tenant isolation.
    // Callers who share an Authorization value but differ in api-key will NOT collide.
    let tenant_salt = extract_credential_salt(&headers, &state.credential_headers, &state.default_tenant_id);

    // Server-side provider configuration is authoritative (eliminating client header spoofing)
    let provider = state.default_provider;

    let canonical_res = canonicalize_and_hash(&payload_bytes, Some(&tenant_salt), provider)?;
    let hash = canonical_res.hash;
    let canonical_val = canonical_res.canonical_value;

    // Step 1: Non-Blocking Streaming Bypass with Dual-Stage TTFB/Inter-Chunk Timeout & Permit Retention
    if canonical_res.is_streaming {
        state.streaming_bypasses_total.fetch_add(1, Ordering::Relaxed);
        let upstream_permit = match tokio::time::timeout(
            Duration::from_secs(5),
            state.upstream_semaphore.clone().acquire_owned(),
        )
        .await {
            Ok(Ok(permit)) => permit,
            _ => {
                state.upstream_shed_total.fetch_add(1, Ordering::Relaxed);
                return Err(SemCacheError::ConcurrencyLimitExceeded("Gateway upstream concurrency saturated".into()));
            }
        };

        let mut req_builder = state
            .http_client
            .post(&state.upstream_url)
            .header(header::CONTENT_TYPE, "application/json");

        for (k, v) in headers.iter() {
            let name = k.as_str().to_ascii_lowercase();
            if state.credential_headers.iter().any(|ch| ch == &name) {
                req_builder = req_builder.header(k, v);
            }
        }

        let upstream_resp = req_builder.body(payload_bytes).send().await?;
        let status = upstream_resp.status();

        if !status.is_success() {
            let err_bytes = upstream_resp.bytes().await.unwrap_or_default();
            let err_msg = String::from_utf8_lossy(&err_bytes).into_owned();
            return Err(SemCacheError::UpstreamError(status.as_u16(), err_msg));
        }

        let idle_stream = IdleTimeoutStream::with_permit(
            upstream_resp.bytes_stream(),
            Duration::from_secs(state.upstream_ttfb_secs),
            Duration::from_secs(30),
            Some(upstream_permit),
        );
        let stream_body = Body::from_stream(idle_stream);
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header("x-semcache-status", "BYPASS_STREAM")
            .body(stream_body)
            .map_err(|e| SemCacheError::InternalError(e.to_string()));
    }

    // Step 2: L1 Exact Match Cache Lookup (with Fail-Open Degradation on SQLite Read Error)
    // Note: L1 hits never acquire or touch the upstream semaphore.
    let pool_clone = state.db.clone();
    let l1_hit = match tokio::task::spawn_blocking(move || get_exact_cache(&pool_clone, &hash)).await {
        Ok(Ok(cached)) => cached,
        Ok(Err(e)) => {
            tracing::warn!("SQLite L1 read error (degrading to cache miss): {}", e);
            None
        }
        Err(e) => {
            tracing::warn!("SQLite read task error (degrading to cache miss): {}", e);
            None
        }
    };

    if let Some(cached_json) = l1_hit {
        state.l1_hits_total.fetch_add(1, Ordering::Relaxed);
        return Ok((
            StatusCode::OK,
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::HeaderName::from_static("x-semcache-status"), "HIT_L1"),
            ],
            cached_json,
        )
            .into_response());
    }

    // Step 3: Single-Flight Request Coalescing
    let (mut rx, is_leader) = match state.coalescer.register_or_wait(hash).await {
        Ok(CoalesceResult::Coalesced(coalesced_bytes)) => {
            state.coalesced_hits_total.fetch_add(1, Ordering::Relaxed);
            return Ok((
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/json"),
                    (header::HeaderName::from_static("x-semcache-status"), "HIT_COALESCED"),
                ],
                coalesced_bytes,
            )
                .into_response());
        }
        Ok(CoalesceResult::Primary(mut guard, leader_rx)) => {
            state.upstream_fetches_total.fetch_add(1, Ordering::Relaxed);
            let http_client = state.http_client.clone();
            let upstream_url = state.upstream_url.clone();
            let db_pool = state.db.clone();
            let mut forwarded_headers: Vec<(header::HeaderName, header::HeaderValue)> = Vec::new();
            for (k, v) in headers.iter() {
                let name = k.as_str().to_ascii_lowercase();
                if state.credential_headers.iter().any(|ch| ch == &name) {
                    forwarded_headers.push((k.clone(), v.clone()));
                }
            }
            let payload_bytes_clone = payload_bytes.clone();
            let canonical_val_clone = canonical_val.clone();
            let write_semaphore = state.sqlite_write_semaphore.clone();
            let upstream_semaphore = state.upstream_semaphore.clone();
            let max_response_bytes = state.max_response_bytes;
            let cancel_orphan_requests = state.cancel_orphan_requests;
            let upstream_shed_total = state.upstream_shed_total.clone();
            let dropped_writes_total = state.dropped_writes_total.clone();
            let consecutive_write_failures = state.consecutive_write_failures.clone();

            tokio::spawn(async move {
                // Post-Dispatch Ghost Task Policy:
                // If the initiating client dropped connection before upstream dispatch and zero followers wait,
                // abort to save tokens if cancel_orphan_requests is enabled.
                if cancel_orphan_requests && !guard.has_active_listeners() {
                    tracing::info!("Ghost task aborted: 0 active listeners before upstream fetch.");
                    guard.evict();
                    return;
                }

                // Acquire upstream concurrency permit
                let _upstream_permit = match tokio::time::timeout(
                    Duration::from_secs(5),
                    upstream_semaphore.acquire_owned(),
                ).await {
                    Ok(Ok(permit)) => permit,
                    _ => {
                        upstream_shed_total.fetch_add(1, Ordering::Relaxed);
                        guard.broadcast_error(SemCacheError::ConcurrencyLimitExceeded(
                            "Gateway upstream concurrency saturated".into(),
                        ));
                        return;
                    }
                };

                let mut req_builder = http_client
                    .post(&upstream_url)
                    .header(header::CONTENT_TYPE, "application/json");

                for (k, v) in forwarded_headers {
                    req_builder = req_builder.header(k, v);
                }

                let upstream_resp = match tokio::time::timeout(
                    Duration::from_secs(state.upstream_ttfb_secs),
                    req_builder.body(payload_bytes_clone).send(),
                ).await {
                    Ok(Ok(resp)) => resp,
                    Ok(Err(e)) => {
                        guard.broadcast_error(SemCacheError::from(e));
                        return;
                    }
                    Err(_) => {
                        guard.broadcast_error(SemCacheError::UpstreamTimeout);
                        return;
                    }
                };

                let status = upstream_resp.status();
                if !status.is_success() {
                    let err_bytes = upstream_resp.bytes().await.unwrap_or_default();
                    let err_msg = String::from_utf8_lossy(&err_bytes).into_owned();
                    guard.broadcast_error(SemCacheError::UpstreamError(status.as_u16(), err_msg));
                    return;
                }

                let resp_no_store = upstream_resp
                    .headers()
                    .get(header::CACHE_CONTROL)
                    .and_then(|h| h.to_str().ok())
                    .map(|s| s.contains("no-store"))
                    .unwrap_or(false);

                // Enforce response size while streaming chunks to avoid buffering oversized bodies into RAM
                let content_length = upstream_resp.content_length();
                if content_length.is_some_and(|len| len > max_response_bytes as u64) {
                    tracing::warn!(
                        "Upstream Content-Length ({} bytes) exceeds limit ({} bytes); signaling bypass to followers",
                        content_length.unwrap_or(0),
                        max_response_bytes
                    );
                    guard.broadcast_error(SemCacheError::UpstreamOversizedBypass);
                    return;
                }

                let mut stream = upstream_resp.bytes_stream();
                let mut accumulated_bytes = Vec::new();

                while let Some(chunk_res) = stream.next().await {
                    if cancel_orphan_requests && !guard.has_active_listeners() {
                        tracing::info!("Ghost task aborted during streaming: 0 active listeners remaining.");
                        guard.evict();
                        return;
                    }

                    let chunk = match chunk_res {
                        Ok(c) => c,
                        Err(e) => {
                            guard.broadcast_error(SemCacheError::from(e));
                            return;
                        }
                    };

                    if accumulated_bytes.len() + chunk.len() > max_response_bytes {
                        tracing::warn!(
                            "Response chunk stream exceeded limit ({} bytes); signaling bypass to followers",
                            max_response_bytes
                        );
                        guard.broadcast_error(SemCacheError::UpstreamOversizedBypass);
                        return;
                    }
                    accumulated_bytes.extend_from_slice(&chunk);
                }

                let resp_bytes = Bytes::from(accumulated_bytes);

                // 1. Mark state in InFlightMap as Ready(bytes, timestamp) & broadcast to current subscribers
                guard.mark_ready_and_broadcast(resp_bytes.clone());

                // RFC 7234: If upstream returns Cache-Control: no-store, do not persist to disk
                if resp_no_store {
                    tracing::debug!("Upstream response contains Cache-Control: no-store; skipping persistence and evicting immediately");
                    guard.evict();
                    return;
                }

                // 2. Persist to SQLite WAL with bounded semaphore backpressure and consecutive failure circuit breaker
                let model = canonical_val_clone
                    .get("model")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let req_json_str = canonical_val_clone.to_string();
                let resp_json_str = String::from_utf8_lossy(&resp_bytes).into_owned();

                let permit_res = tokio::time::timeout(
                    Duration::from_millis(250),
                    write_semaphore.acquire_owned(),
                )
                .await;

                match permit_res {
                    Ok(Ok(permit)) => {
                        let write_handle = tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            insert_exact_cache(
                                &db_pool,
                                &hash,
                                &model,
                                &req_json_str,
                                &resp_json_str,
                            )
                        });

                        match write_handle.await {
                            Ok(Ok(())) => {
                                // SQLite write committed successfully: reset circuit breaker counter and evict from Ready RAM
                                consecutive_write_failures.store(0, Ordering::Relaxed);
                                guard.evict();
                            }
                            Ok(Err(e)) => {
                                dropped_writes_total.fetch_add(1, Ordering::Relaxed);
                                let failures = consecutive_write_failures.fetch_add(1, Ordering::Relaxed) + 1;
                                if failures >= 5 {
                                    tracing::warn!("SQLite write failure circuit breaker tripped ({} consecutive failures); evicting from Ready RAM", failures);
                                    guard.evict();
                                } else {
                                    tracing::warn!("SQLite write failed: {}; retaining entry in Ready RAM cache (failure {}/5)", e, failures);
                                }
                            }
                            Err(e) => {
                                dropped_writes_total.fetch_add(1, Ordering::Relaxed);
                                let failures = consecutive_write_failures.fetch_add(1, Ordering::Relaxed) + 1;
                                if failures >= 5 {
                                    tracing::warn!("SQLite write task failure circuit breaker tripped ({} consecutive failures); evicting from Ready RAM", failures);
                                    guard.evict();
                                } else {
                                    tracing::warn!("SQLite write task error: {}; retaining entry in Ready RAM cache (failure {}/5)", e, failures);
                                }
                            }
                        }
                    }
                    _ => {
                        // Queue backpressure / timeout acquiring write permit under load.
                        // CRITICAL: Do NOT increment consecutive_write_failures here!
                        // Saturated write semaphore during a burst on a healthy disk must not trip the circuit breaker.
                        dropped_writes_total.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("SQLite write backpressure saturated; dropping async disk write while retaining entry in Ready RAM cache");
                    }
                }
            });

            (leader_rx, true)
        }
        Err(SemCacheError::UpstreamOversizedBypass) => {
            state.oversized_bypasses_total.fetch_add(1, Ordering::Relaxed);
            return forward_direct_upstream(&state, &headers, payload_bytes, "BYPASS_OVERSIZED").await;
        }
        Err(e) => return Err(e),
    };

    let resp_bytes = match rx.recv().await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => {
            if let SemCacheError::UpstreamOversizedBypass = err.as_ref() {
                state.oversized_bypasses_total.fetch_add(1, Ordering::Relaxed);
                return forward_direct_upstream(&state, &headers, payload_bytes, "BYPASS_OVERSIZED").await;
            }
            return Err((*err).clone());
        }
        Err(_) => {
            return Err(SemCacheError::UpstreamError(
                502,
                "In-flight primary worker channel closed unexpectedly".to_string(),
            ));
        }
    };

    let status_header = if is_leader {
        "MISS_UPSTREAM"
    } else {
        "HIT_COALESCED"
    };

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::HeaderName::from_static("x-semcache-status"), status_header),
        ],
        resp_bytes,
    )
        .into_response())
}

