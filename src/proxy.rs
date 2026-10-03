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
    pub upstream_shed_total: Arc<AtomicU64>,
    pub dropped_writes_total: Arc<AtomicU64>,
    pub consecutive_write_failures: Arc<AtomicUsize>,
}

/// A stream wrapper that enforces a dual-stage timeout:
/// 1. Long Time-To-First-Byte (TTFB) timeout (e.g. 180s) to accommodate reasoning models
///    (such as `o1-preview` or `o3-mini`) that think silently for 60-120 seconds before emitting token 1.
/// 2. Shorter inter-chunk watchdog timeout (e.g. 30s) once chunk streaming has commenced.
pub struct IdleTimeoutStream<S> {
    inner: S,
    ttfb_timeout: Duration,
    inter_chunk_timeout: Duration,
    has_received_first_chunk: bool,
    sleep: Pin<Box<Sleep>>,
}

impl<S> IdleTimeoutStream<S> {
    pub fn new(inner: S, ttfb_timeout: Duration, inter_chunk_timeout: Duration) -> Self {
        let sleep = Box::pin(sleep_until(Instant::now() + ttfb_timeout));
        Self {
            inner,
            ttfb_timeout,
            inter_chunk_timeout,
            has_received_first_chunk: false,
            sleep,
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

/// Forwards a request directly to upstream, bypassing cache storage and coalescing.
/// Used for `Cache-Control: no-store` and for oversized responses (> max_response_bytes).
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

    if let Some(auth_val) = headers.get(header::AUTHORIZATION) {
        req_builder = req_builder.header(header::AUTHORIZATION, auth_val);
    }
    if let Some(key_val) = headers.get("api-key") {
        req_builder = req_builder.header("api-key", key_val);
    }
    if let Some(x_key_val) = headers.get("x-api-key") {
        req_builder = req_builder.header("x-api-key", x_key_val);
    }

    let upstream_resp = req_builder.body(payload_bytes).send().await?;
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

    // Salt across credential headers with First-Match Precedence (Authorization > api-key > x-api-key).
    // If no credential header is provided, namespace under server default tenant ID.
    let auth_str = headers
        .get(header::AUTHORIZATION)
        .or_else(|| headers.get("api-key"))
        .or_else(|| headers.get("x-api-key"))
        .and_then(|h| h.to_str().ok());

    let tenant_salt = auth_str.unwrap_or(&state.default_tenant_id);

    // Server-side provider configuration is authoritative (eliminating client header spoofing)
    let provider = state.default_provider;

    let canonical_res = canonicalize_and_hash(&payload_bytes, Some(tenant_salt), provider)?;
    let hash = canonical_res.hash;
    let canonical_val = canonical_res.canonical_value;

    // Step 1: Non-Blocking Streaming Bypass with Dual-Stage TTFB/Inter-Chunk Timeout
    if canonical_res.is_streaming {
        let _upstream_permit = match tokio::time::timeout(
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

        if let Some(auth_val) = headers.get(header::AUTHORIZATION) {
            req_builder = req_builder.header(header::AUTHORIZATION, auth_val);
        }
        if let Some(key_val) = headers.get("api-key") {
            req_builder = req_builder.header("api-key", key_val);
        }
        if let Some(x_key_val) = headers.get("x-api-key") {
            req_builder = req_builder.header("x-api-key", x_key_val);
        }

        let upstream_resp = req_builder.body(payload_bytes).send().await?;
        let status = upstream_resp.status();

        if !status.is_success() {
            let err_bytes = upstream_resp.bytes().await.unwrap_or_default();
            let err_msg = String::from_utf8_lossy(&err_bytes).into_owned();
            return Err(SemCacheError::UpstreamError(status.as_u16(), err_msg));
        }

        let idle_stream = IdleTimeoutStream::new(
            upstream_resp.bytes_stream(),
            Duration::from_secs(180),
            Duration::from_secs(30),
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
    let (mut rx, is_leader) = match state.coalescer.register_or_wait(hash).await? {
        CoalesceResult::Coalesced(coalesced_bytes) => {
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
        CoalesceResult::Primary(mut guard, leader_rx) => {
            let http_client = state.http_client.clone();
            let upstream_url = state.upstream_url.clone();
            let db_pool = state.db.clone();
            let auth_header_opt = headers.get(header::AUTHORIZATION).and_then(|h| h.to_str().ok()).map(|s| s.to_string());
            let api_key_opt = headers.get("api-key").and_then(|h| h.to_str().ok()).map(|s| s.to_string());
            let x_api_key_opt = headers.get("x-api-key").and_then(|h| h.to_str().ok()).map(|s| s.to_string());
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

                if let Some(auth_val) = auth_header_opt {
                    req_builder = req_builder.header(header::AUTHORIZATION, auth_val);
                }
                if let Some(key_val) = api_key_opt {
                    req_builder = req_builder.header("api-key", key_val);
                }
                if let Some(x_key_val) = x_api_key_opt {
                    req_builder = req_builder.header("x-api-key", x_key_val);
                }

                let upstream_resp = match req_builder.body(payload_bytes_clone).send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        guard.broadcast_error(SemCacheError::from(e));
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
                                // SQLite write committed successfully: reset circuit breaker counter and evict
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
                        dropped_writes_total.fetch_add(1, Ordering::Relaxed);
                        let failures = consecutive_write_failures.fetch_add(1, Ordering::Relaxed) + 1;
                        if failures >= 5 {
                            tracing::warn!("SQLite write backpressure circuit breaker tripped ({} consecutive failures); evicting from Ready RAM", failures);
                            guard.evict();
                        } else {
                            tracing::warn!("SQLite write backpressure saturated; retaining entry in Ready RAM cache (failure {}/5)", failures);
                        }
                    }
                }
            });

            (leader_rx, true)
        }
    };

    let resp_bytes = match rx.recv().await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => {
            if let SemCacheError::UpstreamOversizedBypass = err.as_ref() {
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
