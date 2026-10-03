use axum::{
    body::Body,
    extract::{Json, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::Stream;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{sleep_until, Instant, Sleep};

use crate::{
    canonical::canonicalize_and_hash,
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
}

/// A stream wrapper that enforces an idle timeout between consecutive chunks.
///
/// CRITICAL FIX (The Guillotine Eradication):
/// As long as chunks are actively received within `timeout_duration`, the stream
/// can run indefinitely (supporting reasoning models and multi-megabyte code streams).
/// If the upstream goes silent for `timeout_duration`, the stream terminates cleanly.
pub struct IdleTimeoutStream<S> {
    inner: S,
    timeout_duration: Duration,
    sleep: Pin<Box<Sleep>>,
}

impl<S> IdleTimeoutStream<S> {
    pub fn new(inner: S, timeout_duration: Duration) -> Self {
        let sleep = Box::pin(sleep_until(Instant::now() + timeout_duration));
        Self {
            inner,
            timeout_duration,
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
                let new_deadline = Instant::now() + self.timeout_duration;
                self.sleep.as_mut().reset(new_deadline);
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(std::io::Error::other(e))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                if self.sleep.as_mut().poll(cx).is_ready() {
                    Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Upstream chunk idle timeout: 30s elapsed with zero bytes",
                    ))))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

/// Core HTTP proxy gateway handler for OpenAI chat completions.
pub async fn handle_chat_completion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Response, SemCacheError> {
    let payload_bytes = serde_json::to_vec(&payload)?;

    let auth_str = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    // Step 1: Canonicalization & BLAKE3 Hashing (with tenant isolation & default normalization)
    let canonical_res = canonicalize_and_hash(&payload_bytes, auth_str)?;
    let hash = canonical_res.hash;
    let canonical_val = canonical_res.canonical_value;

    // Step 1.1: Non-Blocking Streaming Bypass with Per-Chunk Idle Timeout
    if canonical_res.is_streaming {
        let mut req_builder = state
            .http_client
            .post(&state.upstream_url)
            .header(header::CONTENT_TYPE, "application/json");

        if let Some(auth_val) = headers.get(header::AUTHORIZATION) {
            req_builder = req_builder.header(header::AUTHORIZATION, auth_val);
        }

        let upstream_resp = req_builder.body(payload_bytes).send().await?;
        let status = upstream_resp.status();

        if !status.is_success() {
            let err_bytes = upstream_resp.bytes().await?;
            let err_msg = String::from_utf8_lossy(&err_bytes).into_owned();
            return Err(SemCacheError::UpstreamError(status.as_u16(), err_msg));
        }

        let idle_stream = IdleTimeoutStream::new(upstream_resp.bytes_stream(), Duration::from_secs(30));
        let stream_body = Body::from_stream(idle_stream);
        return Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header("x-semcache-status", "BYPASS_STREAM")
            .body(stream_body)
            .map_err(|e| SemCacheError::InternalError(e.to_string()));
    }

    // Step 2: L1 Exact Match Cache Lookup
    let pool_clone = state.db.clone();
    let l1_hit = tokio::task::spawn_blocking(move || get_exact_cache(&pool_clone, &hash))
        .await
        .map_err(|e| SemCacheError::InternalError(format!("Task spawn error: {e}")))??;

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
        CoalesceResult::Primary(mut guard) => {
            let rx = guard.tx.subscribe();
            let http_client = state.http_client.clone();
            let upstream_url = state.upstream_url.clone();
            let db_pool = state.db.clone();
            let auth_header_opt = auth_str.map(|s| s.to_string());
            let payload_bytes_clone = payload_bytes.clone();
            let canonical_val_clone = canonical_val.clone();

            tokio::spawn(async move {
                // CRITICAL FIX (Ghost Tasks):
                // If the initiating client disconnected before upstream dispatch and no followers joined,
                // abort immediately to avoid burning upstream OpenAI tokens!
                if !guard.has_active_listeners() {
                    tracing::info!("Ghost task aborted: 0 active listeners before upstream fetch.");
                    guard.evict();
                    return;
                }

                let mut req_builder = http_client
                    .post(&upstream_url)
                    .header(header::CONTENT_TYPE, "application/json");

                if let Some(auth_val) = auth_header_opt {
                    req_builder = req_builder.header(header::AUTHORIZATION, auth_val);
                }

                let upstream_resp = match req_builder.body(payload_bytes_clone).send().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        guard.broadcast_error(SemCacheError::from(e));
                        return;
                    }
                };

                let status = upstream_resp.status();
                let resp_bytes = match upstream_resp.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        guard.broadcast_error(SemCacheError::from(e));
                        return;
                    }
                };

                if !status.is_success() {
                    let err_msg = String::from_utf8_lossy(&resp_bytes).into_owned();
                    guard.broadcast_error(SemCacheError::UpstreamError(status.as_u16(), err_msg));
                    return;
                }

                // 1. Mark state in InFlightMap as Ready(bytes, timestamp) & broadcast to current subscribers
                guard.mark_ready_and_broadcast(resp_bytes.clone());

                // 2. Persist to SQLite WAL with strict 5-second timeout safety
                let model = canonical_val_clone
                    .get("model")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let req_json_str = canonical_val_clone.to_string();
                let resp_json_str = String::from_utf8_lossy(&resp_bytes).into_owned();

                let write_res = tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio::task::spawn_blocking(move || {
                        insert_exact_cache(
                            &db_pool,
                            &hash,
                            &model,
                            &req_json_str,
                            &resp_json_str,
                        )
                    }),
                )
                .await;

                if write_res.is_err() {
                    tracing::error!("SQLite write timed out after 5s; evicting from memory to prevent leak.");
                }

                // 3. Evict from memory once SQLite write commits or times out
                guard.evict();
            });

            (rx, true)
        }
    };

    let resp_bytes = match rx.recv().await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(err)) => return Err((*err).clone()),
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
