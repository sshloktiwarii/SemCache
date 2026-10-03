use axum::{
    body::Body,
    extract::{Json, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;

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

/// Core HTTP proxy gateway handler for OpenAI chat completions.
///
/// Production Hardened:
/// - Leader Cancellation Immunity: Upstream fetch is detached in tokio::spawn
/// - Persistence Gap Closed: Memory buffer transitions to Ready until SQLite commits
/// - Multi-Tenant Authorization Isolation: Auth token salted into BLAKE3
/// - Syntax-Preserving Canonicalization: Indentation & newlines preserved
/// - Zero Error Cache Poisoning: Only HTTP 200 persisted
/// - Non-Blocking Streaming Bypass: Direct SSE streaming for stream: true
pub async fn handle_chat_completion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Response, SemCacheError> {
    let payload_bytes = serde_json::to_vec(&payload)?;

    let auth_str = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    // Step 1: Canonicalization & BLAKE3 Hashing (with tenant isolation)
    let canonical_res = canonicalize_and_hash(&payload_bytes, auth_str)?;
    let hash = canonical_res.hash;
    let canonical_val = canonical_res.canonical_value;

    // Step 1.1: Non-Blocking Streaming Bypass
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

        let stream_body = Body::from_stream(upstream_resp.bytes_stream());
        return Ok(Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header("x-semcache-status", "BYPASS_STREAM")
            .body(stream_body)
            .map_err(|e| SemCacheError::InternalError(e.to_string()))?);
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

            // CRITICAL FIX (Leader Cancellation Immunity):
            // Spawn the upstream fetch in a detached task. If this specific client disconnects,
            // the background fetch continues, completes, persists to SQLite, and serves all followers.
            tokio::spawn(async move {
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

                // 1. Mark state in InFlightMap as Ready(bytes) & broadcast to current subscribers
                guard.mark_ready_and_broadcast(resp_bytes.clone());

                // 2. Persist to SQLite WAL asynchronously
                let model = canonical_val_clone
                    .get("model")
                    .and_then(|m| m.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let req_json_str = canonical_val_clone.to_string();
                let resp_json_str = String::from_utf8_lossy(&resp_bytes).into_owned();

                let _ = tokio::task::spawn_blocking(move || {
                    if let Err(e) = insert_exact_cache(
                        &db_pool,
                        &hash,
                        &model,
                        &req_json_str,
                        &resp_json_str,
                    ) {
                        tracing::error!("Failed to persist exact cache record: {}", e);
                    }
                })
                .await;

                // 3. Evict from memory once SQLite write commits
                guard.evict();
            });

            (rx, true)
        }
    };

    // Await response from the detached worker
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
