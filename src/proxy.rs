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
/// Implements:
/// - Transparent Streaming Bypass (Flaw #11)
/// - Multi-Tenant Authorization Isolation (Flaw #4)
/// - Syntax-Preserving Canonicalization (Flaw #3)
/// - Atomic Single-Flight Coalescing (Flaws #1, #2, #6)
/// - Zero Error Cache Poisoning (Flaw #5)
pub async fn handle_chat_completion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Response, SemCacheError> {
    let payload_bytes = serde_json::to_vec(&payload)?;

    // Extract authorization header for tenant-isolated caching
    let auth_str = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    // Step 1: Canonicalization & BLAKE3 Hashing (with tenant isolation)
    let canonical_res = canonicalize_and_hash(&payload_bytes, auth_str)?;
    let hash = canonical_res.hash;
    let canonical_val = canonical_res.canonical_value;

    // Step 1.1: CRITICAL FIX (Flaw #11 - Non-Blocking Streaming Bypass)
    // If client requested streaming, bypass cache & coalescing to stream raw SSE chunks back.
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

    // Step 3: Single-Flight Request Coalescing (Atomic & Race-Free)
    let leader_guard = match state.coalescer.register_or_wait(hash).await? {
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
        CoalesceResult::Primary(guard) => guard,
    };

    // Step 4: Upstream Forwarding
    let mut req_builder = state
        .http_client
        .post(&state.upstream_url)
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(auth_val) = headers.get(header::AUTHORIZATION) {
        req_builder = req_builder.header(header::AUTHORIZATION, auth_val);
    }

    let upstream_resp = match req_builder.body(payload_bytes).send().await {
        Ok(resp) => resp,
        Err(e) => {
            let err = SemCacheError::from(e);
            leader_guard.broadcast_error(err.clone());
            return Err(err);
        }
    };

    let status = upstream_resp.status();
    let resp_bytes = match upstream_resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            let err = SemCacheError::from(e);
            leader_guard.broadcast_error(err.clone());
            return Err(err);
        }
    };

    // Step 5: CRITICAL FIX (Flaw #5 - Never Poison Cache With Errors)
    if !status.is_success() {
        let err_msg = String::from_utf8_lossy(&resp_bytes).into_owned();
        let upstream_err = SemCacheError::UpstreamError(status.as_u16(), err_msg);
        // Broadcast the failure to any awaiting followers so they fail immediately
        leader_guard.broadcast_error(upstream_err.clone());
        return Err(upstream_err);
    }

    // Step 6: Asynchronous Persistence to SQLite WAL (Only 200 OK responses)
    let pool_persist = state.db.clone();
    let model = canonical_val
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("unknown")
        .to_string();
    let req_json_str = canonical_val.to_string();
    let resp_json_str = String::from_utf8_lossy(&resp_bytes).into_owned();

    tokio::task::spawn_blocking(move || {
        if let Err(e) = insert_exact_cache(
            &pool_persist,
            &hash,
            &model,
            &req_json_str,
            &resp_json_str,
        ) {
            tracing::error!("Failed to persist exact cache record: {}", e);
        }
    });

    // Step 7: CRITICAL FIX (Flaw #6): Remove-First Broadcast to followers
    leader_guard.broadcast_success(resp_bytes.clone());

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::HeaderName::from_static("x-semcache-status"), "MISS_UPSTREAM"),
        ],
        resp_bytes,
    )
        .into_response())
}
