use axum::{
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
/// Normalizes the request, checks L1 cache, coalesces concurrent identical requests,
/// fetches upstream, asynchronously persists cache, and unblocks in-flight receivers.
pub async fn handle_chat_completion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<Response, SemCacheError> {
    let payload_bytes = serde_json::to_vec(&payload)?;

    // Step 1: Canonicalization & BLAKE3 Hashing
    let (hash, canonical_val) = canonicalize_and_hash(&payload_bytes)?;

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

    // Step 4 (L2 Skip for MVP):
    // =========================================================================
    // PHASE 2 (L2 FUZZY CACHE HOOK):
    // 1. Asynchronously fetch embedding vector for canonical prompt via text-embedding-3-small.
    // 2. Query sqlite-vec virtual table:
    //    SELECT id, distance FROM fuzzy_cache WHERE embedding MATCH ?1 AND distance <= 0.08
    //    ORDER BY distance LIMIT 1;
    // 3. If match found (cosine similarity >= 0.92), fetch response from fuzzy_payloads and return.
    // =========================================================================

    // Step 5: Upstream Forwarding
    let mut req_builder = state
        .http_client
        .post(&state.upstream_url)
        .header(header::CONTENT_TYPE, "application/json");

    if let Some(auth_val) = headers.get(header::AUTHORIZATION) {
        req_builder = req_builder.header(header::AUTHORIZATION, auth_val);
    }

    let upstream_resp = req_builder.body(payload_bytes).send().await?;
    let status = upstream_resp.status();
    let resp_bytes = upstream_resp.bytes().await?;

    if !status.is_success() {
        // Crucial: Do not cache error responses or rate limit failures
        let err_msg = String::from_utf8_lossy(&resp_bytes).into_owned();
        drop(leader_guard);
        return Err(SemCacheError::UpstreamError(status.as_u16(), err_msg));
    }

    // Step 6: Asynchronous Persistence to SQLite WAL
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

    // Step 7: Broadcast result to any pending coalesced subscribers
    leader_guard.broadcast(resp_bytes.clone());

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
