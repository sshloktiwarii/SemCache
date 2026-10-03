use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum SemCacheError {
    #[error("Database error: {0}")]
    DbError(String),

    #[error("Database connection pool error: {0}")]
    PoolError(String),

    #[error("Upstream HTTP client error: {0}")]
    HttpError(String),

    #[error("JSON serialization/deserialization error: {0}")]
    JsonError(String),

    #[error("Request payload too large: {0}")]
    PayloadTooLarge(String),

    #[error("Upstream concurrency limit exceeded: {0}")]
    ConcurrencyLimitExceeded(String),

    #[error("Upstream response oversized; bypass coalescing")]
    UpstreamOversizedBypass,

    #[error("Upstream provider returned status {0}: {1}")]
    UpstreamError(u16, String),

    #[error("Internal gateway error: {0}")]
    InternalError(String),
}

impl From<rusqlite::Error> for SemCacheError {
    fn from(e: rusqlite::Error) -> Self {
        SemCacheError::DbError(e.to_string())
    }
}

impl From<r2d2::Error> for SemCacheError {
    fn from(e: r2d2::Error) -> Self {
        SemCacheError::PoolError(e.to_string())
    }
}

impl From<reqwest::Error> for SemCacheError {
    fn from(e: reqwest::Error) -> Self {
        SemCacheError::HttpError(e.to_string())
    }
}

impl From<serde_json::Error> for SemCacheError {
    fn from(e: serde_json::Error) -> Self {
        SemCacheError::JsonError(e.to_string())
    }
}

impl IntoResponse for SemCacheError {
    fn into_response(self) -> Response {
        if let SemCacheError::ConcurrencyLimitExceeded(ref msg) = self {
            let body = Json(json!({
                "error": {
                    "message": msg,
                    "type": "concurrency_limit_exceeded"
                }
            }));
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [
                    (axum::http::header::RETRY_AFTER, "5"),
                    (axum::http::header::CONTENT_TYPE, "application/json"),
                ],
                body,
            )
                .into_response();
        }

        let (status, error_type) = match &self {
            SemCacheError::PayloadTooLarge(_) => (StatusCode::PAYLOAD_TOO_LARGE, "invalid_request_error"),
            SemCacheError::ConcurrencyLimitExceeded(_) => unreachable!(),
            SemCacheError::UpstreamOversizedBypass => (StatusCode::BAD_GATEWAY, "upstream_oversized"),
            SemCacheError::UpstreamError(code, _) => {
                let status = StatusCode::from_u16(*code).unwrap_or(StatusCode::BAD_GATEWAY);
                (status, "upstream_error")
            }
            SemCacheError::JsonError(_) => (StatusCode::BAD_REQUEST, "invalid_json_error"),
            SemCacheError::DbError(_) | SemCacheError::PoolError(_) | SemCacheError::HttpError(_) | SemCacheError::InternalError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "gateway_error")
            }
        };

        let body = Json(json!({
            "error": {
                "message": self.to_string(),
                "type": error_type
            }
        }));

        (status, body).into_response()
    }
}
