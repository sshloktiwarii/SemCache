use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SemCacheError {
    #[error("Database error: {0}")]
    DbError(#[from] rusqlite::Error),

    #[error("Database connection pool error: {0}")]
    PoolError(#[from] r2d2::Error),

    #[error("Upstream HTTP client error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Streaming requests (stream: true) are not supported in MVP")]
    StreamingNotSupported,

    #[error("Upstream provider returned status {0}: {1}")]
    UpstreamError(u16, String),

    #[error("Internal gateway error: {0}")]
    InternalError(String),
}

impl IntoResponse for SemCacheError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self {
            SemCacheError::StreamingNotSupported => (StatusCode::BAD_REQUEST, "invalid_request_error"),
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
