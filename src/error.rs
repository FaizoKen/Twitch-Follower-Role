use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Twitch API error: {0}")]
    TwitchApi(String),

    #[error("RoleLogic API error: {0}")]
    RoleLogic(String),

    #[error("Role link not found on RoleLogic")]
    RoleLinkNotFound,

    #[error("Role link is disabled on RoleLogic")]
    RoleLinkDisabled,

    #[error("Role link user limit reached ({limit})")]
    UserLimitReached { limit: usize },

    #[error("Invalid request: {0}")]
    BadRequest(String),

    #[error("Unauthorized")]
    Unauthorized,

    #[error("{0}")]
    UnauthorizedWith(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Configuration changed in another tab")]
    StaleVersion,

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AppError::Database(e) => {
                tracing::error!("Database error: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            }
            AppError::TwitchApi(e) => {
                tracing::error!("Twitch API error: {e}");
                (StatusCode::BAD_GATEWAY, "Failed to fetch Twitch data")
            }
            AppError::RoleLogic(e) => {
                tracing::error!("RoleLogic API error: {e}");
                (StatusCode::BAD_GATEWAY, "Failed to sync roles")
            }
            AppError::RoleLinkNotFound => (StatusCode::NOT_FOUND, "Role link not found"),
            AppError::RoleLinkDisabled => (StatusCode::FORBIDDEN, "Role link is disabled"),
            AppError::UserLimitReached { limit } => {
                tracing::warn!("Role link user limit reached: {limit}");
                (StatusCode::FORBIDDEN, "Role link user limit reached")
            }
            AppError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.as_str()),
            AppError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, "Invalid or missing authorization")
            }
            AppError::UnauthorizedWith(msg) => (StatusCode::UNAUTHORIZED, msg.as_str()),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.as_str()),
            AppError::StaleVersion => (
                StatusCode::CONFLICT,
                "This configuration changed in another tab. Reload to get the latest, then re-apply.",
            ),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.as_str()),
            AppError::Internal(e) => {
                tracing::error!("Internal error: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error")
            }
        };

        let body = json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}
