use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::models::condition::TwitchConditions;
use crate::schema;
use crate::services::sync::ConfigSyncEvent;
use crate::AppState;

fn extract_token(headers: &HeaderMap) -> Result<String, AppError> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(AppError::Unauthorized)?;

    let token = auth.strip_prefix("Token ").ok_or(AppError::Unauthorized)?;
    Ok(token.to_string())
}

#[derive(Deserialize)]
pub struct RegisterBody {
    pub guild_id: String,
    pub role_id: String,
}

pub async fn register(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<RegisterBody>,
) -> Result<Json<Value>, AppError> {
    let token = extract_token(&headers)?;

    sqlx::query(
        "INSERT INTO role_links (guild_id, role_id, api_token) VALUES ($1, $2, $3) \
         ON CONFLICT (guild_id, role_id) DO UPDATE SET api_token = $3, updated_at = now()",
    )
    .bind(&body.guild_id)
    .bind(&body.role_id)
    .bind(&token)
    .execute(&state.pool)
    .await?;

    tracing::info!(
        guild_id = body.guild_id,
        role_id = body.role_id,
        "Role link registered"
    );

    Ok(Json(serde_json::json!({"success": true})))
}

pub async fn get_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    let token = extract_token(&headers)?;

    let link = sqlx::query_as::<_, (String, String, sqlx::types::Json<TwitchConditions>, Option<String>, Option<String>)>(
        "SELECT guild_id, role_id, conditions, broadcaster_id, broadcaster_login \
         FROM role_links WHERE api_token = $1",
    )
    .bind(&token)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::Unauthorized)?;

    let (guild_id, role_id, conditions, broadcaster_id, broadcaster_login) = link;

    // Per-guild verify URL. The `?guild=<id>` query param is what the
    // verify page reads to (a) show "Verifying for <Server>" context and
    // (b) auto-clear any existing opt-out so users who previously
    // disabled this server are re-enrolled in one click — no detour
    // through /auth/my_servers, no re-verifying.
    //
    // Guild IDs are Discord snowflakes (digits only) so they're safe to
    // splice directly into the query string without percent-encoding.
    let verify_url = format!("{}/verify?guild={}", state.config.base_url, guild_id);
    let connect_url = format!(
        "{}/connect?guild_id={}&role_id={}",
        state.config.base_url, guild_id, role_id
    );

    let broadcaster_info = match (&broadcaster_id, &broadcaster_login) {
        (Some(id), Some(login)) => Some((id.as_str(), login.as_str())),
        _ => None,
    };

    let schema_json =
        schema::build_config_schema(&conditions, broadcaster_info, &verify_url, &connect_url);

    Ok(Json(schema_json))
}

#[derive(Deserialize)]
pub struct ConfigBody {
    pub guild_id: String,
    pub role_id: String,
    pub config: HashMap<String, Value>,
}

pub async fn post_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<ConfigBody>,
) -> Result<Json<Value>, AppError> {
    let token = extract_token(&headers)?;

    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM role_links WHERE guild_id = $1 AND role_id = $2 AND api_token = $3)",
    )
    .bind(&body.guild_id)
    .bind(&body.role_id)
    .bind(&token)
    .fetch_one(&state.pool)
    .await
    .unwrap_or(false);

    if !exists {
        return Err(AppError::Unauthorized);
    }

    let conditions = schema::parse_config(&body.config)?;

    sqlx::query(
        "UPDATE role_links SET conditions = $1, updated_at = now() WHERE guild_id = $2 AND role_id = $3",
    )
    .bind(sqlx::types::Json(&conditions))
    .bind(&body.guild_id)
    .bind(&body.role_id)
    .execute(&state.pool)
    .await?;

    tracing::info!(
        guild_id = body.guild_id,
        role_id = body.role_id,
        "Config updated"
    );

    let _ = state
        .config_sync_tx
        .send(ConfigSyncEvent {
            guild_id: body.guild_id,
            role_id: body.role_id,
        })
        .await;

    Ok(Json(serde_json::json!({"success": true})))
}

#[derive(Deserialize)]
pub struct DeleteConfigBody {
    pub guild_id: String,
    pub role_id: String,
}

pub async fn delete_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<DeleteConfigBody>,
) -> Result<Json<Value>, AppError> {
    let token = extract_token(&headers)?;

    // Get broadcaster_id before deletion for cleanup
    let broadcaster_id = sqlx::query_scalar::<_, Option<String>>(
        "SELECT broadcaster_id FROM role_links WHERE guild_id = $1 AND role_id = $2 AND api_token = $3",
    )
    .bind(&body.guild_id)
    .bind(&body.role_id)
    .bind(&token)
    .fetch_optional(&state.pool)
    .await?
    .flatten();

    let result = sqlx::query(
        "DELETE FROM role_links WHERE guild_id = $1 AND role_id = $2 AND api_token = $3",
    )
    .bind(&body.guild_id)
    .bind(&body.role_id)
    .bind(&token)
    .execute(&state.pool)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::Unauthorized);
    }

    // Clean up EventSub subscriptions if no other role_links reference this broadcaster
    if let Some(broadcaster_id) = broadcaster_id {
        let still_referenced = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM role_links WHERE broadcaster_id = $1)",
        )
        .bind(&broadcaster_id)
        .fetch_one(&state.pool)
        .await
        .unwrap_or(true);

        if !still_referenced {
            // Clean up EventSub subscriptions
            let subs = sqlx::query_as::<_, (String,)>(
                "DELETE FROM eventsub_subscriptions WHERE broadcaster_id = $1 RETURNING id",
            )
            .bind(&broadcaster_id)
            .fetch_all(&state.pool)
            .await
            .unwrap_or_default();

            if !subs.is_empty() {
                // Best-effort cleanup of Twitch EventSub subscriptions
                if let Ok(app_token) = state.twitch_client.get_app_access_token().await {
                    for (sub_id,) in subs {
                        let _ = state
                            .twitch_client
                            .delete_eventsub_subscription(&sub_id, &app_token)
                            .await;
                    }
                }
            }

            // Clean up orphaned cache entries
            sqlx::query("DELETE FROM user_channel_cache WHERE broadcaster_id = $1")
                .bind(&broadcaster_id)
                .execute(&state.pool)
                .await
                .ok();
        }
    }

    tracing::info!(
        guild_id = body.guild_id,
        role_id = body.role_id,
        "Role link deleted"
    );

    Ok(Json(serde_json::json!({"success": true})))
}
