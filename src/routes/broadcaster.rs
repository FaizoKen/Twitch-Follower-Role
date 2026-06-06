use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use rand::Rng;
use serde::Deserialize;
use serde_json::json;

use crate::error::AppError;
use crate::services::sync::{self, ConfigSyncEvent};
use crate::AppState;

const OAUTH_DONE_TEMPLATE: &str = include_str!("../../templates/oauth_done.html");

#[derive(Deserialize)]
pub struct ConnectQuery {
    pub guild_id: String,
    pub role_id: String,
}

/// Start broadcaster Twitch OAuth flow.
/// The channel owner visits this URL to authorize the plugin.
pub async fn connect(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ConnectQuery>,
) -> Result<Redirect, AppError> {
    // Verify the role link exists
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM role_links WHERE guild_id = $1 AND role_id = $2)",
    )
    .bind(&query.guild_id)
    .bind(&query.role_id)
    .fetch_one(&state.pool)
    .await
    .unwrap_or(false);

    if !exists {
        return Err(AppError::NotFound("Role link not found".into()));
    }

    let state_token: String = {
        let mut rng = rand::thread_rng();
        (0..32)
            .map(|_| {
                let idx = rng.gen_range(0..36);
                if idx < 10 {
                    (b'0' + idx) as char
                } else {
                    (b'a' + idx - 10) as char
                }
            })
            .collect()
    };

    sqlx::query(
        "INSERT INTO oauth_states (state, redirect_data, expires_at) VALUES ($1, $2, now() + interval '10 minutes')",
    )
    .bind(&state_token)
    .bind(json!({"type": "broadcaster", "guild_id": query.guild_id, "role_id": query.role_id}))
    .execute(&state.pool)
    .await?;

    let redirect_uri = state.config.broadcaster_oauth_redirect_uri();
    let url = format!(
        "https://id.twitch.tv/oauth2/authorize?client_id={}&redirect_uri={}&response_type=code&scope=moderator%3Aread%3Afollowers+channel%3Aread%3Asubscriptions&state={}&force_verify=true",
        state.config.twitch_client_id,
        urlencoding::encode(&redirect_uri),
        state_token
    );

    Ok(Redirect::temporary(&url))
}

#[derive(Deserialize)]
pub struct ConnectCallbackQuery {
    pub code: String,
    pub state: String,
}

/// Handle broadcaster OAuth callback.
/// Stores broadcaster token, creates EventSub subscriptions.
pub async fn connect_callback(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ConnectCallbackQuery>,
) -> Result<Response, AppError> {
    // Validate state
    let oauth_state = sqlx::query_as::<_, (serde_json::Value,)>(
        "DELETE FROM oauth_states WHERE state = $1 AND expires_at > now() RETURNING redirect_data",
    )
    .bind(&query.state)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::BadRequest("Invalid or expired OAuth state".into()))?;

    let data = &oauth_state.0;
    let guild_id = data["guild_id"]
        .as_str()
        .ok_or(AppError::Internal("Missing guild_id in state".into()))?
        .to_string();
    let role_id = data["role_id"]
        .as_str()
        .ok_or(AppError::Internal("Missing role_id in state".into()))?
        .to_string();

    // Exchange code for broadcaster tokens
    let redirect_uri = state.config.broadcaster_oauth_redirect_uri();
    let (access_token, refresh_token) = state
        .twitch_client
        .exchange_code(&query.code, &redirect_uri)
        .await?;

    let refresh_token = refresh_token.ok_or(AppError::TwitchApi(
        "No refresh token returned for broadcaster".into(),
    ))?;

    // Get broadcaster info
    let broadcaster = state
        .twitch_client
        .get_user_by_token(&access_token)
        .await?;

    // Store broadcaster connection on the role link
    sqlx::query(
        "UPDATE role_links SET \
         broadcaster_id = $1, broadcaster_login = $2, \
         broadcaster_access_token = $3, broadcaster_refresh_token = $4, \
         updated_at = now() \
         WHERE guild_id = $5 AND role_id = $6",
    )
    .bind(&broadcaster.id)
    .bind(&broadcaster.login)
    .bind(&access_token)
    .bind(&refresh_token)
    .bind(&guild_id)
    .bind(&role_id)
    .execute(&state.pool)
    .await?;

    tracing::info!(
        guild_id,
        role_id,
        broadcaster_id = broadcaster.id,
        broadcaster_login = broadcaster.login,
        "Broadcaster connected"
    );

    // Populate cache entries for all linked users in this guild
    sync::populate_cache_for_broadcaster(&broadcaster.id, &guild_id, &state).await?;

    // Create EventSub subscriptions (best-effort, non-blocking)
    let state_clone = Arc::clone(&state);
    let broadcaster_id = broadcaster.id.clone();
    let broadcaster_login = broadcaster.login.clone();
    let guild_id_clone = guild_id.clone();
    let role_id_clone = role_id.clone();
    tokio::spawn(async move {
        if let Err(e) = create_eventsub_subscriptions(
            &state_clone,
            &broadcaster_id,
        )
        .await
        {
            tracing::error!(
                broadcaster_id,
                broadcaster_login,
                "Failed to create EventSub subscriptions: {e}"
            );
        }
        // Trigger config sync after broadcaster connection
        let _ = state_clone
            .config_sync_tx
            .send(ConfigSyncEvent {
                guild_id: guild_id_clone,
                role_id: role_id_clone,
            })
            .await;
    });

    // Return success page (opened in a new tab from the iframe — it tries to
    // auto-close; the iframe re-fetches its data on focus and shows the new
    // channel on its own).
    let login_safe = broadcaster
        .login
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let html = OAUTH_DONE_TEMPLATE
        .replace("{{BASE_URL}}", &state.config.base_url)
        .replace("{{TWITCH_LOGIN}}", &login_safe)
        .replace("{{GUILD_ID}}", &guild_id);

    Ok((StatusCode::OK, [("content-type", "text/html; charset=utf-8")], html).into_response())
}

/// Best-effort cleanup of a broadcaster's EventSub subscriptions and cached
/// rows once no role link references it anymore. Shared by `delete_config`
/// (role link removed) and the iframe disconnect action. Never propagates DB
/// errors — cleanup hiccups must not block the caller's main action.
pub(crate) async fn cleanup_broadcaster_if_orphaned(state: &AppState, broadcaster_id: &str) {
    let still_referenced = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM role_links WHERE broadcaster_id = $1)",
    )
    .bind(broadcaster_id)
    .fetch_one(&state.pool)
    .await
    .unwrap_or(true);

    if still_referenced {
        return;
    }

    let subs = sqlx::query_as::<_, (String,)>(
        "DELETE FROM eventsub_subscriptions WHERE broadcaster_id = $1 RETURNING id",
    )
    .bind(broadcaster_id)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    if !subs.is_empty() {
        if let Ok(app_token) = state.twitch_client.get_app_access_token().await {
            for (sub_id,) in subs {
                let _ = state
                    .twitch_client
                    .delete_eventsub_subscription(&sub_id, &app_token)
                    .await;
            }
        }
    }

    sqlx::query("DELETE FROM user_channel_cache WHERE broadcaster_id = $1")
        .bind(broadcaster_id)
        .execute(&state.pool)
        .await
        .ok();
}

/// Create EventSub webhook subscriptions for a broadcaster.
async fn create_eventsub_subscriptions(
    state: &AppState,
    broadcaster_id: &str,
) -> Result<(), AppError> {
    let app_token = state.twitch_client.get_app_access_token().await?;
    let callback_url = format!("{}/webhooks/twitch", state.config.base_url);

    let subscriptions = [
        (
            "channel.follow",
            "2",
            json!({
                "broadcaster_user_id": broadcaster_id,
                "moderator_user_id": broadcaster_id
            }),
        ),
        (
            "channel.subscribe",
            "1",
            json!({ "broadcaster_user_id": broadcaster_id }),
        ),
        (
            "channel.subscription.end",
            "1",
            json!({ "broadcaster_user_id": broadcaster_id }),
        ),
    ];

    for (event_type, version, condition) in subscriptions {
        // Skip if already exists
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM eventsub_subscriptions WHERE broadcaster_id = $1 AND event_type = $2)",
        )
        .bind(broadcaster_id)
        .bind(event_type)
        .fetch_one(&state.pool)
        .await
        .unwrap_or(false);

        if exists {
            continue;
        }

        match state
            .twitch_client
            .create_eventsub_subscription(event_type, version, condition, &callback_url, &app_token)
            .await
        {
            Ok(sub) => {
                sqlx::query(
                    "INSERT INTO eventsub_subscriptions (id, broadcaster_id, event_type, status) \
                     VALUES ($1, $2, $3, $4) ON CONFLICT (broadcaster_id, event_type) DO UPDATE SET id = $1, status = $4",
                )
                .bind(&sub.id)
                .bind(broadcaster_id)
                .bind(event_type)
                .bind(&sub.status)
                .execute(&state.pool)
                .await
                .ok();

                tracing::info!(
                    broadcaster_id,
                    event_type,
                    sub_id = sub.id,
                    "EventSub subscription created"
                );
            }
            Err(e) => {
                tracing::error!(broadcaster_id, event_type, "EventSub subscription failed: {e}");
            }
        }
    }

    Ok(())
}
