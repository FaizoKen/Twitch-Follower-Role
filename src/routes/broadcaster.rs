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
    sync::populate_cache_for_broadcaster(&broadcaster.id, &guild_id, &state.pool).await?;

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

    // Return success page
    let html = format!(
        r##"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Twitch Follower Role - Connected</title>
    <link rel="icon" href="{base_url}/favicon.ico" type="image/x-icon">
    <meta name="theme-color" content="#9146ff">
    <style>
        body {{ font-family: system-ui, sans-serif; max-width: 480px; margin: 60px auto; padding: 20px; background: #0e0e10; color: #c8ccd4; text-align: center; }}
        h1 {{ color: #9146ff; }}
        .card {{ background: #18181b; padding: 24px; border-radius: 10px; margin-top: 20px; border: 1px solid #2a2a2e; }}
        .success {{ color: #4ade80; }}
    </style>
</head>
<body>
    <h1>Twitch Follower Role</h1>
    <div class="card">
        <p class="success" style="font-size: 18px; font-weight: 600;">Channel connected!</p>
        <p style="margin-top: 12px;">Connected as <strong>{login}</strong></p>
        <p style="margin-top: 8px; color: #7a8299; font-size: 13px;">You can close this page and return to the RoleLogic dashboard to configure conditions.</p>
    </div>
</body>
</html>"##,
        base_url = state.config.base_url,
        login = broadcaster.login
    );

    Ok((StatusCode::OK, [("content-type", "text/html; charset=utf-8")], html).into_response())
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
