use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use rand::Rng;
use serde::Deserialize;
use serde_json::json;

use crate::error::AppError;
use crate::services::sync::{self, ConfigSyncEvent};
use crate::services::twitch::CreateSubscription;
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

/// Twitch's redirect back: `code` + `state` on success, `error=access_denied`
/// + `state` (no code) when the broadcaster clicks Cancel.
#[derive(Deserialize)]
pub struct ConnectCallbackQuery {
    pub code: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
}

const OAUTH_NOT_CONNECTED_TEMPLATE: &str = include_str!("../../templates/oauth_not_connected.html");

/// The new tab's page when no channel was connected: what happened and what
/// to do, in place of a bare error.
fn not_connected_page(state: &AppState, status: StatusCode, message: &str) -> Response {
    let html = OAUTH_NOT_CONNECTED_TEMPLATE
        .replace("{{BASE_URL}}", &state.config.base_url)
        .replace("{{MESSAGE}}", message);
    (status, [("content-type", "text/html; charset=utf-8")], html).into_response()
}

/// Handle broadcaster OAuth callback.
/// Stores broadcaster token, creates EventSub subscriptions.
pub async fn connect_callback(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ConnectCallbackQuery>,
) -> Result<Response, AppError> {
    // Validate state (consumed on a cancel too: it is single-use either way).
    let oauth_state = match query.state.as_deref() {
        Some(s) => sqlx::query_as::<_, (serde_json::Value,)>(
            "DELETE FROM oauth_states WHERE state = $1 AND expires_at > now() RETURNING redirect_data",
        )
        .bind(s)
        .fetch_optional(&state.pool)
        .await?,
        None => None,
    };
    let Some(oauth_state) = oauth_state else {
        return Ok(not_connected_page(
            &state,
            StatusCode::BAD_REQUEST,
            "This connect link expired or was already used. Go back to the RoleLogic dashboard and click Connect Twitch channel again.",
        ));
    };
    let code = match (query.code.as_deref(), query.error.as_deref()) {
        (Some(code), None) => code.to_string(),
        _ => {
            return Ok(not_connected_page(
                &state,
                StatusCode::OK,
                "You cancelled on Twitch, so nothing changed. Go back to the RoleLogic dashboard and click Connect Twitch channel when you're ready.",
            ))
        }
    };

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
        .exchange_code(&code, &redirect_uri)
        .await?;

    let refresh_token = refresh_token.ok_or(AppError::TwitchApi(
        "No refresh token returned for broadcaster".into(),
    ))?;

    // Get broadcaster info
    let broadcaster = state.twitch_client.get_user_by_token(&access_token).await?;

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

    // The same channel bound to other role links shares one connection: the
    // refresh worker writes a refreshed token to every link of the channel,
    // and drops it from every link when Twitch rejects it. Signing in again
    // here therefore revives those links too, instead of leaving them saying
    // "needs reconnecting" until each one is reconnected separately.
    let revived = sqlx::query(
        "UPDATE role_links SET \
         broadcaster_login = $1, broadcaster_access_token = $2, broadcaster_refresh_token = $3 \
         WHERE broadcaster_id = $4 AND broadcaster_access_token IS NULL \
           AND NOT (guild_id = $5 AND role_id = $6)",
    )
    .bind(&broadcaster.login)
    .bind(&access_token)
    .bind(&refresh_token)
    .bind(&broadcaster.id)
    .bind(&guild_id)
    .bind(&role_id)
    .execute(&state.pool)
    .await?
    .rows_affected();

    tracing::info!(
        guild_id,
        role_id,
        broadcaster_id = broadcaster.id,
        broadcaster_login = broadcaster.login,
        revived_links = revived,
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
        if let Err(e) = create_eventsub_subscriptions(&state_clone, &broadcaster_id).await {
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

    Ok((
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        html,
    )
        .into_response())
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

/// The EventSub types every connected channel is subscribed to, with their
/// versions. The reconcile task holds Twitch to exactly this set.
pub(crate) const EVENTSUB_TYPES: [(&str, &str); 3] = [
    ("channel.follow", "2"),
    ("channel.subscribe", "1"),
    ("channel.subscription.end", "1"),
];

/// The condition Twitch expects for one of [`EVENTSUB_TYPES`]. A follow
/// subscription reads the follower list as a moderator of the channel; the
/// broadcaster is their own channel's moderator.
pub(crate) fn eventsub_condition(event_type: &str, broadcaster_id: &str) -> serde_json::Value {
    if event_type == "channel.follow" {
        json!({
            "broadcaster_user_id": broadcaster_id,
            "moderator_user_id": broadcaster_id
        })
    } else {
        json!({ "broadcaster_user_id": broadcaster_id })
    }
}

/// Where Twitch delivers this plugin's EventSub notifications.
pub(crate) fn eventsub_callback_url(state: &AppState) -> String {
    format!("{}/webhooks/twitch", state.config.base_url)
}

/// Create EventSub webhook subscriptions for a broadcaster.
async fn create_eventsub_subscriptions(
    state: &AppState,
    broadcaster_id: &str,
) -> Result<(), AppError> {
    let app_token = state.twitch_client.get_app_access_token().await?;
    let callback_url = eventsub_callback_url(state);

    for (event_type, version) in EVENTSUB_TYPES {
        let condition = eventsub_condition(event_type, broadcaster_id);
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
            // Twitch already has one; the reconcile task records it.
            Ok(CreateSubscription::AlreadyExists) => {}
            Ok(CreateSubscription::NotAuthorized) => {
                tracing::warn!(
                    broadcaster_id,
                    event_type,
                    "EventSub subscription refused: the broadcaster's grant lacks the scope"
                );
            }
            Ok(CreateSubscription::Created(sub)) => {
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
                tracing::error!(
                    broadcaster_id,
                    event_type,
                    "EventSub subscription failed: {e}"
                );
            }
        }
    }

    Ok(())
}
