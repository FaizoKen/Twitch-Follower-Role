use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::services::sync::UserSyncEvent;
use crate::services::twitch::TwitchClient;
use crate::AppState;

/// Handle incoming Twitch EventSub webhook events.
/// Handles: verification challenges, notifications (follow/sub/unsub), revocations.
pub async fn eventsub_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Extract required headers
    let message_id = match headers
        .get("twitch-eventsub-message-id")
        .and_then(|v| v.to_str().ok())
    {
        Some(id) => id.to_string(),
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let timestamp = match headers
        .get("twitch-eventsub-message-timestamp")
        .and_then(|v| v.to_str().ok())
    {
        Some(ts) => ts.to_string(),
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let signature = match headers
        .get("twitch-eventsub-message-signature")
        .and_then(|v| v.to_str().ok())
    {
        Some(sig) => sig.to_string(),
        None => return StatusCode::BAD_REQUEST.into_response(),
    };
    let message_type = match headers
        .get("twitch-eventsub-message-type")
        .and_then(|v| v.to_str().ok())
    {
        Some(mt) => mt.to_string(),
        None => return StatusCode::BAD_REQUEST.into_response(),
    };

    // Verify HMAC signature
    if !TwitchClient::verify_eventsub_signature(
        &message_id,
        &timestamp,
        &body,
        &state.twitch_client.eventsub_secret,
        &signature,
    ) {
        tracing::warn!("EventSub signature verification failed");
        return StatusCode::FORBIDDEN.into_response();
    }

    // Parse body
    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };

    match message_type.as_str() {
        "webhook_callback_verification" => {
            // Echo back the challenge
            let challenge = payload["challenge"].as_str().unwrap_or("");
            (
                StatusCode::OK,
                [("content-type", "text/plain")],
                challenge.to_string(),
            )
                .into_response()
        }
        "notification" => {
            let event_type = payload["subscription"]["type"].as_str().unwrap_or("");
            let event = &payload["event"];

            match event_type {
                "channel.follow" => {
                    handle_follow(event, &state).await;
                }
                "channel.subscribe" => {
                    handle_subscribe(event, &state).await;
                }
                "channel.subscription.end" => {
                    handle_subscription_end(event, &state).await;
                }
                other => {
                    tracing::debug!("Ignoring unknown EventSub event type: {other}");
                }
            }

            StatusCode::NO_CONTENT.into_response()
        }
        "revocation" => {
            let sub_id = payload["subscription"]["id"].as_str().unwrap_or("");
            let sub_type = payload["subscription"]["type"].as_str().unwrap_or("");
            let status = payload["subscription"]["status"].as_str().unwrap_or("");

            tracing::warn!(sub_id, sub_type, status, "EventSub subscription revoked");

            // Remove from DB
            sqlx::query("DELETE FROM eventsub_subscriptions WHERE id = $1")
                .bind(sub_id)
                .execute(&state.pool)
                .await
                .ok();

            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn handle_follow(event: &serde_json::Value, state: &AppState) {
    let user_id = match event["user_id"].as_str() {
        Some(id) => id,
        None => return,
    };
    let broadcaster_id = match event["broadcaster_user_id"].as_str() {
        Some(id) => id,
        None => return,
    };
    let followed_at = event["followed_at"].as_str().unwrap_or("");

    let followed_at_ts = chrono::DateTime::parse_from_rfc3339(followed_at)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .ok();

    // Upsert cache
    sqlx::query(
        "INSERT INTO user_channel_cache (twitch_user_id, broadcaster_id, is_following, followed_at, fetched_at) \
         VALUES ($1, $2, true, $3, now()) \
         ON CONFLICT (twitch_user_id, broadcaster_id) DO UPDATE SET \
         is_following = true, followed_at = COALESCE($3, user_channel_cache.followed_at), fetched_at = now()",
    )
    .bind(user_id)
    .bind(broadcaster_id)
    .bind(followed_at_ts)
    .execute(&state.pool)
    .await
    .ok();

    // Trigger sync if user is linked
    trigger_sync_for_twitch_user(user_id, state).await;

    tracing::debug!(user_id, broadcaster_id, "EventSub: user followed");
}

async fn handle_subscribe(event: &serde_json::Value, state: &AppState) {
    let user_id = match event["user_id"].as_str() {
        Some(id) => id,
        None => return,
    };
    let broadcaster_id = match event["broadcaster_user_id"].as_str() {
        Some(id) => id,
        None => return,
    };
    let tier: i32 = event["tier"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Upsert cache
    sqlx::query(
        "INSERT INTO user_channel_cache (twitch_user_id, broadcaster_id, is_subscribed, sub_tier, fetched_at) \
         VALUES ($1, $2, true, $3, now()) \
         ON CONFLICT (twitch_user_id, broadcaster_id) DO UPDATE SET \
         is_subscribed = true, sub_tier = $3, fetched_at = now()",
    )
    .bind(user_id)
    .bind(broadcaster_id)
    .bind(tier)
    .execute(&state.pool)
    .await
    .ok();

    trigger_sync_for_twitch_user(user_id, state).await;

    tracing::debug!(user_id, broadcaster_id, tier, "EventSub: user subscribed");
}

async fn handle_subscription_end(event: &serde_json::Value, state: &AppState) {
    let user_id = match event["user_id"].as_str() {
        Some(id) => id,
        None => return,
    };
    let broadcaster_id = match event["broadcaster_user_id"].as_str() {
        Some(id) => id,
        None => return,
    };

    // Update cache
    sqlx::query(
        "UPDATE user_channel_cache SET is_subscribed = false, sub_tier = 0, fetched_at = now() \
         WHERE twitch_user_id = $1 AND broadcaster_id = $2",
    )
    .bind(user_id)
    .bind(broadcaster_id)
    .execute(&state.pool)
    .await
    .ok();

    trigger_sync_for_twitch_user(user_id, state).await;

    tracing::debug!(user_id, broadcaster_id, "EventSub: subscription ended");
}

/// Look up the Discord ID for a Twitch user and trigger a sync event.
async fn trigger_sync_for_twitch_user(twitch_user_id: &str, state: &AppState) {
    let discord_id = sqlx::query_scalar::<_, String>(
        "SELECT discord_id FROM linked_accounts WHERE twitch_user_id = $1",
    )
    .bind(twitch_user_id)
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten();

    if let Some(discord_id) = discord_id {
        let _ = state
            .user_sync_tx
            .send(UserSyncEvent::UserUpdated { discord_id })
            .await;
    }
}
