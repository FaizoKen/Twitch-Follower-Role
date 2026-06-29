use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use crate::services::sync::UserSyncEvent;
use crate::AppState;

const MIN_REFRESH_SECS: i64 = 1800; // 30 min floor
const MAX_REFRESH_SECS: i64 = 86400; // 24 hour cap
const INTERVAL_CACHE_SECS: u64 = 300; // recompute every 5 minutes
const INACTIVE_MULTIPLIER: i64 = 6;

/// A freshly linked user we don't yet see as following/subscribed is re-checked
/// this often, so a just-made follow/sub — or one Twitch's API was briefly slow
/// to surface — lands the role within a minute or two instead of waiting for the
/// normal (slow) inactive cadence.
pub const FAST_RETRY_SECS: i64 = 90;

/// How long after linking the fast re-check window stays open. After this, a
/// user who still doesn't qualify falls back to the normal cadence so someone
/// who links but never follows/subs stops costing API budget.
pub const FAST_RETRY_WINDOW_SECS: i64 = 600;

struct CachedInterval {
    value: AtomicI64,
    last_computed: Mutex<Instant>,
}

impl CachedInterval {
    fn new() -> Self {
        Self {
            value: AtomicI64::new(MIN_REFRESH_SECS),
            last_computed: Mutex::new(
                Instant::now() - std::time::Duration::from_secs(INTERVAL_CACHE_SECS + 1),
            ),
        }
    }

    async fn get(&self, pool: &sqlx::PgPool) -> i64 {
        let mut last = self.last_computed.lock().await;
        if last.elapsed() >= std::time::Duration::from_secs(INTERVAL_CACHE_SECS) {
            // Count cache entries (each needs 2 API calls: follow + sub check)
            let cache_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM user_channel_cache")
                .fetch_one(pool)
                .await
                .unwrap_or(0);

            let interval = if cache_count == 0 {
                MIN_REFRESH_SECS
            } else {
                // 2 API calls per entry, ~240 req/min budget (4/sec)
                let max_per_hour: i64 = 240 * 60; // 14400
                ((cache_count * 2 * 3600) / max_per_hour).clamp(MIN_REFRESH_SECS, MAX_REFRESH_SECS)
            };

            self.value.store(interval, Ordering::Relaxed);
            *last = Instant::now();
        }
        self.value.load(Ordering::Relaxed)
    }
}

pub async fn run(state: Arc<AppState>) {
    tracing::info!("Refresh worker started (polling fallback)");

    let cached_interval = CachedInterval::new();

    loop {
        // Wait for rate limiter
        state.twitch_client.wait_for_permit().await;

        // Pick next stale cache entry
        let next = sqlx::query_as::<_, (String, String, bool, chrono::DateTime<chrono::Utc>)>(
            "SELECT ucc.twitch_user_id, ucc.broadcaster_id, \
             EXISTS(SELECT 1 FROM role_assignments ra \
               JOIN linked_accounts la ON la.discord_id = ra.discord_id \
               WHERE la.twitch_user_id = ucc.twitch_user_id) as is_active, \
             la2.linked_at \
             FROM user_channel_cache ucc \
             JOIN linked_accounts la2 ON la2.twitch_user_id = ucc.twitch_user_id \
             WHERE ucc.next_fetch_at <= now() \
             ORDER BY is_active DESC, ucc.fetch_failures ASC, ucc.next_fetch_at ASC \
             LIMIT 1",
        )
        .fetch_optional(&state.pool)
        .await;

        let (twitch_user_id, broadcaster_id, is_active, linked_at) = match next {
            Ok(Some(row)) => row,
            Ok(None) => {
                tracing::debug!("No cache entries due for refresh, sleeping 30s");
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                continue;
            }
            Err(e) => {
                tracing::error!("Refresh worker DB error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        // Get broadcaster token
        let broadcaster_token = sqlx::query_as::<_, (String, String)>(
            "SELECT broadcaster_access_token, broadcaster_refresh_token \
             FROM role_links WHERE broadcaster_id = $1 AND broadcaster_access_token IS NOT NULL \
             LIMIT 1",
        )
        .bind(&broadcaster_id)
        .fetch_optional(&state.pool)
        .await;

        let (mut access_token, refresh_token) = match broadcaster_token {
            Ok(Some(row)) => row,
            Ok(None) => {
                // No broadcaster token available, skip
                let _ = sqlx::query(
                    "UPDATE user_channel_cache SET next_fetch_at = now() + interval '1 hour' \
                     WHERE twitch_user_id = $1 AND broadcaster_id = $2",
                )
                .bind(&twitch_user_id)
                .bind(&broadcaster_id)
                .execute(&state.pool)
                .await;
                continue;
            }
            Err(e) => {
                tracing::error!("Failed to get broadcaster token: {e}");
                continue;
            }
        };

        // Helper: attempt to refresh the broadcaster token once per cycle
        let mut token_refreshed = false;
        let mut refresh_tok = refresh_token;

        let try_refresh_token = async |state: &AppState,
                                       access_token: &mut String,
                                       refresh_tok: &mut String,
                                       broadcaster_id: &str,
                                       token_refreshed: &mut bool|
               -> Result<(), crate::error::AppError> {
            if *token_refreshed {
                return Err(crate::error::AppError::TwitchApi(
                    "Token already refreshed this cycle, still failing".into(),
                ));
            }
            *token_refreshed = true;

            match state.twitch_client.refresh_token(refresh_tok).await {
                Ok((new_access, new_refresh)) => {
                    sqlx::query(
                        "UPDATE role_links SET broadcaster_access_token = $1, broadcaster_refresh_token = $2 \
                         WHERE broadcaster_id = $3",
                    )
                    .bind(&new_access)
                    .bind(&new_refresh)
                    .bind(broadcaster_id)
                    .execute(&state.pool)
                    .await
                    .ok();

                    tracing::info!(broadcaster_id, "Broadcaster token refreshed successfully");
                    *access_token = new_access;
                    *refresh_tok = new_refresh;
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(broadcaster_id, "Broadcaster token refresh failed: {e}");
                    sqlx::query(
                        "UPDATE role_links SET broadcaster_access_token = NULL, broadcaster_refresh_token = NULL \
                         WHERE broadcaster_id = $1",
                    )
                    .bind(broadcaster_id)
                    .execute(&state.pool)
                    .await
                    .ok();
                    Err(e)
                }
            }
        };

        // Check follower status (with token refresh on 401)
        let mut follower_result = state
            .twitch_client
            .check_follower(&broadcaster_id, &twitch_user_id, &access_token)
            .await;

        if is_twitch_unauthorized(&follower_result)
            && try_refresh_token(
                &state,
                &mut access_token,
                &mut refresh_tok,
                &broadcaster_id,
                &mut token_refreshed,
            )
            .await
            .is_ok()
        {
            follower_result = state
                .twitch_client
                .check_follower(&broadcaster_id, &twitch_user_id, &access_token)
                .await;
        }

        // Check subscription status (with token refresh on 401)
        state.twitch_client.wait_for_permit().await;
        let mut sub_result = state
            .twitch_client
            .check_subscription(&broadcaster_id, &twitch_user_id, &access_token)
            .await;

        if is_twitch_unauthorized(&sub_result)
            && try_refresh_token(
                &state,
                &mut access_token,
                &mut refresh_tok,
                &broadcaster_id,
                &mut token_refreshed,
            )
            .await
            .is_ok()
        {
            sub_result = state
                .twitch_client
                .check_subscription(&broadcaster_id, &twitch_user_id, &access_token)
                .await;
        }

        match (follower_result, sub_result) {
            (Ok(follower), Ok(sub)) => {
                let is_following = follower.is_some();
                let followed_at = follower.as_ref().map(|f| f.followed_at);
                let is_subscribed = sub.is_some();
                let sub_tier = sub.as_ref().map(|s| s.tier).unwrap_or(0);

                let base_interval = cached_interval.get(&state.pool).await;
                let multiplier = if is_active { 1 } else { INACTIVE_MULTIPLIER };
                // Keep a freshly linked user who still doesn't follow/sub on the
                // fast cadence (bounded to FAST_RETRY_WINDOW_SECS after linking) so
                // a just-made / API-lagged follow or sub lands the role within a
                // minute or two. Everyone else uses the normal cadence.
                let within_fast_window =
                    (chrono::Utc::now() - linked_at).num_seconds() < FAST_RETRY_WINDOW_SECS;
                let interval = if !is_following && !is_subscribed && within_fast_window {
                    FAST_RETRY_SECS
                } else {
                    base_interval * multiplier
                };
                let next_fetch = chrono::Utc::now() + chrono::Duration::seconds(interval);

                if let Err(e) = sqlx::query(
                    "UPDATE user_channel_cache SET \
                     is_following = $1, followed_at = $2, \
                     is_subscribed = $3, sub_tier = $4, \
                     fetched_at = now(), next_fetch_at = $5, fetch_failures = 0 \
                     WHERE twitch_user_id = $6 AND broadcaster_id = $7",
                )
                .bind(is_following)
                .bind(followed_at)
                .bind(is_subscribed)
                .bind(sub_tier)
                .bind(next_fetch)
                .bind(&twitch_user_id)
                .bind(&broadcaster_id)
                .execute(&state.pool)
                .await
                {
                    tracing::error!(
                        twitch_user_id,
                        broadcaster_id,
                        "Failed to update cache: {e}"
                    );
                    continue;
                }

                // Trigger sync for this user
                let discord_id = sqlx::query_scalar::<_, String>(
                    "SELECT discord_id FROM linked_accounts WHERE twitch_user_id = $1",
                )
                .bind(&twitch_user_id)
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

                tracing::debug!(twitch_user_id, broadcaster_id, is_active, "Cache refreshed");
            }
            (Err(e), _) | (_, Err(e)) => {
                // Exponential backoff
                let _ = sqlx::query(
                    "UPDATE user_channel_cache SET fetch_failures = fetch_failures + 1, \
                     next_fetch_at = now() + LEAST(INTERVAL '60 seconds' * POWER(2, fetch_failures), INTERVAL '1 hour') \
                     WHERE twitch_user_id = $1 AND broadcaster_id = $2",
                )
                .bind(&twitch_user_id)
                .bind(&broadcaster_id)
                .execute(&state.pool)
                .await;

                tracing::warn!(
                    twitch_user_id,
                    broadcaster_id,
                    "Twitch API fetch failed: {e}"
                );
            }
        }
    }
}

/// Check if a Twitch API result is a 401 Unauthorized error.
fn is_twitch_unauthorized<T>(result: &Result<T, crate::error::AppError>) -> bool {
    matches!(
        result,
        Err(crate::error::AppError::TwitchApi(msg)) if msg.contains("401") || msg.contains("expired") || msg.contains("Unauthorized")
    )
}
