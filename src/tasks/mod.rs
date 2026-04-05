pub mod config_sync_worker;
pub mod refresh_worker;
pub mod user_sync_worker;

use std::sync::Arc;

use crate::AppState;

/// Periodically clean up expired OAuth states.
pub async fn cleanup_expired(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(300)).await;

        if let Err(e) = sqlx::query("DELETE FROM oauth_states WHERE expires_at < now()")
            .execute(&state.pool)
            .await
        {
            tracing::error!("Failed to clean up OAuth states: {e}");
        }

        // Clean up orphaned cache entries (broadcaster no longer exists in any role link)
        if let Err(e) = sqlx::query(
            "DELETE FROM user_channel_cache WHERE broadcaster_id NOT IN \
             (SELECT DISTINCT broadcaster_id FROM role_links WHERE broadcaster_id IS NOT NULL)",
        )
        .execute(&state.pool)
        .await
        {
            tracing::error!("Failed to clean up orphaned cache entries: {e}");
        }
    }
}
