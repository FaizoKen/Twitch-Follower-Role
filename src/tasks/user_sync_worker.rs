use std::sync::Arc;

use tokio::sync::mpsc;

use crate::services::sync::{self, UserSyncEvent};
use crate::AppState;

pub async fn run(mut rx: mpsc::Receiver<UserSyncEvent>, state: Arc<AppState>) {
    tracing::info!("User sync worker started");

    while let Some(event) = rx.recv().await {
        let result = match &event {
            UserSyncEvent::UserUpdated { discord_id }
            | UserSyncEvent::AccountLinked { discord_id } => {
                tracing::debug!(discord_id, event = ?event, "Syncing roles for user");
                sync::sync_for_user(discord_id, &state).await
            }
            UserSyncEvent::AccountUnlinked { discord_id } => {
                tracing::debug!(discord_id, "Removing all assignments for unlinked user");
                sync::remove_all_assignments(discord_id, &state).await
            }
        };

        if let Err(e) = result {
            tracing::error!(event = ?event, "User sync failed: {e}");
        }
    }

    tracing::warn!("User sync worker channel closed");
}
