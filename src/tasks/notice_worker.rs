//! Keeps RoleLogic's dashboard told which role links need their owner.
//!
//! A channel whose Twitch connection died (the broadcaster changed their
//! password, or removed the app on Twitch) stops updating every role on it,
//! and the only place that said so was this plugin's settings page — which an
//! owner has no reason to open while things look fine. So the link itself
//! carries a notice in RoleLogic's dashboard, set the moment the connection is
//! lost and cleared once it is back.
//!
//! The state is derived, never stored twice: a link needs a notice exactly
//! while it has a channel and no token. `role_links.reported_notice` is what
//! RoleLogic last accepted, and each pass sends only the links where the two
//! differ. That makes every path that drops or restores a token (the refresh
//! worker, a reconnect, a reconnect of the same channel elsewhere) reach the
//! dashboard within a pass, without any of them knowing about notices.

use std::sync::Arc;
use std::time::Duration;

use crate::services::rolelogic::NoticeOutcome;
use crate::services::sync::delete_orphan_role_link;
use crate::AppState;

/// Gap between passes. A pass that finds nothing to send is one query.
const TICK: Duration = Duration::from_secs(60);
/// Links sent per pass.
const BATCH: i64 = 50;

pub const RECONNECT_REQUIRED: &str = "reconnect_required";
const NONE: &str = "none";

/// Retry delays, by why the last attempt did not land.
const RETRY_UNSUPPORTED_SECS: i64 = 3600;
const RETRY_INACTIVE_SECS: i64 = 6 * 3600;
const RETRY_ERROR_SECS: i64 = 600;

/// The notice code a link needs, in SQL. Kept in one place so the pass query
/// and the tests agree.
const DESIRED_SQL: &str =
    "CASE WHEN broadcaster_id IS NOT NULL AND broadcaster_access_token IS NULL \
     THEN 'reconnect_required' ELSE 'none' END";

/// What the owner reads under the dashboard's "Reconnect needed" heading.
pub fn reconnect_message(login: Option<&str>) -> String {
    let channel = match login.filter(|l| !l.is_empty()) {
        Some(l) => format!("Twitch channel {l}"),
        None => "The Twitch channel".to_string(),
    };
    format!(
        "{channel} needs reconnecting: Twitch stopped accepting this connection, so this \
         role is not being updated. Open the settings and click Reconnect Twitch channel."
    )
}

pub async fn run(state: Arc<AppState>) {
    tracing::info!("Notice worker started");
    loop {
        tokio::time::sleep(TICK).await;
        if let Err(e) = pass(&state).await {
            tracing::warn!("Notice pass failed: {e}");
        }
    }
}

type Row = (String, String, String, Option<String>, String);

async fn pass(state: &AppState) -> Result<(), sqlx::Error> {
    let rows: Vec<Row> = sqlx::query_as(&format!(
        "SELECT guild_id, role_id, api_token, broadcaster_login, {DESIRED_SQL} AS desired \
         FROM role_links \
         WHERE {DESIRED_SQL} <> reported_notice \
           AND (notice_retry_at IS NULL OR notice_retry_at <= now()) \
         ORDER BY notice_retry_at NULLS FIRST \
         LIMIT {BATCH}"
    ))
    .fetch_all(&state.pool)
    .await?;

    let mut unsupported = 0usize;
    for (guild_id, role_id, token, login, desired) in rows {
        let result = if desired == RECONNECT_REQUIRED {
            state
                .rl_client
                .set_notice(
                    &guild_id,
                    &role_id,
                    &token,
                    RECONNECT_REQUIRED,
                    &reconnect_message(login.as_deref()),
                )
                .await
        } else {
            state
                .rl_client
                .clear_notice(&guild_id, &role_id, &token)
                .await
        };

        let retry_in = match result {
            Ok(NoticeOutcome::Applied) => {
                sqlx::query(
                    "UPDATE role_links SET reported_notice = $1, notice_retry_at = NULL \
                     WHERE guild_id = $2 AND role_id = $3",
                )
                .bind(&desired)
                .bind(&guild_id)
                .bind(&role_id)
                .execute(&state.pool)
                .await?;
                tracing::info!(
                    guild_id,
                    role_id,
                    notice = desired,
                    "RoleLogic notice {}",
                    if desired == NONE { "cleared" } else { "set" }
                );
                continue;
            }
            Ok(NoticeOutcome::LinkGone) => {
                delete_orphan_role_link(&guild_id, &role_id, &state.pool).await;
                continue;
            }
            Ok(NoticeOutcome::Unsupported) => {
                unsupported += 1;
                RETRY_UNSUPPORTED_SECS
            }
            Ok(NoticeOutcome::Inactive) => RETRY_INACTIVE_SECS,
            Err(e) => {
                tracing::warn!(guild_id, role_id, "RoleLogic notice not sent: {e}");
                RETRY_ERROR_SECS
            }
        };

        sqlx::query(
            "UPDATE role_links SET notice_retry_at = now() + make_interval(secs => $1) \
             WHERE guild_id = $2 AND role_id = $3",
        )
        .bind(retry_in as f64)
        .bind(&guild_id)
        .bind(&role_id)
        .execute(&state.pool)
        .await?;
    }

    if unsupported > 0 {
        tracing::info!(
            links = unsupported,
            "RoleLogic does not accept role-link notices yet; retrying hourly"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_message_names_the_channel_and_the_fix() {
        let m = reconnect_message(Some("cheztatasimone"));
        assert!(m.starts_with("Twitch channel cheztatasimone needs reconnecting"));
        assert!(m.contains("Reconnect Twitch channel"));
        // RoleLogic caps notice messages at 300 characters; a Twitch login is
        // at most 25.
        assert!(reconnect_message(Some(&"a".repeat(25))).len() <= 300);
    }

    #[test]
    fn a_missing_login_still_reads() {
        assert!(reconnect_message(None).starts_with("The Twitch channel needs"));
        assert!(reconnect_message(Some("")).starts_with("The Twitch channel needs"));
    }

    #[test]
    fn the_desired_codes_match_what_is_stored() {
        assert!(DESIRED_SQL.contains(&format!("'{RECONNECT_REQUIRED}'")));
        assert!(DESIRED_SQL.contains(&format!("'{NONE}'")));
    }
}
