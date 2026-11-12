//! Holds Twitch's EventSub subscriptions to what this plugin needs.
//!
//! Every connected channel (one with a broadcaster token) should have exactly
//! one live subscription for each of [`EVENTSUB_TYPES`]. Twitch can drop one
//! at any time: it revokes a subscription when the webhook fails too often,
//! when the broadcaster takes back the grant, or when a verification never
//! completes. A dropped follow subscription means new followers wait for the
//! 3-hour full check instead of seconds, and nothing in the request path would
//! ever notice.
//!
//! So this task asks Twitch for the full list every [`INTERVAL`] (and soon
//! after a revocation), creates what is missing, deletes what is dead,
//! duplicated or no longer wanted, and rewrites `eventsub_subscriptions` to
//! mirror what Twitch holds. Twitch is the record; the table is a copy for the
//! connect and cleanup paths.
//!
//! Cost: one call per listed page (Twitch decides the page size), plus one per
//! create or delete. A run with nothing to fix is a handful of calls.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::error::AppError;
use crate::routes::broadcaster::{eventsub_callback_url, eventsub_condition, EVENTSUB_TYPES};
use crate::services::twitch::{CreateSubscription, ListedSubscription};
use crate::AppState;

/// Normal gap between runs.
const INTERVAL: Duration = Duration::from_secs(6 * 3600);
/// A revocation asks for an early run; this bounds how early, so a burst of
/// revocations is handled by one run.
const MIN_GAP: Duration = Duration::from_secs(5 * 60);
/// Let the server come up and answer verification challenges first.
const FIRST_RUN_DELAY: Duration = Duration::from_secs(120);
/// A subscription still waiting for its verification challenge after this
/// long will not complete; Twitch gives up on a challenge within minutes.
const PENDING_GRACE_MINS: i64 = 10;
/// Creates per run. The rest wait for the next run instead of one run taking
/// the whole rate budget after, say, a callback URL change.
const MAX_CREATES_PER_RUN: usize = 600;

pub async fn run(state: Arc<AppState>) {
    tracing::info!("EventSub reconcile task started");
    tokio::time::sleep(FIRST_RUN_DELAY).await;

    loop {
        let started = Instant::now();
        match reconcile(&state).await {
            Ok(summary) => tracing::info!(
                live = summary.live,
                created = summary.created,
                deleted = summary.deleted,
                already_existed = summary.already_existed,
                not_authorized = summary.not_authorized,
                failed = summary.failed,
                deferred = summary.deferred,
                other_callbacks = summary.other_callbacks,
                "EventSub reconcile"
            ),
            Err(e) => tracing::warn!("EventSub reconcile failed: {e}"),
        }

        tokio::select! {
            _ = tokio::time::sleep(INTERVAL) => {}
            _ = state.eventsub_nudge.notified() => {}
        }
        if let Some(rest) = MIN_GAP.checked_sub(started.elapsed()) {
            tokio::time::sleep(rest).await;
        }
    }
}

#[derive(Debug, Default)]
struct Summary {
    /// Subscriptions live after the run (enabled or freshly pending).
    live: usize,
    created: usize,
    deleted: usize,
    already_existed: usize,
    not_authorized: usize,
    failed: usize,
    /// Creates left for the next run.
    deferred: usize,
    /// Webhook subscriptions of this client pointing elsewhere (another
    /// environment); listed for visibility, never touched.
    other_callbacks: usize,
}

/// What a run should do.
#[derive(Debug, Default)]
struct Plan {
    /// Subscriptions to leave alone, by (type, broadcaster).
    keep: Vec<ListedSubscription>,
    /// Ids to delete at Twitch.
    delete: Vec<String>,
    /// (type, version, broadcaster) to create.
    create: Vec<(&'static str, &'static str, String)>,
}

fn same_callback(a: &str, b: &str) -> bool {
    a.trim_end_matches('/')
        .eq_ignore_ascii_case(b.trim_end_matches('/'))
}

/// Is this subscription doing its job, or about to?
fn is_live(sub: &ListedSubscription, now: DateTime<Utc>) -> bool {
    match sub.status.as_str() {
        "enabled" => true,
        "webhook_callback_verification_pending" => sub
            .created_at
            .is_none_or(|at| (now - at).num_minutes() < PENDING_GRACE_MINS),
        _ => false,
    }
}

/// Decide what to keep, delete and create. `ours` are the subscriptions whose
/// callback is this plugin's; `wanted` the channels that should have them.
fn plan(ours: Vec<ListedSubscription>, wanted: &HashSet<String>, now: DateTime<Utc>) -> Plan {
    let mut plan = Plan::default();
    let mut best: HashMap<(String, String), ListedSubscription> = HashMap::new();

    for sub in ours {
        let known = EVENTSUB_TYPES
            .iter()
            .any(|(t, v)| *t == sub.event_type && *v == sub.version);
        let channel = sub.broadcaster_id.clone().filter(|b| wanted.contains(b));
        let Some(channel) = channel.filter(|_| known && is_live(&sub, now)) else {
            plan.delete.push(sub.id);
            continue;
        };

        let key = (sub.event_type.clone(), channel);
        match best.remove(&key) {
            None => {
                best.insert(key, sub);
            }
            // One per key: an enabled one beats a pending one, then the newer.
            Some(held) => {
                let rank = |s: &ListedSubscription| (s.status == "enabled", s.created_at);
                let (winner, loser) = if rank(&sub) > rank(&held) {
                    (sub, held)
                } else {
                    (held, sub)
                };
                plan.delete.push(loser.id);
                best.insert(key, winner);
            }
        }
    }

    let mut channels: Vec<&String> = wanted.iter().collect();
    channels.sort();
    for channel in channels {
        for (event_type, version) in EVENTSUB_TYPES {
            if !best.contains_key(&(event_type.to_string(), channel.clone())) {
                plan.create.push((event_type, version, channel.clone()));
            }
        }
    }

    let mut keep: Vec<ListedSubscription> = best.into_values().collect();
    keep.sort_by(|a, b| a.id.cmp(&b.id));
    plan.keep = keep;
    plan
}

async fn reconcile(state: &AppState) -> Result<Summary, AppError> {
    let twitch = &state.twitch_client;
    let app_token = twitch.get_app_access_token().await?;
    let callback = eventsub_callback_url(state);

    let listed = twitch.list_eventsub_subscriptions(&app_token).await?;
    let mut summary = Summary::default();
    let (ours, other): (Vec<_>, Vec<_>) = listed.into_iter().partition(|s| {
        s.callback
            .as_deref()
            .is_some_and(|c| same_callback(c, &callback))
    });
    summary.other_callbacks = other.iter().filter(|s| s.callback.is_some()).count();

    let wanted: HashSet<String> = sqlx::query_scalar(
        "SELECT DISTINCT broadcaster_id FROM role_links \
         WHERE broadcaster_id IS NOT NULL AND broadcaster_access_token IS NOT NULL",
    )
    .fetch_all(&state.pool)
    .await?
    .into_iter()
    .collect();

    let plan = plan(ours, &wanted, Utc::now());

    for id in &plan.delete {
        twitch.wait_for_permit().await;
        // Already gone counts as deleted; the call logs anything else.
        let _ = twitch.delete_eventsub_subscription(id, &app_token).await;
        summary.deleted += 1;
    }

    // (id, broadcaster, type, status) to record.
    let mut record: Vec<(String, String, String, String)> = plan
        .keep
        .iter()
        .filter_map(|s| {
            Some((
                s.id.clone(),
                s.broadcaster_id.clone()?,
                s.event_type.clone(),
                s.status.clone(),
            ))
        })
        .collect();

    summary.deferred = plan.create.len().saturating_sub(MAX_CREATES_PER_RUN);
    for (event_type, version, channel) in plan.create.into_iter().take(MAX_CREATES_PER_RUN) {
        twitch.wait_for_permit().await;
        match twitch
            .create_eventsub_subscription(
                event_type,
                version,
                eventsub_condition(event_type, &channel),
                &callback,
                &app_token,
            )
            .await
        {
            Ok(CreateSubscription::Created(sub)) => {
                summary.created += 1;
                record.push((sub.id, channel, event_type.to_string(), sub.status));
            }
            // Created between our list and now (a channel connecting). The
            // next run records it.
            Ok(CreateSubscription::AlreadyExists) => summary.already_existed += 1,
            // The broadcaster's grant no longer covers this. Their token is
            // about to fail too, and the refresh worker then drops it, which
            // takes the channel out of `wanted`.
            Ok(CreateSubscription::NotAuthorized) => summary.not_authorized += 1,
            Err(e) => {
                summary.failed += 1;
                tracing::debug!(channel, event_type, "EventSub create failed: {e}");
            }
        }
    }
    summary.live = record.len();

    let mut tx = state.pool.begin().await?;
    sqlx::query("DELETE FROM eventsub_subscriptions")
        .execute(&mut *tx)
        .await?;
    for (id, broadcaster_id, event_type, status) in &record {
        sqlx::query(
            "INSERT INTO eventsub_subscriptions (id, broadcaster_id, event_type, status) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (broadcaster_id, event_type) DO UPDATE SET id = $1, status = $4",
        )
        .bind(id)
        .bind(broadcaster_id)
        .bind(event_type)
        .bind(status)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sub(id: &str, t: &str, channel: &str, status: &str, age_mins: i64) -> ListedSubscription {
        ListedSubscription {
            id: id.into(),
            status: status.into(),
            event_type: t.into(),
            version: EVENTSUB_TYPES
                .iter()
                .find(|(et, _)| *et == t)
                .map(|(_, v)| v.to_string())
                .unwrap_or_else(|| "1".into()),
            broadcaster_id: Some(channel.into()),
            callback: Some("https://x/webhooks/twitch".into()),
            created_at: Some(Utc::now() - chrono::Duration::minutes(age_mins)),
        }
    }

    fn wanted(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    fn ids(subs: &[ListedSubscription]) -> Vec<&str> {
        subs.iter().map(|s| s.id.as_str()).collect()
    }

    #[test]
    fn a_healthy_channel_needs_nothing() {
        let p = plan(
            vec![
                sub("1", "channel.follow", "A", "enabled", 900),
                sub("2", "channel.subscribe", "A", "enabled", 900),
                sub("3", "channel.subscription.end", "A", "enabled", 900),
            ],
            &wanted(&["A"]),
            Utc::now(),
        );
        assert_eq!(ids(&p.keep), vec!["1", "2", "3"]);
        assert!(p.delete.is_empty() && p.create.is_empty());
    }

    #[test]
    fn a_revoked_subscription_is_replaced() {
        let p = plan(
            vec![
                sub(
                    "1",
                    "channel.follow",
                    "A",
                    "notification_failures_exceeded",
                    900,
                ),
                sub("2", "channel.subscribe", "A", "enabled", 900),
                sub("3", "channel.subscription.end", "A", "enabled", 900),
            ],
            &wanted(&["A"]),
            Utc::now(),
        );
        assert_eq!(p.delete, vec!["1"]);
        assert_eq!(p.create, vec![("channel.follow", "2", "A".to_string())]);
    }

    #[test]
    fn a_new_pending_subscription_is_left_to_verify() {
        let p = plan(
            vec![sub(
                "1",
                "channel.follow",
                "A",
                "webhook_callback_verification_pending",
                2,
            )],
            &wanted(&["A"]),
            Utc::now(),
        );
        assert_eq!(ids(&p.keep), vec!["1"]);
        assert_eq!(p.create.len(), 2, "the other two types are missing");
    }

    #[test]
    fn a_stuck_pending_subscription_is_replaced() {
        let p = plan(
            vec![sub(
                "1",
                "channel.follow",
                "A",
                "webhook_callback_verification_pending",
                PENDING_GRACE_MINS + 1,
            )],
            &wanted(&["A"]),
            Utc::now(),
        );
        assert_eq!(p.delete, vec!["1"]);
        assert_eq!(p.create.len(), 3);
    }

    #[test]
    fn a_channel_no_longer_connected_loses_its_subscriptions() {
        let p = plan(
            vec![sub("1", "channel.follow", "GONE", "enabled", 900)],
            &wanted(&[]),
            Utc::now(),
        );
        assert_eq!(p.delete, vec!["1"]);
        assert!(p.keep.is_empty() && p.create.is_empty());
    }

    #[test]
    fn duplicates_keep_the_enabled_one() {
        let p = plan(
            vec![
                sub("old", "channel.follow", "A", "enabled", 900),
                sub(
                    "new",
                    "channel.follow",
                    "A",
                    "webhook_callback_verification_pending",
                    1,
                ),
                sub("newer", "channel.follow", "A", "enabled", 100),
            ],
            &wanted(&["A"]),
            Utc::now(),
        );
        assert_eq!(ids(&p.keep), vec!["newer"]);
        let mut deleted = p.delete.clone();
        deleted.sort();
        assert_eq!(deleted, vec!["new", "old"]);
    }

    #[test]
    fn an_old_follow_version_is_replaced() {
        let mut v1 = sub("1", "channel.follow", "A", "enabled", 900);
        v1.version = "1".into();
        let p = plan(vec![v1], &wanted(&["A"]), Utc::now());
        assert_eq!(p.delete, vec!["1"]);
        assert!(p.create.contains(&("channel.follow", "2", "A".to_string())));
    }

    #[test]
    fn callbacks_compare_without_a_trailing_slash_or_case() {
        assert!(same_callback(
            "https://Plugin.example/twitch-follower-role/webhooks/twitch/",
            "https://plugin.example/twitch-follower-role/webhooks/twitch"
        ));
        assert!(!same_callback(
            "https://dev.example/webhooks/twitch",
            "https://plugin.example/webhooks/twitch"
        ));
    }
}
