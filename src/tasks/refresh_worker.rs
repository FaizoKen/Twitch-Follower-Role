//! Polling fallback for what Twitch never tells us.
//!
//! Follows, new subscriptions and ended subscriptions arrive through EventSub.
//! An unfollow has no event, so the only way to notice one is to ask Twitch
//! again. This worker does that, and spends its calls where an unfollow can
//! actually cost someone a role:
//!
//! - **Fast tier**: the member holds a role, on a link to this channel, whose
//!   rule reads the follow. Re-checked every [`FAST_FLOOR_SECS`] with a single
//!   follow lookup; their roles are re-synced only when the answer changed.
//! - **Slow tier**: every other linked member of a connected channel. Nothing
//!   they hold depends on an unfollow, and anything they could gain arrives by
//!   EventSub, so this is a reconciliation pass every [`SLOW_FLOOR_SECS`].
//!
//! Every row, fast or slow, also gets a *full* check once per slow interval:
//! follow and subscription both fetched, roles re-synced unconditionally. That
//! is what catches a missed EventSub event, a rule that turns on follow age,
//! and a role change RoleLogic did not accept the first time.
//!
//! Both intervals stretch on their own when the tiers outgrow the call budget
//! ([`plan_intervals`]). They are sized from the rows this worker polls and
//! nothing else: sizing from the whole table is what once turned a 30-minute
//! re-check into a 7-hour one.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures_util::stream::{self, StreamExt};

use crate::error::AppError;
use crate::services::sync::UserSyncEvent;
use crate::AppState;

/// A freshly linked user we don't yet see as following/subscribed is re-checked
/// this often, so a just-made follow/sub — or one Twitch's API was briefly slow
/// to surface — lands the role within a minute or two instead of waiting for the
/// normal (slow) cadence.
pub const FAST_RETRY_SECS: i64 = 90;

/// How long after linking the fast re-check window stays open. After this, a
/// user who still doesn't qualify falls back to the normal cadence so someone
/// who links but never follows/subs stops costing API budget.
pub const FAST_RETRY_WINDOW_SECS: i64 = 600;

/// Shortest gap between follow re-checks for a member whose role depends on
/// the follow. This is how long an unfollow can go unnoticed.
const FAST_FLOOR_SECS: i64 = 600;
/// Shortest gap between full checks, for every row.
const SLOW_FLOOR_SECS: i64 = 3 * 3600;
/// Neither interval stretches past a day.
const MAX_INTERVAL_SECS: i64 = 86400;
/// Twitch calls per second this worker may average. The client's limiter
/// allows 8, Twitch 800 a minute; the rest is headroom for the inline checks
/// a burst of members linking makes.
const POLL_BUDGET_PER_SEC: f64 = 4.0;
/// How long a computed plan is reused before the tiers are counted again.
const PLAN_CACHE_SECS: u64 = 300;

/// Due rows taken per pass.
const BATCH_SIZE: i64 = 40;
/// Channels refreshed at once. Rows of one channel always run one after the
/// other: they share a broadcaster token, and two tasks refreshing the same
/// token would have Twitch reject the slower one's refresh token — which reads
/// as "revoked" and disconnects a working channel.
const CONCURRENT_CHANNELS: usize = 4;
/// Sleep when nothing is due. Also the longest a member-requested re-check
/// waits before it is picked up.
const IDLE_SLEEP_SECS: u64 = 15;

/// Every row this worker polls: a linked member's row for a channel that still
/// has a broadcaster token. `is_fast` is the tier (see the module docs); the
/// rule-tree test keeps a member whose role turns only on the subscription out
/// of the fast tier, since a follow lookup can tell us nothing about them.
///
/// The two target names are `ConditionTarget::as_str` values; a test pins them.
const CANDIDATES: &str = "\
    SELECT ucc.twitch_user_id, ucc.broadcaster_id, la.discord_id, la.linked_at, \
           ucc.is_following, ucc.followed_at, ucc.fetched_at, ucc.full_fetched_at, \
           ucc.next_fetch_at, ucc.fetch_failures, \
           EXISTS ( \
             SELECT 1 FROM role_assignments ra \
             JOIN role_links rl ON rl.guild_id = ra.guild_id AND rl.role_id = ra.role_id \
             WHERE ra.discord_id = la.discord_id \
               AND rl.broadcaster_id = ucc.broadcaster_id \
               AND jsonb_path_exists(rl.rule_tree, \
                 '$.groups[*].conditions[*] ? (@.target == \"is_follower\" || @.target == \"follow_age_days\")') \
           ) AS is_fast \
    FROM user_channel_cache ucc \
    JOIN linked_accounts la ON la.twitch_user_id = ucc.twitch_user_id \
    WHERE EXISTS (SELECT 1 FROM role_links t \
                  WHERE t.broadcaster_id = ucc.broadcaster_id \
                    AND t.broadcaster_access_token IS NOT NULL)";

/// How many rows each tier holds: (fast, slow).
fn tier_count_sql() -> String {
    format!(
        "WITH cand AS ({CANDIDATES}) \
         SELECT count(*) FILTER (WHERE is_fast), count(*) FILTER (WHERE NOT is_fast) FROM cand"
    )
}

/// The rows due now, longest overdue first. `$1` is the fast interval in
/// seconds, `$2` the batch size.
///
/// A row is due at its `next_fetch_at`. A fast-tier row is also due one fast
/// interval after it was last fetched, whatever `next_fetch_at` says: a member
/// can enter the fast tier (a follow event, a config change that grants the
/// role) long before a slow-tier schedule written earlier comes round. A row
/// that is backing off after a failure keeps to `next_fetch_at` alone.
fn due_sql() -> String {
    format!(
        "WITH cand AS ({CANDIDATES}), due AS ( \
           SELECT cand.*, \
                  CASE WHEN is_fast AND fetch_failures = 0 \
                       THEN LEAST(next_fetch_at, fetched_at + make_interval(secs => $1)) \
                       ELSE next_fetch_at END AS due_at \
           FROM cand) \
         SELECT twitch_user_id, broadcaster_id, discord_id, linked_at, \
                is_following, followed_at, full_fetched_at, is_fast \
         FROM due WHERE due_at <= now() ORDER BY due_at ASC LIMIT $2"
    )
}

/// How often each tier is re-checked right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Plan {
    fast_secs: i64,
    slow_secs: i64,
}

impl Plan {
    const FLOORS: Plan = Plan {
        fast_secs: FAST_FLOOR_SECS,
        slow_secs: SLOW_FLOOR_SECS,
    };
}

/// Size both intervals from the number of rows in each tier.
///
/// A full check costs two calls (follow + subscription) and every row gets one
/// per slow interval; that may use up to half the budget. A fast check costs
/// one call per fast-tier row per fast interval and gets whatever the full
/// checks leave. At the floors the budget covers roughly 10,000 rows in total
/// and 2,000 of them fast; past that the intervals grow in proportion instead
/// of the worker falling behind.
fn plan_intervals(fast_rows: i64, slow_rows: i64) -> Plan {
    let total = (fast_rows.max(0) + slow_rows.max(0)) as f64;
    let fast = fast_rows.max(0) as f64;

    let slow_secs = ((2.0 * total / (POLL_BUDGET_PER_SEC * 0.5)).ceil() as i64)
        .clamp(SLOW_FLOOR_SECS, MAX_INTERVAL_SECS);
    let full_check_rate = 2.0 * total / slow_secs as f64;
    // Only past the one-day cap can full checks exceed their half; the fast
    // tier still keeps a quarter of the budget then.
    let fast_budget = (POLL_BUDGET_PER_SEC - full_check_rate).max(POLL_BUDGET_PER_SEC * 0.25);
    let fast_secs = ((fast / fast_budget).ceil() as i64).clamp(FAST_FLOOR_SECS, slow_secs);

    Plan {
        fast_secs,
        slow_secs,
    }
}

/// The current [`Plan`], recounted every [`PLAN_CACHE_SECS`].
struct PlanCache {
    plan: Plan,
    computed_at: Option<Instant>,
}

impl PlanCache {
    fn new() -> Self {
        Self {
            plan: Plan::FLOORS,
            computed_at: None,
        }
    }

    async fn get(&mut self, pool: &sqlx::PgPool) -> Plan {
        let fresh = self
            .computed_at
            .is_some_and(|at| at.elapsed() < Duration::from_secs(PLAN_CACHE_SECS));
        if fresh {
            return self.plan;
        }

        let counts = sqlx::query_as::<_, (i64, i64)>(&tier_count_sql())
            .fetch_one(pool)
            .await;

        match counts {
            Ok((fast_rows, slow_rows)) => {
                let plan = plan_intervals(fast_rows, slow_rows);
                if self.computed_at.is_none() || plan != self.plan {
                    tracing::info!(
                        fast_rows,
                        slow_rows,
                        fast_secs = plan.fast_secs,
                        slow_secs = plan.slow_secs,
                        "Refresh plan"
                    );
                }
                self.plan = plan;
            }
            // Keep the last plan; a wrong interval for five minutes is harmless.
            Err(e) => tracing::error!("Refresh worker could not count its tiers: {e}"),
        }
        self.computed_at = Some(Instant::now());
        self.plan
    }
}

/// One cache row that is due, with what the worker needs to act on it.
struct DueRow {
    twitch_user_id: String,
    broadcaster_id: String,
    discord_id: String,
    linked_at: DateTime<Utc>,
    is_following: bool,
    followed_at: Option<DateTime<Utc>>,
    full_fetched_at: Option<DateTime<Utc>>,
    is_fast: bool,
}

type DueTuple = (
    String,
    String,
    String,
    DateTime<Utc>,
    bool,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    bool,
);

/// One batch of [`due_sql`].
async fn due_rows(pool: &sqlx::PgPool, fast_secs: i64) -> Result<Vec<DueRow>, sqlx::Error> {
    let rows = sqlx::query_as::<_, DueTuple>(&due_sql())
        .bind(fast_secs as f64)
        .bind(BATCH_SIZE)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(
            |(
                twitch_user_id,
                broadcaster_id,
                discord_id,
                linked_at,
                is_following,
                followed_at,
                full_fetched_at,
                is_fast,
            )| DueRow {
                twitch_user_id,
                broadcaster_id,
                discord_id,
                linked_at,
                is_following,
                followed_at,
                full_fetched_at,
                is_fast,
            },
        )
        .collect())
}

/// Split a batch into per-channel runs, keeping the batch's order.
fn group_by_channel(rows: Vec<DueRow>) -> Vec<(String, Vec<DueRow>)> {
    let mut groups: Vec<(String, Vec<DueRow>)> = Vec::new();
    for row in rows {
        match groups.iter_mut().find(|(b, _)| *b == row.broadcaster_id) {
            Some((_, group)) => group.push(row),
            None => groups.push((row.broadcaster_id.clone(), vec![row])),
        }
    }
    groups
}

/// Whether this pass fetches the subscription too and re-syncs regardless.
/// Always for the slow tier; for the fast tier once per slow interval.
fn needs_full_check(row: &DueRow, now: DateTime<Utc>, slow_secs: i64) -> bool {
    if !row.is_fast {
        return true;
    }
    match row.full_fetched_at {
        None => true,
        Some(at) => (now - at).num_seconds() >= slow_secs,
    }
}

/// Whether a follow lookup came back different from the cached row. Compared
/// to the second: an EventSub follow carries sub-second precision the Helix
/// lookup drops, and that is not a change.
fn follow_changed(row: &DueRow, is_following: bool, followed_at: Option<DateTime<Utc>>) -> bool {
    is_following != row.is_following
        || followed_at.map(|t| t.timestamp()) != row.followed_at.map(|t| t.timestamp())
}

pub async fn run(state: Arc<AppState>) {
    tracing::info!("Refresh worker started (polling fallback)");

    let mut plan_cache = PlanCache::new();

    loop {
        let plan = plan_cache.get(&state.pool).await;

        let rows = match due_rows(&state.pool, plan.fast_secs).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!("Refresh worker DB error: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if rows.is_empty() {
            tokio::time::sleep(Duration::from_secs(IDLE_SLEEP_SECS)).await;
            continue;
        }

        // A row whose result could not be written is still due, and would be
        // fetched from Twitch again at once. Pause instead of spinning.
        let db_trouble = AtomicBool::new(false);
        stream::iter(group_by_channel(rows))
            .for_each_concurrent(CONCURRENT_CHANNELS, |(broadcaster_id, rows)| {
                let state = &state;
                let db_trouble = &db_trouble;
                async move {
                    refresh_channel(state, &broadcaster_id, rows, plan, db_trouble).await;
                }
            })
            .await;

        if db_trouble.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

/// A channel's broadcaster token for the length of one batch.
struct ChannelToken {
    access: String,
    refresh: String,
    /// One refresh per batch: a second 401 right after a successful refresh
    /// is not something another refresh fixes.
    refresh_tried: bool,
    /// The refresh failed, so every further call in this batch would too.
    unusable: bool,
}

impl ChannelToken {
    /// Refresh after a 401. Returns whether the call is worth repeating.
    async fn refresh_after_401(&mut self, state: &AppState, broadcaster_id: &str) -> bool {
        if self.refresh_tried {
            return false;
        }
        self.refresh_tried = true;

        match state.twitch_client.refresh_token(&self.refresh).await {
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
                self.access = new_access;
                self.refresh = new_refresh;
                true
            }
            Err(e) => {
                tracing::warn!(broadcaster_id, "Broadcaster token refresh failed: {e}");
                // Only a refresh token Twitch rejected is dead. Dropping the
                // tokens on a network or server hiccup used to cut off a
                // working channel until its owner reconnected it; now the
                // tokens stay and the backoff retries. A dropped token is
                // what the role-config page shows as "needs reconnecting".
                if matches!(e, AppError::TwitchTokenRevoked(_)) {
                    sqlx::query(
                        "UPDATE role_links SET broadcaster_access_token = NULL, broadcaster_refresh_token = NULL \
                         WHERE broadcaster_id = $1",
                    )
                    .bind(broadcaster_id)
                    .execute(&state.pool)
                    .await
                    .ok();
                }
                self.unusable = true;
                false
            }
        }
    }
}

/// Re-check one channel's due rows, one after the other.
async fn refresh_channel(
    state: &AppState,
    broadcaster_id: &str,
    rows: Vec<DueRow>,
    plan: Plan,
    db_trouble: &AtomicBool,
) {
    let token = sqlx::query_as::<_, (String, String)>(
        "SELECT broadcaster_access_token, broadcaster_refresh_token \
         FROM role_links WHERE broadcaster_id = $1 \
           AND broadcaster_access_token IS NOT NULL AND broadcaster_refresh_token IS NOT NULL \
         LIMIT 1",
    )
    .bind(broadcaster_id)
    .fetch_optional(&state.pool)
    .await;

    let mut token = match token {
        Ok(Some((access, refresh))) => ChannelToken {
            access,
            refresh,
            refresh_tried: false,
            unusable: false,
        },
        // Disconnected since the batch was picked. The rows stop being
        // candidates; the backoff covers the moment in between.
        Ok(None) => {
            for row in &rows {
                back_off(state, row, db_trouble).await;
            }
            return;
        }
        Err(e) => {
            tracing::error!(broadcaster_id, "Failed to get broadcaster token: {e}");
            db_trouble.store(true, Ordering::Relaxed);
            return;
        }
    };

    for row in &rows {
        if token.unusable {
            back_off(state, row, db_trouble).await;
            continue;
        }
        if let Err(e) = refresh_row(state, row, &mut token, plan, db_trouble).await {
            back_off(state, row, db_trouble).await;
            tracing::warn!(
                twitch_user_id = row.twitch_user_id,
                broadcaster_id,
                "Twitch API fetch failed: {e}"
            );
        }
    }
}

/// Ask Twitch about one row, store the answer, and re-sync the member's roles
/// when that is called for. An `Err` is a Twitch failure; the caller backs off.
async fn refresh_row(
    state: &AppState,
    row: &DueRow,
    token: &mut ChannelToken,
    plan: Plan,
    db_trouble: &AtomicBool,
) -> Result<(), AppError> {
    let twitch = &state.twitch_client;
    let broadcaster_id = row.broadcaster_id.as_str();
    let user_id = row.twitch_user_id.as_str();
    let full = needs_full_check(row, Utc::now(), plan.slow_secs);

    twitch.wait_for_permit().await;
    let mut follower = twitch
        .check_follower(broadcaster_id, user_id, &token.access)
        .await;
    if is_twitch_unauthorized(&follower) && token.refresh_after_401(state, broadcaster_id).await {
        twitch.wait_for_permit().await;
        follower = twitch
            .check_follower(broadcaster_id, user_id, &token.access)
            .await;
    }
    let follower = follower?;
    let is_following = follower.is_some();
    let followed_at = follower.map(|f| f.followed_at);

    let subscription = if full {
        twitch.wait_for_permit().await;
        let mut sub = twitch
            .check_subscription(broadcaster_id, user_id, &token.access)
            .await;
        if is_twitch_unauthorized(&sub) && token.refresh_after_401(state, broadcaster_id).await {
            twitch.wait_for_permit().await;
            sub = twitch
                .check_subscription(broadcaster_id, user_id, &token.access)
                .await;
        }
        Some(sub?)
    } else {
        None
    };

    let now = Utc::now();
    let stored = match &subscription {
        Some(sub) => {
            let is_subscribed = sub.is_some();
            let sub_tier = sub.as_ref().map(|s| s.tier).unwrap_or(0);
            // Keep a freshly linked user who still doesn't follow/sub on the
            // fast cadence (bounded to FAST_RETRY_WINDOW_SECS after linking) so
            // a just-made / API-lagged follow or sub lands the role within a
            // minute or two. Everyone else uses their tier's cadence.
            let within_fast_window = (now - row.linked_at).num_seconds() < FAST_RETRY_WINDOW_SECS;
            let interval = if !is_following && !is_subscribed && within_fast_window {
                FAST_RETRY_SECS
            } else if row.is_fast {
                plan.fast_secs
            } else {
                plan.slow_secs
            };
            sqlx::query(
                "UPDATE user_channel_cache SET \
                 is_following = $1, followed_at = $2, \
                 is_subscribed = $3, sub_tier = $4, \
                 fetched_at = now(), full_fetched_at = now(), \
                 next_fetch_at = $5, fetch_failures = 0 \
                 WHERE twitch_user_id = $6 AND broadcaster_id = $7",
            )
            .bind(is_following)
            .bind(followed_at)
            .bind(is_subscribed)
            .bind(sub_tier)
            .bind(now + chrono::Duration::seconds(interval))
            .bind(user_id)
            .bind(broadcaster_id)
            .execute(&state.pool)
            .await
        }
        // Follow only. The subscription columns are EventSub's between full
        // checks and are left alone.
        None => {
            sqlx::query(
                "UPDATE user_channel_cache SET \
                 is_following = $1, followed_at = $2, \
                 fetched_at = now(), next_fetch_at = $3, fetch_failures = 0 \
                 WHERE twitch_user_id = $4 AND broadcaster_id = $5",
            )
            .bind(is_following)
            .bind(followed_at)
            .bind(now + chrono::Duration::seconds(plan.fast_secs))
            .bind(user_id)
            .bind(broadcaster_id)
            .execute(&state.pool)
            .await
        }
    };

    if let Err(e) = stored {
        tracing::error!(
            twitch_user_id = row.twitch_user_id,
            broadcaster_id,
            "Failed to update cache: {e}"
        );
        db_trouble.store(true, Ordering::Relaxed);
        return Ok(());
    }

    // A full check always re-syncs: that is the reconciliation. Between full
    // checks an unchanged follow means nothing to do, and the member's guild
    // list is not fetched for it.
    if full || follow_changed(row, is_following, followed_at) {
        let _ = state
            .user_sync_tx
            .send(UserSyncEvent::UserUpdated {
                discord_id: row.discord_id.clone(),
            })
            .await;
    }

    tracing::debug!(
        twitch_user_id = row.twitch_user_id,
        broadcaster_id,
        is_fast = row.is_fast,
        full,
        "Cache refreshed"
    );
    Ok(())
}

/// Push a row that could not be checked back, doubling the wait each time.
async fn back_off(state: &AppState, row: &DueRow, db_trouble: &AtomicBool) {
    let result = sqlx::query(
        "UPDATE user_channel_cache SET fetch_failures = fetch_failures + 1, \
         next_fetch_at = now() + LEAST(INTERVAL '60 seconds' * POWER(2, fetch_failures), INTERVAL '1 hour') \
         WHERE twitch_user_id = $1 AND broadcaster_id = $2",
    )
    .bind(&row.twitch_user_id)
    .bind(&row.broadcaster_id)
    .execute(&state.pool)
    .await;

    if let Err(e) = result {
        tracing::error!(
            twitch_user_id = row.twitch_user_id,
            broadcaster_id = row.broadcaster_id,
            "Failed to back off cache row: {e}"
        );
        db_trouble.store(true, Ordering::Relaxed);
    }
}

/// Check if a Twitch API result is a 401 Unauthorized error.
fn is_twitch_unauthorized<T>(result: &Result<T, AppError>) -> bool {
    matches!(
        result,
        Err(AppError::TwitchApi(msg)) if msg.contains("401") || msg.contains("expired") || msg.contains("Unauthorized")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::condition::ConditionTarget;

    fn row(broadcaster_id: &str, is_fast: bool) -> DueRow {
        DueRow {
            twitch_user_id: "u".into(),
            broadcaster_id: broadcaster_id.into(),
            discord_id: "d".into(),
            linked_at: Utc::now() - chrono::Duration::days(30),
            is_following: true,
            followed_at: None,
            full_fetched_at: None,
            is_fast,
        }
    }

    #[test]
    fn small_tiers_sit_at_the_floors() {
        assert_eq!(plan_intervals(0, 0), Plan::FLOORS);
        // Production on 2026-11-12.
        assert_eq!(plan_intervals(1404, 643), Plan::FLOORS);
    }

    #[test]
    fn a_large_fast_tier_stretches_only_as_far_as_the_budget_needs() {
        // 6,000 fast rows: full checks take 12,000 / 10,800 s = 1.11 calls/s,
        // leaving 2.89 calls/s, so one pass over the tier takes 2,077 s.
        let plan = plan_intervals(6000, 0);
        assert_eq!(plan.slow_secs, SLOW_FLOOR_SECS);
        assert_eq!(plan.fast_secs, 2077);
    }

    #[test]
    fn the_plan_stays_inside_the_budget() {
        for (fast, slow) in [(1404, 643), (6000, 0), (20_000, 30_000), (50_000, 30_000)] {
            let plan = plan_intervals(fast, slow);
            let rate = fast as f64 / plan.fast_secs as f64
                + 2.0 * (fast + slow) as f64 / plan.slow_secs as f64;
            assert!(
                rate <= POLL_BUDGET_PER_SEC + 0.01,
                "{fast}/{slow}: {rate} calls/s with {plan:?}"
            );
            assert!(plan.fast_secs <= plan.slow_secs);
        }
    }

    #[test]
    fn intervals_never_pass_a_day() {
        let plan = plan_intervals(2_000_000, 2_000_000);
        assert_eq!(plan.slow_secs, MAX_INTERVAL_SECS);
        assert_eq!(plan.fast_secs, MAX_INTERVAL_SECS);
    }

    #[test]
    fn the_slow_tier_always_gets_a_full_check() {
        let mut r = row("b", false);
        r.full_fetched_at = Some(Utc::now());
        assert!(needs_full_check(&r, Utc::now(), SLOW_FLOOR_SECS));
    }

    #[test]
    fn the_fast_tier_gets_a_full_check_once_per_slow_interval() {
        let now = Utc::now();
        let mut r = row("b", true);
        assert!(needs_full_check(&r, now, SLOW_FLOOR_SECS), "never checked");
        r.full_fetched_at = Some(now - chrono::Duration::seconds(SLOW_FLOOR_SECS - 1));
        assert!(!needs_full_check(&r, now, SLOW_FLOOR_SECS));
        r.full_fetched_at = Some(now - chrono::Duration::seconds(SLOW_FLOOR_SECS));
        assert!(needs_full_check(&r, now, SLOW_FLOOR_SECS));
    }

    #[test]
    fn an_unfollow_and_a_refollow_are_changes() {
        let at = Utc::now() - chrono::Duration::days(3);
        let mut r = row("b", true);
        r.followed_at = Some(at);
        assert!(!follow_changed(&r, true, Some(at)));
        assert!(follow_changed(&r, false, None), "unfollowed");
        // Unfollowed and followed again between two checks: still following,
        // but the follow age a rule may read has restarted.
        assert!(follow_changed(&r, true, Some(Utc::now())));
    }

    #[test]
    fn sub_second_precision_is_not_a_change() {
        let event = DateTime::parse_from_rfc3339("2026-11-10T19:47:35.171067Z")
            .unwrap()
            .with_timezone(&Utc);
        let helix = DateTime::parse_from_rfc3339("2026-11-10T19:47:35Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut r = row("b", true);
        r.followed_at = Some(event);
        assert!(!follow_changed(&r, true, Some(helix)));
    }

    #[test]
    fn a_batch_groups_by_channel_in_order() {
        let groups = group_by_channel(vec![
            row("a", true),
            row("b", true),
            row("a", false),
            row("c", true),
            row("b", false),
        ]);
        let shape: Vec<(&str, usize)> = groups
            .iter()
            .map(|(b, rows)| (b.as_str(), rows.len()))
            .collect();
        assert_eq!(shape, vec![("a", 2), ("b", 2), ("c", 1)]);
        assert!(groups[0].1[0].is_fast && !groups[0].1[1].is_fast);
    }

    #[test]
    fn the_tier_query_names_the_follow_targets() {
        for target in [ConditionTarget::IsFollower, ConditionTarget::FollowAgeDays] {
            let needle = format!("@.target == \"{}\"", target.as_str());
            assert!(CANDIDATES.contains(&needle), "missing {needle}");
        }
        for target in [ConditionTarget::IsSubscriber, ConditionTarget::SubTier] {
            assert!(!CANDIDATES.contains(&format!("\"{}\"", target.as_str())));
        }
    }
}
