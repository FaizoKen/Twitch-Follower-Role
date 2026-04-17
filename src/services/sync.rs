use std::collections::HashSet;

use futures_util::stream::{self, StreamExt};
use sqlx::PgPool;

use crate::error::AppError;
use crate::models::condition::TwitchConditions;
use crate::services::auth_gateway;
use crate::services::condition_eval::{evaluate, CacheData};
use crate::AppState;

/// Events sent to the user sync worker (lightweight, per-user).
#[derive(Debug, Clone)]
pub enum UserSyncEvent {
    UserUpdated { discord_id: String },
    AccountLinked { discord_id: String },
    AccountUnlinked { discord_id: String },
}

/// Events sent to the config sync worker (heavy, per-role-link).
#[derive(Debug, Clone)]
pub struct ConfigSyncEvent {
    pub guild_id: String,
    pub role_id: String,
}

/// Sync roles for a single user across all guilds.
/// Evaluates conditions locally, then executes RoleLogic API calls concurrently.
pub async fn sync_for_user(
    discord_id: &str,
    state: &AppState,
) -> Result<(), AppError> {
    let pool = &state.pool;
    let rl_client = &state.rl_client;

    // Get user's Twitch ID
    let twitch_user_id = sqlx::query_scalar::<_, String>(
        "SELECT twitch_user_id FROM linked_accounts WHERE discord_id = $1",
    )
    .bind(discord_id)
    .fetch_optional(pool)
    .await?;

    let Some(twitch_user_id) = twitch_user_id else {
        return Ok(());
    };

    // Get guild IDs from Auth Gateway
    let guild_ids = auth_gateway::fetch_user_guild_ids(
        &state.http,
        &state.config.auth_gateway_url,
        &state.config.internal_api_key,
        discord_id,
    )
    .await?;

    if guild_ids.is_empty() {
        return Ok(());
    }

    // Get role links only for guilds this user is a member of,
    // and that have a broadcaster connected
    let role_links = sqlx::query_as::<_, (String, String, String, sqlx::types::Json<TwitchConditions>, String)>(
        "SELECT rl.guild_id, rl.role_id, rl.api_token, rl.conditions, rl.broadcaster_id \
         FROM role_links rl \
         WHERE rl.guild_id = ANY($1) AND rl.broadcaster_id IS NOT NULL",
    )
    .bind(&guild_ids[..])
    .fetch_all(pool)
    .await?;

    if role_links.is_empty() {
        return Ok(());
    }

    // Get all cached data for this user across all broadcasters
    let cache_rows = sqlx::query_as::<_, (String, bool, Option<chrono::DateTime<chrono::Utc>>, bool, i32)>(
        "SELECT broadcaster_id, is_following, followed_at, is_subscribed, sub_tier \
         FROM user_channel_cache WHERE twitch_user_id = $1",
    )
    .bind(&twitch_user_id)
    .fetch_all(pool)
    .await?;

    let cache_map: std::collections::HashMap<String, CacheData> = cache_rows
        .into_iter()
        .map(|(bid, is_following, followed_at, is_subscribed, sub_tier)| {
            (
                bid,
                CacheData {
                    is_following,
                    followed_at,
                    is_subscribed,
                    sub_tier,
                },
            )
        })
        .collect();

    // Get existing assignments
    let existing: HashSet<(String, String)> = sqlx::query_as::<_, (String, String)>(
        "SELECT guild_id, role_id FROM role_assignments WHERE discord_id = $1",
    )
    .bind(discord_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    // Phase 1: evaluate conditions locally (no I/O)
    enum Action {
        Add {
            guild_id: String,
            role_id: String,
            api_token: String,
        },
        Remove {
            guild_id: String,
            role_id: String,
            api_token: String,
        },
    }

    let mut actions: Vec<Action> = Vec::new();
    for (guild_id, role_id, api_token, conditions, broadcaster_id) in &role_links {
        let cache = cache_map.get(broadcaster_id.as_str());
        let qualifies = match cache {
            Some(c) => evaluate(conditions, c),
            None => false, // No cache data yet
        };
        let currently_assigned = existing.contains(&(guild_id.clone(), role_id.clone()));
        match (qualifies, currently_assigned) {
            (true, false) => actions.push(Action::Add {
                guild_id: guild_id.clone(),
                role_id: role_id.clone(),
                api_token: api_token.clone(),
            }),
            (false, true) => actions.push(Action::Remove {
                guild_id: guild_id.clone(),
                role_id: role_id.clone(),
                api_token: api_token.clone(),
            }),
            _ => {}
        }
    }

    if actions.is_empty() {
        return Ok(());
    }

    // Phase 2: execute API calls concurrently (max 10 parallel)
    let discord_id_owned = discord_id.to_string();
    stream::iter(actions)
        .for_each_concurrent(10, |action| {
            let pool = pool.clone();
            let rl_client = rl_client.clone();
            let discord_id = discord_id_owned.clone();
            async move {
                match action {
                    Action::Add {
                        guild_id,
                        role_id,
                        api_token,
                    } => {
                        match rl_client
                            .add_user(&guild_id, &role_id, &discord_id, &api_token)
                            .await
                        {
                            Err(AppError::UserLimitReached { limit }) => {
                                tracing::warn!(
                                    guild_id, role_id, discord_id, limit,
                                    "Cannot add user: role link user limit reached"
                                );
                                return;
                            }
                            Err(e) => {
                                tracing::error!(
                                    guild_id, role_id, discord_id,
                                    "Failed to add user to role: {e}"
                                );
                                return;
                            }
                            Ok(_) => {}
                        }
                        if let Err(e) = sqlx::query(
                            "INSERT INTO role_assignments (guild_id, role_id, discord_id) \
                             VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                        )
                        .bind(&guild_id)
                        .bind(&role_id)
                        .bind(&discord_id)
                        .execute(&pool)
                        .await
                        {
                            tracing::error!(guild_id, role_id, discord_id, "Failed to insert assignment: {e}");
                        }
                    }
                    Action::Remove {
                        guild_id,
                        role_id,
                        api_token,
                    } => {
                        if let Err(e) = rl_client
                            .remove_user(&guild_id, &role_id, &discord_id, &api_token)
                            .await
                        {
                            tracing::error!(
                                guild_id, role_id, discord_id,
                                "Failed to remove user from role: {e}"
                            );
                            return;
                        }
                        if let Err(e) = sqlx::query(
                            "DELETE FROM role_assignments WHERE guild_id = $1 AND role_id = $2 AND discord_id = $3",
                        )
                        .bind(&guild_id)
                        .bind(&role_id)
                        .bind(&discord_id)
                        .execute(&pool)
                        .await
                        {
                            tracing::error!(guild_id, role_id, discord_id, "Failed to delete assignment: {e}");
                        }
                    }
                }
            }
        })
        .await;

    Ok(())
}

/// Build a SQL WHERE clause from TwitchConditions for SQL-side filtering.
/// All conditions map to columns on user_channel_cache (no JSONB parsing).
fn build_condition_where(conditions: &TwitchConditions) -> (String, Vec<ConditionBind>) {
    let mut clauses: Vec<String> = Vec::new();
    let mut binds: Vec<ConditionBind> = Vec::new();

    if conditions.require_follower {
        clauses.push("ucc.is_following = true".to_string());
        if conditions.min_follow_days > 0 {
            let idx = binds.len() + 1;
            clauses.push(format!(
                "ucc.followed_at <= now() - make_interval(days => ${idx})"
            ));
            binds.push(ConditionBind::Int(conditions.min_follow_days));
        }
    }

    if conditions.require_subscriber {
        clauses.push("ucc.is_subscribed = true".to_string());
        if conditions.min_sub_tier > 0 {
            let idx = binds.len() + 1;
            clauses.push(format!("ucc.sub_tier >= ${idx}"));
            binds.push(ConditionBind::Int((conditions.min_sub_tier as i64) * 1000));
        }
    }

    if clauses.is_empty() {
        return ("TRUE".to_string(), vec![]);
    }

    (clauses.join(" AND "), binds)
}

enum ConditionBind {
    Int(i64),
}

/// Re-evaluate all users for a specific role link (after config change).
/// Uses SQL-side filtering on user_channel_cache columns.
/// Uses atomic PUT to replace entire user list.
pub async fn sync_for_role_link(
    guild_id: &str,
    role_id: &str,
    state: &AppState,
) -> Result<(), AppError> {
    let pool = &state.pool;
    let rl_client = &state.rl_client;

    let link = sqlx::query_as::<_, (String, sqlx::types::Json<TwitchConditions>, Option<String>)>(
        "SELECT api_token, conditions, broadcaster_id FROM role_links WHERE guild_id = $1 AND role_id = $2",
    )
    .bind(guild_id)
    .bind(role_id)
    .fetch_optional(pool)
    .await?;

    let Some((api_token, conditions, broadcaster_id)) = link else {
        return Ok(());
    };

    let Some(broadcaster_id) = broadcaster_id else {
        // No broadcaster connected, clear assignments
        rl_client.replace_users(guild_id, role_id, &[], &api_token).await?;
        sqlx::query("DELETE FROM role_assignments WHERE guild_id = $1 AND role_id = $2")
            .bind(guild_id)
            .bind(role_id)
            .execute(pool)
            .await?;
        return Ok(());
    };

    // Role is unconfigured when neither follower nor subscriber is required.
    if !conditions.require_follower && !conditions.require_subscriber {
        rl_client.replace_users(guild_id, role_id, &[], &api_token).await?;
        sqlx::query("DELETE FROM role_assignments WHERE guild_id = $1 AND role_id = $2")
            .bind(guild_id)
            .bind(role_id)
            .execute(pool)
            .await?;
        return Ok(());
    }

    let member_ids = auth_gateway::fetch_guild_member_ids(
        &state.http,
        &state.config.auth_gateway_url,
        &state.config.internal_api_key,
        guild_id,
    )
    .await?;

    if member_ids.is_empty() {
        rl_client.replace_users(guild_id, role_id, &[], &api_token).await?;
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM role_assignments WHERE guild_id = $1 AND role_id = $2")
            .bind(guild_id).bind(role_id)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(());
    }

    // Query the user limit from RoleLogic
    let (_user_count, user_limit) = rl_client
        .get_user_info(guild_id, role_id, &api_token)
        .await
        .unwrap_or((0, 100));

    // Build SQL WHERE clause from conditions
    let (where_clause, binds) = build_condition_where(&conditions);

    // Dynamic bind indexes: binds... + broadcaster_id + member_ids + limit
    let broadcaster_bind_idx = binds.len() + 1;
    let members_bind_idx = binds.len() + 2;
    let limit_bind_idx = binds.len() + 3;

    let query_str = format!(
        "SELECT la.discord_id \
         FROM linked_accounts la \
         JOIN user_channel_cache ucc ON ucc.twitch_user_id = la.twitch_user_id \
           AND ucc.broadcaster_id = ${broadcaster_bind_idx} \
         WHERE la.discord_id = ANY(${members_bind_idx}::text[]) \
           AND ({where_clause}) \
         ORDER BY la.linked_at ASC \
         LIMIT ${limit_bind_idx}",
    );

    let qualifying_ids = exec_condition_query(
        &query_str,
        &binds,
        &broadcaster_id,
        &member_ids,
        user_limit,
        pool,
    )
    .await?;

    // Check if more users qualify than the limit allows (logging only)
    if !qualifying_ids.is_empty() && qualifying_ids.len() == user_limit {
        let count_query = format!(
            "SELECT COUNT(*) FROM linked_accounts la \
             JOIN user_channel_cache ucc ON ucc.twitch_user_id = la.twitch_user_id \
               AND ucc.broadcaster_id = ${broadcaster_bind_idx} \
             WHERE la.discord_id = ANY(${members_bind_idx}::text[]) \
               AND ({where_clause})",
        );
        let total: i64 = exec_condition_count(&count_query, &binds, &broadcaster_id, &member_ids, pool)
            .await
            .unwrap_or(qualifying_ids.len() as i64);
        if total as usize > user_limit {
            tracing::warn!(
                guild_id, role_id, total, user_limit,
                "Role link user limit reached: {total} users qualify but limit is {user_limit}"
            );
        }
    }

    // Atomic replace
    rl_client
        .replace_users(guild_id, role_id, &qualifying_ids, &api_token)
        .await?;

    // Update local assignments atomically
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM role_assignments WHERE guild_id = $1 AND role_id = $2")
        .bind(guild_id)
        .bind(role_id)
        .execute(&mut *tx)
        .await?;

    if !qualifying_ids.is_empty() {
        sqlx::query(
            "INSERT INTO role_assignments (guild_id, role_id, discord_id) \
             SELECT $1, $2, UNNEST($3::text[])",
        )
        .bind(guild_id)
        .bind(role_id)
        .bind(&qualifying_ids)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn exec_condition_query(
    query: &str,
    binds: &[ConditionBind],
    broadcaster_id: &str,
    member_ids: &[String],
    limit: usize,
    pool: &PgPool,
) -> Result<Vec<String>, AppError> {
    let mut q = sqlx::query_scalar::<_, String>(query);
    for bind in binds {
        q = match bind {
            ConditionBind::Int(v) => q.bind(*v),
        };
    }
    q = q.bind(broadcaster_id);
    q = q.bind(member_ids);
    q = q.bind(limit as i64);

    Ok(q.fetch_all(pool).await?)
}

async fn exec_condition_count(
    query: &str,
    binds: &[ConditionBind],
    broadcaster_id: &str,
    member_ids: &[String],
    pool: &PgPool,
) -> Result<i64, AppError> {
    let mut q = sqlx::query_scalar::<_, i64>(query);
    for bind in binds {
        q = match bind {
            ConditionBind::Int(v) => q.bind(*v),
        };
    }
    q = q.bind(broadcaster_id);
    q = q.bind(member_ids);
    Ok(q.fetch_one(pool).await?)
}

/// Remove a user from all role assignments (after account unlink).
pub async fn remove_all_assignments(
    discord_id: &str,
    state: &AppState,
) -> Result<(), AppError> {
    let pool = &state.pool;
    let rl_client = &state.rl_client;

    let assignments = sqlx::query_as::<_, (String, String, String)>(
        "SELECT ra.guild_id, ra.role_id, rl.api_token \
         FROM role_assignments ra \
         JOIN role_links rl ON rl.guild_id = ra.guild_id AND rl.role_id = ra.role_id \
         WHERE ra.discord_id = $1",
    )
    .bind(discord_id)
    .fetch_all(pool)
    .await?;

    for (guild_id, role_id, api_token) in &assignments {
        if let Err(e) = rl_client
            .remove_user(guild_id, role_id, discord_id, api_token)
            .await
        {
            tracing::error!(
                guild_id, role_id, discord_id,
                "Failed to remove user during unlink: {e}"
            );
        }
    }

    sqlx::query("DELETE FROM role_assignments WHERE discord_id = $1")
        .bind(discord_id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Populate user_channel_cache rows for a newly linked user across all active broadcasters.
pub async fn populate_cache_for_user(
    discord_id: &str,
    twitch_user_id: &str,
    state: &AppState,
) -> Result<(), AppError> {
    let guild_ids = auth_gateway::fetch_user_guild_ids(
        &state.http,
        &state.config.auth_gateway_url,
        &state.config.internal_api_key,
        discord_id,
    )
    .await?;

    if guild_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO user_channel_cache (twitch_user_id, broadcaster_id) \
         SELECT $1, rl.broadcaster_id \
         FROM role_links rl \
         WHERE rl.guild_id = ANY($2) AND rl.broadcaster_id IS NOT NULL \
         ON CONFLICT DO NOTHING",
    )
    .bind(twitch_user_id)
    .bind(&guild_ids[..])
    .execute(&state.pool)
    .await?;

    Ok(())
}

/// Populate user_channel_cache rows for all linked users when a broadcaster connects.
pub async fn populate_cache_for_broadcaster(
    broadcaster_id: &str,
    guild_id: &str,
    state: &AppState,
) -> Result<(), AppError> {
    let member_ids = auth_gateway::fetch_guild_member_ids(
        &state.http,
        &state.config.auth_gateway_url,
        &state.config.internal_api_key,
        guild_id,
    )
    .await?;

    if member_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO user_channel_cache (twitch_user_id, broadcaster_id) \
         SELECT la.twitch_user_id, $1 \
         FROM linked_accounts la \
         WHERE la.discord_id = ANY($2::text[]) \
         ON CONFLICT DO NOTHING",
    )
    .bind(broadcaster_id)
    .bind(&member_ids[..])
    .execute(&state.pool)
    .await?;

    Ok(())
}
