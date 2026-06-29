use std::collections::HashSet;

use futures_util::stream::{self, StreamExt};
use serde_json::Value;
use sqlx::PgPool;

use crate::error::AppError;
use crate::models::rule::RuleTree;
use crate::services::auth_gateway;
use crate::services::condition_eval::{evaluate, CacheData};
use crate::services::rule_sql::{self, Bind};
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

/// Default facts for a viewer with no cache row yet — all false / unknown, so
/// channel-scoped conditions fail closed exactly like the SQL builder's
/// COALESCE/NULL handling.
fn default_facts() -> CacheData {
    CacheData {
        is_following: false,
        followed_at: None,
        is_subscribed: false,
        sub_tier: 0,
    }
}

/// Whether a viewer qualifies for one role link, given the link's rule tree,
/// its connected broadcaster (if any), and the viewer's cached facts.
fn qualifies(tree: &RuleTree, broadcaster_id: Option<&str>, facts: Option<&CacheData>) -> bool {
    if tree.grant_on_any_relation {
        return true;
    }
    if broadcaster_id.is_none() || tree.groups.is_empty() {
        return false;
    }
    let fallback = default_facts();
    evaluate(tree, facts.unwrap_or(&fallback))
}

/// Sync roles for a single user across all guilds.
/// Evaluates the rule tree locally, then executes RoleLogic API calls concurrently.
pub async fn sync_for_user(discord_id: &str, state: &AppState) -> Result<(), AppError> {
    let pool = &state.pool;
    let rl_client = &state.rl_client;

    let twitch_user_id = sqlx::query_scalar::<_, String>(
        "SELECT twitch_user_id FROM linked_accounts WHERE discord_id = $1",
    )
    .bind(discord_id)
    .fetch_optional(pool)
    .await?;

    let Some(twitch_user_id) = twitch_user_id else {
        return Ok(());
    };

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

    // All role links in the user's guilds. No broadcaster filter — a
    // `grant_on_any_relation` link needs no channel.
    let role_links = sqlx::query_as::<_, (String, String, String, Value, Option<String>)>(
        "SELECT rl.guild_id, rl.role_id, rl.api_token, rl.rule_tree, rl.broadcaster_id \
         FROM role_links rl WHERE rl.guild_id = ANY($1)",
    )
    .bind(&guild_ids[..])
    .fetch_all(pool)
    .await?;

    if role_links.is_empty() {
        return Ok(());
    }

    // Cached (broadcaster_id → facts) for this user across all channels.
    let cache_rows = sqlx::query_as::<
        _,
        (
            String,
            bool,
            Option<chrono::DateTime<chrono::Utc>>,
            bool,
            i32,
        ),
    >(
        "SELECT broadcaster_id, is_following, followed_at, is_subscribed, sub_tier \
         FROM user_channel_cache WHERE twitch_user_id = $1",
    )
    .bind(&twitch_user_id)
    .fetch_all(pool)
    .await?;

    let cache_map: std::collections::HashMap<String, CacheData> = cache_rows
        .into_iter()
        .map(
            |(bid, is_following, followed_at, is_subscribed, sub_tier)| {
                (
                    bid,
                    CacheData {
                        is_following,
                        followed_at,
                        is_subscribed,
                        sub_tier,
                    },
                )
            },
        )
        .collect();

    let existing: HashSet<(String, String)> = sqlx::query_as::<_, (String, String)>(
        "SELECT guild_id, role_id FROM role_assignments WHERE discord_id = $1",
    )
    .bind(discord_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

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
    for (guild_id, role_id, api_token, rule_tree, broadcaster_id) in &role_links {
        let tree: RuleTree = serde_json::from_value(rule_tree.clone()).unwrap_or_default();
        let facts = broadcaster_id.as_deref().and_then(|bid| cache_map.get(bid));
        let q = qualifies(&tree, broadcaster_id.as_deref(), facts);
        let currently_assigned = existing.contains(&(guild_id.clone(), role_id.clone()));
        match (q, currently_assigned) {
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
                            Err(AppError::RoleLinkNotFound) => {
                                delete_orphan_role_link(&guild_id, &role_id, &pool).await;
                                return;
                            }
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
                        match rl_client
                            .remove_user(&guild_id, &role_id, &discord_id, &api_token)
                            .await
                        {
                            Err(AppError::RoleLinkNotFound) => {
                                delete_orphan_role_link(&guild_id, &role_id, &pool).await;
                                return;
                            }
                            Err(e) => {
                                tracing::error!(
                                    guild_id, role_id, discord_id,
                                    "Failed to remove user from role: {e}"
                                );
                                return;
                            }
                            Ok(_) => {}
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

/// Re-evaluate all users for a specific role link (after config change).
/// Pushes the rule down into SQL and replaces the entire user list atomically.
pub async fn sync_for_role_link(
    guild_id: &str,
    role_id: &str,
    state: &AppState,
) -> Result<(), AppError> {
    let pool = &state.pool;
    let rl_client = &state.rl_client;

    let link = sqlx::query_as::<_, (String, Value, Option<String>)>(
        "SELECT api_token, rule_tree, broadcaster_id FROM role_links WHERE guild_id = $1 AND role_id = $2",
    )
    .bind(guild_id)
    .bind(role_id)
    .fetch_optional(pool)
    .await?;

    let Some((api_token, rule_tree, broadcaster_id)) = link else {
        return Ok(());
    };
    let tree: RuleTree = serde_json::from_value(rule_tree).unwrap_or_default();

    // Unconfigured, or a channel-scoped rule with no broadcaster connected →
    // grant to nobody. (grant_on_any_relation is channel-agnostic and handled
    // below, so it does NOT hit this clear path.)
    if !tree.grant_on_any_relation && (tree.groups.is_empty() || broadcaster_id.is_none()) {
        return clear_role(guild_id, role_id, &api_token, state).await;
    }

    let member_ids = auth_gateway::fetch_guild_member_ids(
        &state.http,
        &state.config.auth_gateway_url,
        &state.config.internal_api_key,
        guild_id,
    )
    .await?;

    if member_ids.is_empty() {
        return clear_role(guild_id, role_id, &api_token, state).await;
    }

    // RoleLogic user limit (caps the qualifying set).
    let (_user_count, user_limit) =
        match rl_client.get_user_info(guild_id, role_id, &api_token).await {
            Ok(v) => v,
            Err(AppError::RoleLinkNotFound) => {
                delete_orphan_role_link(guild_id, role_id, pool).await;
                return Ok(());
            }
            Err(_) => (0, 100),
        };

    // Build the qualifying-id query. Channel-agnostic grant skips the cache
    // join entirely; otherwise we LEFT JOIN this broadcaster's cache and push
    // the DNF rule into SQL.
    let qualifying_ids: Vec<String> = if tree.grant_on_any_relation {
        sqlx::query_scalar::<_, String>(
            "SELECT la.discord_id FROM linked_accounts la \
             WHERE la.discord_id = ANY($1::text[]) ORDER BY la.linked_at ASC LIMIT $2",
        )
        .bind(&member_ids)
        .bind(user_limit as i64)
        .fetch_all(pool)
        .await?
    } else {
        let broadcaster_id = broadcaster_id
            .as_deref()
            .expect("broadcaster bound for channel-scoped rule");
        let (rule_where, binds) = rule_sql::build_rule_where(&tree, 2);
        let limit_idx = 2 + binds.len() + 1;
        let query = format!(
            "SELECT la.discord_id FROM linked_accounts la \
             LEFT JOIN user_channel_cache ucc \
               ON ucc.twitch_user_id = la.twitch_user_id AND ucc.broadcaster_id = $1 \
             WHERE la.discord_id = ANY($2::text[]) AND ({rule_where}) \
             ORDER BY la.linked_at ASC LIMIT ${limit_idx}"
        );
        let mut q = sqlx::query_scalar::<_, String>(&query)
            .bind(broadcaster_id)
            .bind(&member_ids);
        for b in &binds {
            q = match b {
                Bind::Bool(v) => q.bind(*v),
                Bind::Int(v) => q.bind(*v),
            };
        }
        q = q.bind(user_limit as i64);
        q.fetch_all(pool).await?
    };

    if !qualifying_ids.is_empty() && qualifying_ids.len() == user_limit {
        tracing::warn!(
            guild_id,
            role_id,
            user_limit,
            "Role link user limit reached: at least {user_limit} users qualify"
        );
    }

    match rl_client
        .upload_users(guild_id, role_id, &qualifying_ids, &api_token)
        .await
    {
        Ok(_) => {}
        Err(AppError::RoleLinkNotFound) => {
            delete_orphan_role_link(guild_id, role_id, pool).await;
            return Ok(());
        }
        Err(e) => return Err(e),
    }

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

/// Clear all assignments for a role link (RoleLogic + local mirror).
async fn clear_role(
    guild_id: &str,
    role_id: &str,
    api_token: &str,
    state: &AppState,
) -> Result<(), AppError> {
    match state
        .rl_client
        .upload_users(guild_id, role_id, &[], api_token)
        .await
    {
        Ok(_) => {}
        Err(AppError::RoleLinkNotFound) => {
            delete_orphan_role_link(guild_id, role_id, &state.pool).await;
            return Ok(());
        }
        Err(e) => return Err(e),
    }
    sqlx::query("DELETE FROM role_assignments WHERE guild_id = $1 AND role_id = $2")
        .bind(guild_id)
        .bind(role_id)
        .execute(&state.pool)
        .await?;
    Ok(())
}

/// Remove a user from all role assignments (after account unlink).
pub async fn remove_all_assignments(discord_id: &str, state: &AppState) -> Result<(), AppError> {
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
        match rl_client
            .remove_user(guild_id, role_id, discord_id, api_token)
            .await
        {
            Ok(_) => {}
            Err(AppError::RoleLinkNotFound) => {
                delete_orphan_role_link(guild_id, role_id, pool).await;
            }
            Err(e) => {
                tracing::error!(
                    guild_id,
                    role_id,
                    discord_id,
                    "Failed to remove user during unlink: {e}"
                );
            }
        }
    }

    sqlx::query("DELETE FROM role_assignments WHERE discord_id = $1")
        .bind(discord_id)
        .execute(pool)
        .await?;

    Ok(())
}

/// Delete a role_link the RoleLogic API reports as gone (403 Invalid or
/// revoked token). CASCADE clears role_assignments. Best-effort.
async fn delete_orphan_role_link(guild_id: &str, role_id: &str, pool: &PgPool) {
    tracing::warn!(
        guild_id,
        role_id,
        "Role link not found on RoleLogic; removing orphaned local row"
    );
    if let Err(e) = sqlx::query("DELETE FROM role_links WHERE guild_id = $1 AND role_id = $2")
        .bind(guild_id)
        .bind(role_id)
        .execute(pool)
        .await
    {
        tracing::error!(guild_id, role_id, "Failed to delete orphan role_link: {e}");
    }
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
