//! Public "all users" listing — every linked member with their relationship
//! to a Twitch channel connected to this guild. Shows username + follower /
//! subscriber / tier, so admins can see who's in their server at a glance.
//!
//! Gated by `guild_settings.view_permission`:
//!   * 'disabled' — nobody (page renders an explanatory notice)
//!   * 'managers' — Manage-Server only
//!   * 'members'  — any member of the guild
//!
//! Only members who linked their Twitch account appear (we only have a
//! username for linked users, and surfacing only opted-in members is the
//! privacy-respecting default). On 401 the page renders an in-page
//! "Login with Discord" prompt — it never auto-redirects.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use axum_extra::extract::cookie::CookieJar;
use serde_json::{json, Value};

use crate::error::AppError;
use crate::services::auth::{extract_bearer, guild_members, guild_permission, require_guild_admin};
use crate::services::csrf;
use crate::AppState;

const USERS_PAGE: &str = include_str!("../../templates/users.html");

pub async fn users_page(
    State(state): State<Arc<AppState>>,
    Path(guild_id): Path<String>,
) -> impl IntoResponse {
    let html = USERS_PAGE
        .replace("{{BASE_URL}}", &state.config.base_url)
        .replace("{{GUILD_ID}}", &guild_id);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        html,
    )
}

#[allow(clippy::type_complexity)]
pub async fn users_data(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path(guild_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let view_permission: String =
        sqlx::query_scalar("SELECT view_permission FROM guild_settings WHERE guild_id = $1")
            .bind(&guild_id)
            .fetch_optional(&state.pool)
            .await?
            .unwrap_or_else(|| "managers".to_string());

    if view_permission == "disabled" {
        return Err(AppError::Forbidden(
            "The user list is disabled for this server.".into(),
        ));
    }

    let perm = guild_permission(&state, &jar, &guild_id).await?;
    if !perm.is_member {
        return Err(AppError::Forbidden(
            "You're not a member of this server.".into(),
        ));
    }
    if view_permission == "managers" && !perm.is_manager {
        return Err(AppError::Forbidden(
            "This list is visible to server managers only.".into(),
        ));
    }

    // "Who is in this guild" comes from the Auth Gateway, NOT from a local
    // table. A member who linked their Twitch account must appear the instant
    // they link — before any broadcaster is connected and before any
    // follow/sub event lands — with the relationship columns simply blank. One
    // user-cookie call returns both the member filter and the guild name.
    let (member_ids, guild_name) = guild_members(&state, &jar, &guild_id).await?;

    // One row per linked member who is a current member of this guild.
    // `user_channel_cache` is LEFT-joined and scoped to the broadcasters this
    // guild has connected (via role_links), then collapsed (OR / max / min) so
    // a member linked to several of the guild's channels appears once. A member
    // with no relation row at all still appears, with flags false / tier 0.
    let rows = sqlx::query_as::<
        _,
        (
            String,
            Option<String>,
            String,
            bool,
            bool,
            i32,
            Option<chrono::DateTime<chrono::Utc>>,
            chrono::DateTime<chrono::Utc>,
        ),
    >(
        "SELECT la.discord_id, \
                la.discord_name, \
                la.twitch_login, \
                COALESCE(bool_or(ucc.is_following),  false) AS is_follower, \
                COALESCE(bool_or(ucc.is_subscribed), false) AS is_subscriber, \
                COALESCE(max(ucc.sub_tier), 0) AS sub_tier, \
                min(ucc.followed_at) AS followed_at, \
                la.linked_at \
         FROM linked_accounts la \
         LEFT JOIN role_links rl \
                ON rl.guild_id = $1 AND rl.broadcaster_id IS NOT NULL \
         LEFT JOIN user_channel_cache ucc \
                ON ucc.twitch_user_id = la.twitch_user_id \
               AND ucc.broadcaster_id = rl.broadcaster_id \
         WHERE la.discord_id = ANY($2) \
         GROUP BY la.discord_id, la.discord_name, la.twitch_login, la.linked_at \
         ORDER BY la.twitch_login ASC \
         LIMIT 1000",
    )
    .bind(&guild_id)
    .bind(&member_ids)
    .fetch_all(&state.pool)
    .await?;

    let users = rows
        .into_iter()
        .map(
            |(
                discord_id,
                discord_name,
                username,
                is_follower,
                is_subscriber,
                sub_tier,
                followed_at,
                linked_at,
            )| {
                json!({
                    "discord_id": discord_id,
                    "discord_name": discord_name,
                    "username": username,
                    "is_follower": is_follower,
                    "is_subscriber": is_subscriber,
                    // Friendly 1/2/3 scale (0 when not subscribed).
                    "sub_tier": sub_tier / 1000,
                    "followed_at": followed_at.map(|x| x.to_rfc3339()),
                    "linked_at": linked_at.to_rfc3339(),
                })
            },
        )
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "guild_id": guild_id,
        "guild_name": guild_name,
        "count": users.len(),
        "users": users,
    })))
}

// ---------------------------------------------------------------------
// POST /admin/{guild_id}/view-permission  (manager-only)
// ---------------------------------------------------------------------

#[derive(serde::Deserialize)]
pub struct ViewPermBody {
    pub view_permission: String,
}

pub async fn set_view_permission(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    headers: HeaderMap,
    Path(guild_id): Path<String>,
    Json(body): Json<ViewPermBody>,
) -> Result<Json<Value>, AppError> {
    if extract_bearer(&headers).is_none() {
        csrf::verify_origin(&headers, &state.config.allowed_origins)?;
    }
    require_guild_admin(&state, &jar, &headers, &guild_id).await?;

    let vp = match body.view_permission.as_str() {
        "disabled" | "managers" | "members" => body.view_permission.as_str(),
        other => {
            return Err(AppError::BadRequest(format!(
                "Unknown view_permission '{other}' (expected disabled|managers|members)."
            )))
        }
    };

    sqlx::query(
        "INSERT INTO guild_settings (guild_id, view_permission, updated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (guild_id) DO UPDATE SET view_permission = EXCLUDED.view_permission, \
                                              updated_at = now()",
    )
    .bind(&guild_id)
    .bind(vp)
    .execute(&state.pool)
    .await?;

    Ok(Json(json!({ "success": true, "view_permission": vp })))
}
