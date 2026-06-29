//! Admin iframe routes: the RoleLogic-embedded rule-builder page plus its
//! data/save/preview XHRs and the per-role-link broadcaster connect/disconnect
//! actions.
//!
//! Dual-mode auth (Convention 45): the page is entered either with a RoleLogic
//! `?rl_token=` (iframe) — verified locally, then a short-lived iframe-session
//! `Bearer ifs:…` is minted and used by every subsequent XHR — or by direct
//! navigation authenticated with the `rl_session` cookie + an Auth-Gateway
//! manager check.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use axum_extra::extract::cookie::CookieJar;
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::AppError;
use crate::models::condition::{ConditionOperator, ConditionTarget, TargetKind};
use crate::models::rule::{RuleTree, MAX_CONDITIONS_PER_GROUP, MAX_GROUPS};
use crate::routes::broadcaster;
use crate::services::auth::{extract_bearer, require_guild_admin, require_manager};
use crate::services::rule_sql::{self, Bind};
use crate::services::rule_validator::{self, RuleTreeBody};
use crate::services::security_headers::admin_iframe_csp;
use crate::services::sync::ConfigSyncEvent;
use crate::services::{auth_gateway, csrf, rl_token};
use crate::AppState;

const ROLE_CONFIG_TEMPLATE: &str = include_str!("../../templates/role_config.html");

/// Twitch OAuth scopes the broadcaster grants so we can read followers and
/// subscriptions for their channel.
const BROADCASTER_SCOPES: &str = "moderator:read:followers channel:read:subscriptions";

/// 32-char lowercase alphanumeric OAuth `state` token.
fn new_state_token() -> String {
    let mut rng = rand::thread_rng();
    (0..32)
        .map(|_| {
            let idx = rng.gen_range(0..36);
            if idx < 10 {
                (b'0' + idx) as char
            } else {
                (b'a' + idx - 10) as char
            }
        })
        .collect()
}

// ---------------------------------------------------------------------
// POST /admin/{guild_id}/role/{role_id}/connect
// Starts the broadcaster OAuth flow for THIS role link. Returns the Twitch
// authorize URL as JSON so the iframe can open it in a new tab.
// ---------------------------------------------------------------------

pub async fn broadcaster_connect(
    State(state): State<Arc<AppState>>,
    Path((guild_id, role_id)): Path<(String, String)>,
    jar: CookieJar,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    if extract_bearer(&headers).is_none() {
        csrf::verify_origin(&headers, &state.config.allowed_origins)?;
    }
    let access = require_role_config_access(&state, &jar, &headers, &guild_id, &role_id).await?;
    // Read-only sessions (a developer impersonating the user) may view but not
    // write — the server-side half of the read-only contract.
    if access.read_only {
        return Err(AppError::Forbidden(
            "This configuration is read-only while impersonating a user.".into(),
        ));
    }

    // The role link must exist before we attach a channel to it.
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT id FROM role_links WHERE guild_id = $1 AND role_id = $2")
            .bind(&guild_id)
            .bind(&role_id)
            .fetch_optional(&state.pool)
            .await?;
    if exists.is_none() {
        return Err(AppError::NotFound(
            "This role link doesn't exist. Has it been added in RoleLogic?".into(),
        ));
    }

    let state_token = new_state_token();
    sqlx::query(
        "INSERT INTO oauth_states (state, redirect_data, expires_at) \
         VALUES ($1, $2, now() + interval '10 minutes')",
    )
    .bind(&state_token)
    .bind(json!({"type": "broadcaster", "guild_id": guild_id, "role_id": role_id}))
    .execute(&state.pool)
    .await?;

    let redirect_uri = state.config.broadcaster_oauth_redirect_uri();
    let authorize_url = format!(
        "https://id.twitch.tv/oauth2/authorize?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}&force_verify=true",
        state.config.twitch_client_id,
        urlencoding::encode(&redirect_uri),
        urlencoding::encode(BROADCASTER_SCOPES),
        state_token
    );

    Ok(Json(json!({ "authorize_url": authorize_url })))
}

// ---------------------------------------------------------------------
// POST /admin/{guild_id}/role/{role_id}/disconnect
// Detach the broadcaster from this role link and clean up its EventSub
// subscriptions if no other role link still references it.
// ---------------------------------------------------------------------

pub async fn broadcaster_disconnect(
    State(state): State<Arc<AppState>>,
    Path((guild_id, role_id)): Path<(String, String)>,
    jar: CookieJar,
    headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    if extract_bearer(&headers).is_none() {
        csrf::verify_origin(&headers, &state.config.allowed_origins)?;
    }
    let access = require_role_config_access(&state, &jar, &headers, &guild_id, &role_id).await?;
    // Read-only sessions (a developer impersonating the user) may view but not
    // write — the server-side half of the read-only contract.
    if access.read_only {
        return Err(AppError::Forbidden(
            "This configuration is read-only while impersonating a user.".into(),
        ));
    }

    let broadcaster_id: Option<String> = sqlx::query_scalar(
        "SELECT broadcaster_id FROM role_links WHERE guild_id = $1 AND role_id = $2",
    )
    .bind(&guild_id)
    .bind(&role_id)
    .fetch_optional(&state.pool)
    .await?
    .flatten();

    sqlx::query(
        "UPDATE role_links SET broadcaster_id = NULL, broadcaster_login = NULL, \
         broadcaster_access_token = NULL, broadcaster_refresh_token = NULL, \
         updated_at = now() WHERE guild_id = $1 AND role_id = $2",
    )
    .bind(&guild_id)
    .bind(&role_id)
    .execute(&state.pool)
    .await?;

    if let Some(bid) = broadcaster_id {
        broadcaster::cleanup_broadcaster_if_orphaned(&state, &bid).await;
    }

    // Re-sync so the role is cleared for everyone (channel-scoped rules now
    // grant to nobody without a channel).
    let _ = state
        .config_sync_tx
        .send(ConfigSyncEvent {
            guild_id: guild_id.clone(),
            role_id: role_id.clone(),
        })
        .await;

    tracing::info!(guild_id, role_id, "Broadcaster disconnected from role link");
    Ok(Json(json!({ "removed": true })))
}

// ---------------------------------------------------------------------
// GET /admin/{guild_id}/role/{role_id}  — the iframe rule-builder page
// ---------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RoleConfigPageQuery {
    #[serde(default)]
    rl_token: Option<String>,
}

pub async fn role_config_page(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    headers: HeaderMap,
    Path((guild_id, role_id)): Path<(String, String)>,
    Query(query): Query<RoleConfigPageQuery>,
) -> Response {
    let has_rl_token = query
        .rl_token
        .as_deref()
        .map(|t| !t.is_empty())
        .unwrap_or(false);

    // Path 1: iframe entry — verify rl_token, mint an iframe-session token.
    // `read_only` is true when a developer is impersonating the user.
    let (iframe_session, read_only) = match query.rl_token.as_deref() {
        Some(token) if !token.is_empty() => {
            match verify_iframe_entry(&state, &guild_id, &role_id, token).await {
                Ok((t, ro)) => (Some(t), ro),
                Err(resp) => return resp,
            }
        }
        _ => (None, false),
    };

    // Path 2: direct nav — cookie + manager check. A cross-site iframe won't
    // carry our first-party `rl_session` cookie, so landing here while embedded
    // almost always means RoleLogic never appended `?rl_token=` (a BASE_URL /
    // registered-plugin-URL mismatch). Surface that precisely.
    if iframe_session.is_none() {
        if let Err(e) = require_manager(&state, &jar, &guild_id).await {
            if !has_rl_token && looks_embedded(&headers) {
                tracing::warn!(
                    guild_id,
                    role_id,
                    base_url = %state.config.base_url,
                    "role_config_page reached inside an iframe with no rl_token — \
                     RoleLogic did not pass an auth token. Verify BASE_URL exactly \
                     matches the plugin URL registered in RoleLogic (https, \
                     including the /twitch-follower-role path prefix)."
                );
                return render_iframe_no_token(&state);
            }
            return render_signin_page(&state, &e.to_string());
        }
    }

    let body = ROLE_CONFIG_TEMPLATE
        .replace("__BASE_URL__", &state.config.base_url)
        .replace("__GUILD_ID__", &guild_id)
        .replace("__ROLE_ID__", &role_id)
        .replace("__IFRAME_TOKEN__", iframe_session.as_deref().unwrap_or(""))
        .replace("__READ_ONLY__", if read_only { "1" } else { "0" });

    let csp = admin_iframe_csp(state.config.rl_dashboard_origin.as_deref());
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8".to_string()),
            (header::CONTENT_SECURITY_POLICY, csp),
            (
                header::CACHE_CONTROL,
                "private, max-age=300, must-revalidate".to_string(),
            ),
        ],
        body,
    )
        .into_response()
}

/// Verify `?rl_token=…` and return a freshly minted iframe-session token. On
/// failure returns a rendered error page so the iframe shows something useful.
async fn verify_iframe_entry(
    state: &AppState,
    guild_id: &str,
    role_id: &str,
    rl_token_str: &str,
) -> Result<(String, bool), Response> {
    let api_token: Option<String> =
        sqlx::query_scalar("SELECT api_token FROM role_links WHERE guild_id = $1 AND role_id = $2")
            .bind(guild_id)
            .bind(role_id)
            .fetch_optional(&state.pool)
            .await
            .map_err(|e| render_inline_error(state, &format!("Database error: {e}")))?;

    let Some(api_token) = api_token else {
        return Err(render_inline_error(
            state,
            "This role link isn't registered with this plugin yet.",
        ));
    };

    let verified =
        rl_token::verify(rl_token_str, &api_token, &state.config.base_url).map_err(|e| {
            let msg = match e {
                rl_token::RlTokenError::Expired => {
                    "Your session expired. Reopen the plugin in the RoleLogic dashboard."
                }
                rl_token::RlTokenError::BadSignature | rl_token::RlTokenError::Malformed => {
                    "Invalid auth token."
                }
                rl_token::RlTokenError::WrongAudience => "Token is for a different plugin.",
                rl_token::RlTokenError::WrongIssuer => "Token was not issued by RoleLogic.",
            };
            render_inline_error(state, msg)
        })?;

    if verified.guild_id != guild_id || verified.role_id != role_id {
        return Err(render_inline_error(
            state,
            "Token does not match this role link.",
        ));
    }

    if verified.read_only {
        tracing::info!(
            guild_id,
            role_id,
            target = %verified.discord_id,
            actor = verified.actor_id.as_deref().unwrap_or("?"),
            "Role config opened read-only (developer impersonation)"
        );
    }

    // Carry the read-only flag into the minted iframe-session so every XHR is
    // gated; return it too so the page renders in read-only mode.
    let token = rl_token::mint_iframe_session(
        &verified.discord_id,
        guild_id,
        role_id,
        verified.read_only,
        &state.config.session_secret,
    );
    Ok((token, verified.read_only))
}

fn render_inline_error(state: &AppState, message: &str) -> Response {
    let base_url = &state.config.base_url;
    let msg = message
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let body = format!(
        r##"<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cannot load configuration</title>
<link rel="icon" href="{base_url}/favicon.ico">
<style>body{{font-family:system-ui,sans-serif;background:#0e0e10;color:#e8eaed;padding:32px 24px;line-height:1.5}}
h1{{color:#fca5a5;font-size:18px;margin-bottom:10px}}p{{color:#9aa3b2}}</style>
</head><body><h1>Cannot load configuration</h1><p>{msg}</p>
<p style="margin-top:14px;color:#7a8497">If you opened this from the RoleLogic dashboard, close and reopen the role's plugin tab.</p>
</body></html>"##
    );
    let csp = admin_iframe_csp(state.config.rl_dashboard_origin.as_deref());
    (
        StatusCode::FORBIDDEN,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8".to_string()),
            (header::CONTENT_SECURITY_POLICY, csp),
        ],
        body,
    )
        .into_response()
}

/// Heuristic: is this the document load of a cross-site iframe? Used only to
/// pick the right *message* (never for authz).
fn looks_embedded(headers: &HeaderMap) -> bool {
    let h = |k: &str| {
        headers
            .get(k)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase()
    };
    let dest = h("sec-fetch-dest");
    dest == "iframe" || dest == "frame" || h("sec-fetch-site") == "cross-site"
}

fn render_iframe_no_token(state: &AppState) -> Response {
    let base_url = &state.config.base_url;
    let body = format!(
        r##"<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Configuration unavailable</title>
<link rel="icon" href="{base_url}/favicon.ico">
<style>body{{font-family:system-ui,sans-serif;background:#0e0e10;color:#e8eaed;padding:32px 24px;line-height:1.55;max-width:560px}}
h1{{color:#fbbf24;font-size:18px;margin:0 0 10px}}p{{color:#9aa3b2;margin:8px 0}}
code{{background:#0b0d12;padding:2px 6px;border-radius:4px;font-size:12px}}</style>
</head><body>
<h1>RoleLogic didn't pass an authentication token</h1>
<p>This plugin page must be opened from inside the RoleLogic dashboard, which
attaches a one-time token. None arrived with this request.</p>
<p><strong>If you're the server admin:</strong> close this tab and reopen the
role's plugin tab from RoleLogic. If it keeps happening, the plugin is
mis-registered — its <code>BASE_URL</code> must exactly match the URL
configured for this plugin in RoleLogic: HTTPS, no trailing slash, and
including the <code>/twitch-follower-role</code> path prefix.</p>
<p style="color:#7a8497;font-size:12px;margin-top:16px">Configured BASE_URL:
<code>{base_url}</code></p>
</body></html>"##
    );
    let csp = admin_iframe_csp(state.config.rl_dashboard_origin.as_deref());
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8".to_string()),
            (header::CONTENT_SECURITY_POLICY, csp),
        ],
        body,
    )
        .into_response()
}

/// Direct-nav (non-iframe) sign-in prompt — rendered, never auto-redirected.
fn render_signin_page(state: &AppState, reason: &str) -> Response {
    let base_url = &state.config.base_url;
    let reason = reason
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let body = format!(
        r##"<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sign in — Twitch Follower Role</title>
<link rel="icon" href="{base_url}/favicon.ico">
<style>body{{font-family:system-ui,sans-serif;background:#0e0e10;color:#e8eaed;padding:48px 24px;max-width:520px;margin:0 auto;line-height:1.55}}
h1{{font-size:22px;margin:0 0 12px}}p{{color:#9aa3b2}}
a.btn{{display:inline-block;margin-top:18px;background:#5865f2;color:#fff;padding:12px 22px;border-radius:8px;text-decoration:none;font-weight:600}}
.actions{{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:18px}}
.actions a.btn{{margin-top:0}}
form.logout-form{{margin:0}}
button.logout{{background:none;color:#8a93a4;border:1px solid #2a2f3a;
  padding:10px 16px;border-radius:8px;font-size:13px;font-weight:600;
  cursor:pointer;font-family:inherit}}
button.logout:hover{{color:#fca5a5;border-color:#5c2630}}</style>
</head><body>
<h1>Sign in to continue</h1>
<p>You need <strong>Manage Server</strong> on this guild to edit its
Twitch-Follower-Role configuration.</p>
<p style="color:#7a8497;font-size:12px">{reason}</p>
<div class="actions">
  <a class="btn" id="login">Sign in with Discord</a>
  <form class="logout-form" method="POST" action="/auth/logout">
    <button type="submit" class="logout">Sign out &amp; try another account</button>
  </form>
</div>
<script>
const ORIGIN=new URL("{base_url}").origin;
const RET=encodeURIComponent(location.pathname);
document.getElementById('login').href=ORIGIN+'/auth/login?return_to='+RET;
document.querySelectorAll('form.logout-form').forEach(f=>{{
  f.action=ORIGIN+'/auth/logout?return_to='+RET;
}});
</script>
</body></html>"##
    );
    let csp = admin_iframe_csp(state.config.rl_dashboard_origin.as_deref());
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8".to_string()),
            (header::CONTENT_SECURITY_POLICY, csp),
        ],
        body,
    )
        .into_response()
}

/// Dual gate: `Authorization: Bearer ifs:…` (iframe) OR cookie+manager (direct
/// nav). Returns the caller's discord_id.
/// Outcome of an access check for the role-config endpoints: who is calling and
/// whether the session is read-only (a developer impersonating the user).
struct RoleConfigAccess {
    #[allow(dead_code)]
    discord_id: String,
    read_only: bool,
}

async fn require_role_config_access(
    state: &Arc<AppState>,
    jar: &CookieJar,
    headers: &HeaderMap,
    guild_id: &str,
    role_id: &str,
) -> Result<RoleConfigAccess, AppError> {
    if let Some(bearer) = extract_bearer(headers) {
        let s = rl_token::verify_iframe_session(&bearer, &state.config.session_secret).ok_or_else(
            || {
                AppError::UnauthorizedWith(
                    "Your session expired. Reopen the plugin in the RoleLogic dashboard.".into(),
                )
            },
        )?;
        if s.guild_id != guild_id || s.role_id != role_id {
            return Err(AppError::Forbidden(
                "Token does not grant access to this role link.".into(),
            ));
        }
        return Ok(RoleConfigAccess {
            discord_id: s.discord_id,
            read_only: s.read_only,
        });
    }
    // Reuse the guild-scoped gate's cookie path (manager check).
    let discord_id = require_guild_admin(state, jar, headers, guild_id).await?;
    Ok(RoleConfigAccess {
        discord_id,
        read_only: false,
    })
}

// ---------------------------------------------------------------------
// GET /admin/{guild_id}/role/{role_id}/data
// ---------------------------------------------------------------------

pub async fn role_config_data(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    headers: HeaderMap,
    Path((guild_id, role_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    require_role_config_access(&state, &jar, &headers, &guild_id, &role_id).await?;

    let link = sqlx::query_as::<_, (Option<String>, Option<String>, Value, i32)>(
        "SELECT broadcaster_id, broadcaster_login, rule_tree, rule_tree_version \
         FROM role_links WHERE guild_id = $1 AND role_id = $2",
    )
    .bind(&guild_id)
    .bind(&role_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| {
        AppError::NotFound("This role link doesn't exist. Has it been added in RoleLogic?".into())
    })?;
    let (broadcaster_id, broadcaster_login, rule_tree, rule_tree_version) = link;
    let tree: RuleTree = serde_json::from_value(rule_tree).unwrap_or_default();

    // Surface the public users-list settings here so admins discover the
    // feature: without this they'd never see the public page exists.
    let view_permission: String =
        sqlx::query_scalar("SELECT view_permission FROM guild_settings WHERE guild_id = $1")
            .bind(&guild_id)
            .fetch_optional(&state.pool)
            .await?
            .unwrap_or_else(|| "managers".to_string());

    // Per-guild verify URL: `?guild=<id>` lets the verify page show server
    // context and auto-clear any opt-out for this server in one click. Guild
    // IDs are Discord snowflakes (digits only) so they need no encoding.
    Ok(Json(json!({
        "guild_id": guild_id,
        "role_id": role_id,
        "config": {
            "grant_on_any_relation": tree.grant_on_any_relation,
            "groups": tree.groups,
        },
        "rule_tree_version": rule_tree_version,
        "channel": {
            "connected": broadcaster_id.is_some(),
            "broadcaster_id": broadcaster_id,
            "broadcaster_login": broadcaster_login,
        },
        "targets": target_catalog(),
        "operators": operator_catalog(),
        "limits": {
            "max_groups": MAX_GROUPS,
            "max_conditions_per_group": MAX_CONDITIONS_PER_GROUP,
        },
        "verify_url": format!("{}/verify?guild={}", state.config.base_url, guild_id),
        "users": {
            "url": format!("{}/users/{}", state.config.base_url, guild_id),
            "view_permission": view_permission,
        },
    })))
}

// ---------------------------------------------------------------------
// POST /admin/{guild_id}/role/{role_id}/save  (optimistic-locked)
// ---------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RoleConfigSaveBody {
    pub rule_tree_version: i32,
    #[serde(flatten)]
    pub tree: RuleTreeBody,
}

pub async fn role_config_save(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    headers: HeaderMap,
    Path((guild_id, role_id)): Path<(String, String)>,
    Json(body): Json<RoleConfigSaveBody>,
) -> Result<Json<Value>, AppError> {
    if extract_bearer(&headers).is_none() {
        csrf::verify_origin(&headers, &state.config.allowed_origins)?;
    }
    let access = require_role_config_access(&state, &jar, &headers, &guild_id, &role_id).await?;
    // Read-only sessions (a developer impersonating the user) may view but not
    // write — the server-side half of the read-only contract.
    if access.read_only {
        return Err(AppError::Forbidden(
            "This configuration is read-only while impersonating a user.".into(),
        ));
    }

    let expected_version = body.rule_tree_version;
    let parsed = rule_validator::parse_rule_tree(body.tree)?;

    // A relation rule (anything other than "anyone who linked their Twitch")
    // is evaluated against the connected channel. With no broadcaster bound it
    // would silently grant the role to nobody — reject so the dashboard
    // surfaces the reason instead of persisting a no-op rule. The same guard
    // lives in `preview_count_for`'s "nobody" short-circuit.
    if !parsed.rule_tree.grant_on_any_relation && !parsed.rule_tree.groups.is_empty() {
        let connected: Option<String> = sqlx::query_scalar(
            "SELECT broadcaster_id FROM role_links WHERE guild_id = $1 AND role_id = $2",
        )
        .bind(&guild_id)
        .bind(&role_id)
        .fetch_optional(&state.pool)
        .await?
        .flatten();
        if connected.is_none() {
            return Err(AppError::BadRequest(
                "Connect the Twitch channel this rule checks against before saving — \
                 without a connected channel it would grant the role to nobody."
                    .into(),
            ));
        }
    }

    let tree_json = serde_json::to_value(&parsed.rule_tree)
        .map_err(|e| AppError::Internal(format!("serialize rule_tree: {e}")))?;

    // Optimistic lock: only update if the version still matches what the editor
    // loaded, so a second tab can't silently clobber.
    let result = sqlx::query(
        "UPDATE role_links \
         SET rule_tree = $1, rule_tree_version = rule_tree_version + 1, updated_at = now() \
         WHERE guild_id = $2 AND role_id = $3 AND rule_tree_version = $4",
    )
    .bind(&tree_json)
    .bind(&guild_id)
    .bind(&role_id)
    .bind(expected_version)
    .execute(&state.pool)
    .await?;

    if result.rows_affected() == 0 {
        let exists: Option<i32> = sqlx::query_scalar(
            "SELECT rule_tree_version FROM role_links WHERE guild_id=$1 AND role_id=$2",
        )
        .bind(&guild_id)
        .bind(&role_id)
        .fetch_optional(&state.pool)
        .await?;
        return match exists {
            None => Err(AppError::NotFound(
                "This role link doesn't exist. Has it been added in RoleLogic?".into(),
            )),
            Some(_) => Err(AppError::StaleVersion),
        };
    }

    let new_version: i32 = sqlx::query_scalar(
        "SELECT rule_tree_version FROM role_links WHERE guild_id=$1 AND role_id=$2",
    )
    .bind(&guild_id)
    .bind(&role_id)
    .fetch_one(&state.pool)
    .await?;

    let _ = state
        .config_sync_tx
        .send(ConfigSyncEvent {
            guild_id: guild_id.clone(),
            role_id: role_id.clone(),
        })
        .await;

    tracing::info!(
        guild_id,
        role_id,
        groups = parsed.rule_tree.groups.len(),
        grant_on_any = parsed.rule_tree.grant_on_any_relation,
        "Role rule_tree updated"
    );

    Ok(Json(
        json!({ "success": true, "rule_tree_version": new_version }),
    ))
}

// ---------------------------------------------------------------------
// GET/POST /admin/{guild_id}/role/{role_id}/preview
// Dry-run: how many guild members currently match? No RoleLogic call.
// ---------------------------------------------------------------------

pub async fn role_config_preview(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    headers: HeaderMap,
    Path((guild_id, role_id)): Path<(String, String)>,
) -> Result<Json<Value>, AppError> {
    require_role_config_access(&state, &jar, &headers, &guild_id, &role_id).await?;

    let link = sqlx::query_as::<_, (Option<String>, Value)>(
        "SELECT broadcaster_id, rule_tree FROM role_links WHERE guild_id=$1 AND role_id=$2",
    )
    .bind(&guild_id)
    .bind(&role_id)
    .fetch_optional(&state.pool)
    .await?
    .ok_or_else(|| AppError::NotFound("Role link not found.".into()))?;
    let (broadcaster_id, raw_tree) = link;
    let tree: RuleTree = serde_json::from_value(raw_tree).unwrap_or_default();

    preview_count_for(&state, &guild_id, broadcaster_id.as_deref(), &tree).await
}

/// POST variant: previews a proposed (unsaved) rule the admin is building.
pub async fn role_config_preview_edit(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    headers: HeaderMap,
    Path((guild_id, role_id)): Path<(String, String)>,
    Json(body): Json<RuleTreeBody>,
) -> Result<Json<Value>, AppError> {
    if extract_bearer(&headers).is_none() {
        csrf::verify_origin(&headers, &state.config.allowed_origins)?;
    }
    require_role_config_access(&state, &jar, &headers, &guild_id, &role_id).await?;

    let parsed = rule_validator::parse_rule_tree(body)?;
    let broadcaster_id: Option<String> = sqlx::query_scalar(
        "SELECT broadcaster_id FROM role_links WHERE guild_id=$1 AND role_id=$2",
    )
    .bind(&guild_id)
    .bind(&role_id)
    .fetch_optional(&state.pool)
    .await?
    .flatten();

    preview_count_for(
        &state,
        &guild_id,
        broadcaster_id.as_deref(),
        &parsed.rule_tree,
    )
    .await
}

/// Shared core for GET (saved tree) and POST (proposed tree) previews.
async fn preview_count_for(
    state: &Arc<AppState>,
    guild_id: &str,
    broadcaster_id: Option<&str>,
    tree: &RuleTree,
) -> Result<Json<Value>, AppError> {
    // A rule grants to nobody when it is NOT channel-agnostic AND it has no
    // channel bound or no groups. `grant_on_any_relation` is channel-agnostic,
    // so the "Anyone who linked their Twitch" preset (no channel) must NOT
    // short-circuit here — it matches every linked member.
    let nobody =
        !tree.grant_on_any_relation && (broadcaster_id.is_none() || tree.groups.is_empty());
    if nobody {
        return Ok(Json(
            json!({ "matching": 0, "linked": 0, "available": true }),
        ));
    }

    let member_ids = match auth_gateway::fetch_guild_member_ids(
        &state.http,
        &state.config.auth_gateway_url,
        &state.config.internal_api_key,
        guild_id,
    )
    .await
    {
        Ok(v) => v,
        Err(_) => {
            return Ok(Json(json!({
                "available": false,
                "reason": "Member list temporarily unavailable; preview will work once the Auth Gateway responds."
            })))
        }
    };
    if member_ids.is_empty() {
        return Ok(Json(
            json!({ "matching": 0, "linked": 0, "available": true }),
        ));
    }

    let linked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM linked_accounts WHERE discord_id = ANY($1::text[])",
    )
    .bind(&member_ids)
    .fetch_one(&state.pool)
    .await?;

    // Channel-agnostic "anyone who linked Twitch": every linked member matches.
    if tree.grant_on_any_relation {
        return Ok(Json(json!({
            "available": true,
            "matching": linked,
            "linked": linked,
        })));
    }

    // Channel-scoped rule. The `nobody` guard guarantees a channel is bound and
    // at least one group exists for this path.
    let broadcaster_id = broadcaster_id.expect("channel bound for non-grant preview");

    let (rule_where, binds) = rule_sql::build_rule_where(tree, 2);
    let query = format!(
        "SELECT count(DISTINCT la.discord_id) \
         FROM linked_accounts la \
         LEFT JOIN user_channel_cache ucc \
           ON ucc.twitch_user_id = la.twitch_user_id AND ucc.broadcaster_id = $1 \
         WHERE la.discord_id = ANY($2::text[]) AND ({rule_where})"
    );
    let mut q = sqlx::query_scalar::<_, i64>(&query)
        .bind(broadcaster_id)
        .bind(&member_ids);
    for b in &binds {
        q = match b {
            Bind::Bool(v) => q.bind(*v),
            Bind::Int(v) => q.bind(*v),
        };
    }
    let matching: i64 = q.fetch_one(&state.pool).await?;

    Ok(Json(json!({
        "available": true,
        "matching": matching,
        "linked": linked,
    })))
}

// ---------------------------------------------------------------------
// Catalogs consumed by the rule-builder front-end
// ---------------------------------------------------------------------

fn kind_str(k: TargetKind) -> &'static str {
    match k {
        TargetKind::Bool => "bool",
        TargetKind::Int => "int",
    }
}

fn target_catalog() -> Vec<Value> {
    use ConditionTarget::*;
    let entries: &[(ConditionTarget, &str)] = &[
        (IsFollower, "Is a follower"),
        (FollowAgeDays, "Days since first followed"),
        (IsSubscriber, "Is an active subscriber"),
        (SubTier, "Subscription tier (1-3)"),
    ];
    entries
        .iter()
        .map(|(t, label)| {
            json!({
                "key": t.as_str(),
                "label": label,
                "kind": kind_str(t.kind()),
                "group": "viewer",
            })
        })
        .collect()
}

fn operator_catalog() -> Vec<Value> {
    use ConditionOperator::*;
    let all = [
        (Eq, "equals"),
        (Neq, "not equals"),
        (Gt, "greater than"),
        (Gte, "at least"),
        (Lt, "less than"),
        (Lte, "at most"),
        (Between, "between"),
    ];
    all.iter()
        .map(|(op, label)| {
            json!({
                "key": op.as_str(),
                "label": label,
                "valid_for": {
                    "bool": op.valid_for(TargetKind::Bool),
                    "int": op.valid_for(TargetKind::Int),
                },
                "needs_value_end": matches!(op, Between),
            })
        })
        .collect()
}
