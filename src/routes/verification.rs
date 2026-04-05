use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::Json;
use axum_extra::extract::cookie::{Cookie, CookieJar};
use rand::Rng;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::AppError;
use crate::services::session;
use crate::services::sync::{self, UserSyncEvent};
use crate::AppState;

const SESSION_COOKIE: &str = "rl_session";

fn get_session(jar: &CookieJar, secret: &str) -> Result<(String, String), AppError> {
    let cookie = jar.get(SESSION_COOKIE).ok_or(AppError::Unauthorized)?;
    session::verify_session(cookie.value(), secret)
        .ok_or(AppError::Unauthorized)
}

pub fn render_verify_page(base_url: &str) -> String {
    format!(
        r##"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Twitch Follower Role - Link Account</title>
    <link rel="icon" href="{base_url}/favicon.ico" type="image/x-icon">
    <meta name="description" content="Link your Discord and Twitch accounts to automatically receive server roles based on your channel follow/subscription status.">
    <meta name="theme-color" content="#9146ff">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 580px; margin: 0 auto; padding: 32px 20px; background: #0e0e10; color: #c8ccd4; min-height: 100vh; }}
        h1 {{ color: #9146ff; font-size: 24px; margin-bottom: 4px; }}
        p {{ line-height: 1.6; margin: 6px 0; font-size: 14px; }}
        a {{ color: #bf94ff; }}
        .subtitle {{ color: #7a8299; font-size: 14px; margin-bottom: 20px; }}
        .card {{ background: #18181b; padding: 22px; border-radius: 10px; margin: 14px 0; border: 1px solid #2a2a2e; }}
        .btn {{ display: inline-flex; align-items: center; gap: 8px; padding: 10px 22px; color: #fff; text-decoration: none; border-radius: 6px; font-size: 14px; font-weight: 500; border: none; cursor: pointer; font-family: inherit; transition: background .15s; }}
        .btn-discord {{ background: #5865f2; }}
        .btn-discord:hover {{ background: #4752c4; }}
        .btn-twitch {{ background: #9146ff; }}
        .btn-twitch:hover {{ background: #772ce8; }}
        .btn-danger {{ background: transparent; color: #f87171; border: 1px solid #7f1d1d; font-size: 13px; padding: 8px 16px; }}
        .btn-danger:hover {{ background: #7f1d1d33; }}
        .btn-secondary {{ background: transparent; color: #94a3b8; border: 1px solid #334155; font-size: 13px; padding: 8px 16px; }}
        .btn-secondary:hover {{ background: #1e293b; }}
        .badge {{ display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 12px; font-weight: 500; }}
        .badge-ok {{ background: #052e16; color: #4ade80; border: 1px solid #14532d; }}
        .msg {{ padding: 10px 14px; border-radius: 6px; margin: 12px 0; font-size: 13px; line-height: 1.5; }}
        .msg-error {{ background: #1c0a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .msg-success {{ background: #052e16; color: #86efac; border: 1px solid #14532d; }}
        .hidden {{ display: none; }}
        .actions {{ display: flex; gap: 10px; margin-top: 14px; flex-wrap: wrap; }}
    </style>
</head>
<body>
    <h1>Twitch Follower Role</h1>
    <p class="subtitle">Link your Discord and Twitch accounts</p>

    <div id="loading" class="card"><p>Loading...</p></div>
    <div id="login" class="card hidden">
        <p>Sign in with Discord to get started.</p>
        <div class="actions">
            <a href="{base_url}/verify/login" class="btn btn-discord">Login with Discord</a>
        </div>
    </div>
    <div id="link-twitch" class="card hidden">
        <p>Logged in as <strong id="discord-name"></strong> <span class="badge badge-ok">Discord</span></p>
        <p style="margin-top:12px;">Now link your Twitch account:</p>
        <div class="actions">
            <a href="{base_url}/verify/twitch" class="btn btn-twitch">Link Twitch Account</a>
            <button onclick="doLogout()" class="btn btn-secondary">Logout</button>
        </div>
    </div>
    <div id="linked" class="card hidden">
        <p>Logged in as <strong id="discord-name2"></strong> <span class="badge badge-ok">Discord</span></p>
        <p>Twitch: <strong id="twitch-name"></strong> <span class="badge badge-ok">Linked</span></p>
        <p style="margin-top:8px; color:#86efac; font-size:13px;">Your accounts are linked. Roles will be assigned automatically based on your Twitch channel status.</p>
        <div class="actions">
            <button onclick="doUnlink()" class="btn btn-danger">Unlink Twitch</button>
            <button onclick="doLogout()" class="btn btn-secondary">Logout</button>
        </div>
    </div>
    <div id="error" class="msg msg-error hidden"></div>

    <script>
    async function init() {{
        try {{
            const r = await fetch('{base_url}/verify/status', {{credentials:'include'}});
            const d = await r.json();
            document.getElementById('loading').classList.add('hidden');
            if (!d.discord_id) {{
                document.getElementById('login').classList.remove('hidden');
            }} else if (!d.twitch_login) {{
                document.getElementById('discord-name').textContent = d.discord_name;
                document.getElementById('link-twitch').classList.remove('hidden');
            }} else {{
                document.getElementById('discord-name2').textContent = d.discord_name;
                document.getElementById('twitch-name').textContent = d.twitch_login;
                document.getElementById('linked').classList.remove('hidden');
            }}
        }} catch(e) {{
            document.getElementById('loading').classList.add('hidden');
            document.getElementById('login').classList.remove('hidden');
        }}
    }}
    async function doUnlink() {{
        if (!confirm('Unlink your Twitch account? You will lose all roles assigned by this plugin.')) return;
        const r = await fetch('{base_url}/verify/unlink', {{method:'POST', credentials:'include'}});
        if (r.ok) location.reload();
        else {{ const d = await r.json(); showError(d.error); }}
    }}
    async function doLogout() {{
        await fetch('{base_url}/verify/logout', {{method:'POST', credentials:'include'}});
        location.reload();
    }}
    function showError(msg) {{
        const el = document.getElementById('error');
        el.textContent = msg;
        el.classList.remove('hidden');
    }}
    init();
    </script>
</body>
</html>"##
    )
}

pub async fn verify_page(State(state): State<Arc<AppState>>) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        state.verify_html.clone(),
    )
        .into_response()
}

pub async fn login(State(state): State<Arc<AppState>>) -> Result<Redirect, AppError> {
    let return_to = "/twitch-follower-role/verify";
    let url = format!(
        "/auth/login?return_to={}",
        urlencoding::encode(return_to),
    );
    Ok(Redirect::temporary(&url))
}

#[derive(Deserialize)]
pub struct CallbackQuery {
    pub code: String,
    pub state: String,
}

pub async fn status(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Json<Value> {
    let session = get_session(&jar, &state.config.session_secret);

    match session {
        Ok((discord_id, display_name)) => {
            // Check if Twitch is linked
            let twitch = sqlx::query_as::<_, (String,)>(
                "SELECT twitch_login FROM linked_accounts WHERE discord_id = $1",
            )
            .bind(&discord_id)
            .fetch_optional(&state.pool)
            .await
            .ok()
            .flatten();

            Json(json!({
                "discord_id": discord_id,
                "discord_name": display_name,
                "twitch_login": twitch.map(|(l,)| l),
            }))
        }
        Err(_) => Json(json!({
            "discord_id": null,
            "discord_name": null,
            "twitch_login": null,
        })),
    }
}

pub async fn twitch_login(State(state): State<Arc<AppState>>, jar: CookieJar) -> Result<Redirect, AppError> {
    let (discord_id, _) = get_session(&jar, &state.config.session_secret)?;

    let state_token: String = {
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
    };

    sqlx::query(
        "INSERT INTO oauth_states (state, redirect_data, expires_at) VALUES ($1, $2, now() + interval '10 minutes')",
    )
    .bind(&state_token)
    .bind(json!({"type": "twitch_user", "discord_id": discord_id}))
    .execute(&state.pool)
    .await?;

    let redirect_uri = state.config.twitch_user_oauth_redirect_uri();
    let url = format!(
        "https://id.twitch.tv/oauth2/authorize?client_id={}&redirect_uri={}&response_type=code&scope=&state={}&force_verify=true",
        state.config.twitch_client_id,
        urlencoding::encode(&redirect_uri),
        state_token
    );

    Ok(Redirect::temporary(&url))
}

pub async fn twitch_callback(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CallbackQuery>,
) -> Result<Redirect, AppError> {
    // Validate state and extract discord_id
    let oauth_state = sqlx::query_as::<_, (serde_json::Value,)>(
        "DELETE FROM oauth_states WHERE state = $1 AND expires_at > now() RETURNING redirect_data",
    )
    .bind(&query.state)
    .fetch_optional(&state.pool)
    .await?
    .ok_or(AppError::BadRequest("Invalid or expired OAuth state".into()))?;

    let discord_id = oauth_state.0["discord_id"]
        .as_str()
        .ok_or(AppError::Internal("Missing discord_id in state".into()))?
        .to_string();

    // Exchange code for token
    let redirect_uri = state.config.twitch_user_oauth_redirect_uri();
    let (access_token, _) = state
        .twitch_client
        .exchange_code(&query.code, &redirect_uri)
        .await?;

    // Get Twitch user info
    let twitch_user = state.twitch_client.get_user_by_token(&access_token).await?;

    // Check if this Twitch account is already linked to another Discord user
    let existing = sqlx::query_scalar::<_, String>(
        "SELECT discord_id FROM linked_accounts WHERE twitch_user_id = $1",
    )
    .bind(&twitch_user.id)
    .fetch_optional(&state.pool)
    .await?;

    if let Some(existing_discord_id) = existing {
        if existing_discord_id != discord_id {
            return Err(AppError::BadRequest(
                "This Twitch account is already linked to another Discord user".into(),
            ));
        }
        // Already linked to this user, update login
        sqlx::query("UPDATE linked_accounts SET twitch_login = $1 WHERE discord_id = $2")
            .bind(&twitch_user.login)
            .bind(&discord_id)
            .execute(&state.pool)
            .await?;
    } else {
        // Check if this Discord user already has a different Twitch linked
        sqlx::query("DELETE FROM linked_accounts WHERE discord_id = $1")
            .bind(&discord_id)
            .execute(&state.pool)
            .await?;

        sqlx::query(
            "INSERT INTO linked_accounts (discord_id, twitch_user_id, twitch_login) VALUES ($1, $2, $3)",
        )
        .bind(&discord_id)
        .bind(&twitch_user.id)
        .bind(&twitch_user.login)
        .execute(&state.pool)
        .await?;
    }

    // Populate cache entries for all active broadcasters in this user's guilds
    sync::populate_cache_for_user(&discord_id, &twitch_user.id, &state.pool).await?;

    // Trigger sync
    let _ = state
        .user_sync_tx
        .send(UserSyncEvent::AccountLinked {
            discord_id: discord_id.clone(),
        })
        .await;

    tracing::info!(
        discord_id,
        twitch_user_id = twitch_user.id,
        twitch_login = twitch_user.login,
        "Account linked"
    );

    Ok(Redirect::temporary("/twitch-follower-role/verify"))
}

pub async fn unlink(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Json<Value>, AppError> {
    let (discord_id, _) = get_session(&jar, &state.config.session_secret)?;

    let deleted = sqlx::query("DELETE FROM linked_accounts WHERE discord_id = $1")
        .bind(&discord_id)
        .execute(&state.pool)
        .await?;

    if deleted.rows_affected() > 0 {
        let _ = state
            .user_sync_tx
            .send(UserSyncEvent::AccountUnlinked {
                discord_id: discord_id.clone(),
            })
            .await;

        tracing::info!(discord_id, "Account unlinked");
    }

    Ok(Json(json!({"success": true})))
}

pub async fn logout(jar: CookieJar) -> (CookieJar, Json<Value>) {
    let cookie = Cookie::build((SESSION_COOKIE, ""))
        .path("/")
        .max_age(time::Duration::ZERO)
        .build();

    (jar.remove(cookie), Json(json!({"success": true})))
}
