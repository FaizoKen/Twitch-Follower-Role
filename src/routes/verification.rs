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

/// How far ahead to schedule the next background refresh after the inline check
/// done at link time, when the user already qualifies (follows/subs). Matches
/// the worker's 30-min floor so we don't immediately re-spend API budget.
const INITIAL_RECHECK_SECS: i64 = 1800;

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
        .guild-ctx {{ display: none; align-items: center; gap: 10px; background: #052e16; border: 1px solid #14532d; color: #86efac; padding: 8px 14px; border-radius: 8px; margin: 12px 0 6px; font-size: 13px; line-height: 1.5; }}
        .guild-ctx.show {{ display: flex; }}
        .guild-ctx.warn {{ background: #1c1208; border-color: #422006; color: #fbbf24; }}
        .guild-ctx .gctx-icon {{ flex-shrink: 0; }}
        .guild-ctx .gctx-name {{ color: #fff; font-weight: 600; }}
        .manage-link {{ font-size: 13px; color: #94a3b8; margin-top: 14px; }}
        .manage-link a {{ color: #bf94ff; }}
        .refresh-note {{ font-size: 13px; color: #94a3b8; margin-top: 10px; min-height: 18px; transition: color .15s; }}
        .step-label {{ color: #fff; font-size: 16px; font-weight: 600; margin-bottom: 6px; }}
        .channel-list {{ display: flex; flex-direction: column; gap: 8px; margin-top: 8px; }}
        .channel-row {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; background: #0e0e10; border: 1px solid #2a2a2e; border-radius: 8px; padding: 10px 12px; }}
        .channel-meta {{ display: flex; flex-direction: column; gap: 2px; min-width: 0; }}
        .channel-name {{ color: #fff; font-weight: 600; font-size: 14px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
        a.channel-open {{ flex-shrink: 0; background: #9146ff; color: #fff; text-decoration: none; font-size: 13px; font-weight: 600; padding: 8px 14px; border-radius: 6px; transition: background .15s; }}
        a.channel-open:hover {{ background: #772ce8; }}
    </style>
</head>
<body>
    <h1>Twitch Follower Role</h1>
    <p class="subtitle">Everything's on this page: follow on Twitch, link your Discord + Twitch accounts, and your server roles are assigned automatically.</p>

    <!-- Server context banner: only shown when ?guild=<id> is present in the URL.
         Lets a server admin share a per-guild link that both verifies the user
         AND auto-enables the role for that specific server in one shot. -->
    <div id="guild-ctx" class="guild-ctx">
        <svg class="gctx-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
        <span id="guild-ctx-text"></span>
    </div>

    <!-- Step 1: follow / subscribe. Always visible (instructional — the real
         follow/sub is detected once linked), lists the channel(s) this server's
         roles check against, each linking straight to Twitch so the admin
         doesn't have to paste the link separately. -->
    <div id="follow-section" class="card">
        <div class="step-label">Step 1: Follow on Twitch</div>
        <p>Open the channel below and follow it — or subscribe, if the role you want needs a sub. We pick it up automatically once your accounts are linked.</p>
        <div class="channel-list" id="channel-list">
            <p style="color:#7a8299; font-size:13px;">Loading channel…</p>
        </div>
    </div>

    <div id="loading" class="card"><p>Loading...</p></div>
    <div id="login" class="card hidden">
        <p><strong>Step 2:</strong> Sign in with Discord to get started.</p>
        <div class="actions">
            <a href="{base_url}/verify/login" class="btn btn-discord">Login with Discord</a>
        </div>
    </div>
    <div id="link-twitch" class="card hidden">
        <p>Logged in as <strong id="discord-name"></strong> <span class="badge badge-ok">Discord</span></p>
        <p style="margin-top:12px;"><strong>Step 3:</strong> Now link your Twitch account:</p>
        <div class="actions">
            <a href="{base_url}/verify/twitch" class="btn btn-twitch">Link Twitch Account</a>
            <button onclick="doLogout()" class="btn btn-secondary">Logout</button>
        </div>
    </div>
    <div id="linked" class="card hidden">
        <p>Logged in as <strong id="discord-name2"></strong> <span class="badge badge-ok">Discord</span></p>
        <p>Twitch: <strong id="twitch-name"></strong> <span class="badge badge-ok">Linked</span></p>
        <p style="margin-top:8px; color:#86efac; font-size:13px;">Your accounts are linked. Roles will be assigned automatically based on your Twitch channel status.</p>
        <p id="refresh-note" class="refresh-note"></p>
        <p class="manage-link">
            Receiving Twitch roles in servers you didn't intend?
            <a href="/auth/my_servers?from=/twitch-follower-role/verify">Choose which servers receive roles →</a>
        </p>
        <div class="actions">
            <button onclick="doRefresh(false)" class="btn btn-secondary">Re-check now</button>
            <button onclick="doUnlink()" class="btn btn-danger">Unlink Twitch</button>
            <button onclick="doLogout()" class="btn btn-secondary">Logout</button>
        </div>
    </div>
    <div id="error" class="msg msg-error hidden"></div>

    <script>
    const PLUGIN_SLUG = 'twitch-follower-role';
    let isLinked = false;

    // Optional ?guild=<id> tells us the user came from a per-guild verify
    // link an admin shared in their Discord. We use it to (a) show a
    // contextual banner so the user knows which server this is for and
    // (b) automatically clear any existing opt-out (both per-plugin and
    // the guild-wide master) once they're authenticated — so a returning
    // user who'd previously disabled this server doesn't have to find
    // /auth/my_servers to re-enable it.
    const guildId = (() => {{
        try {{
            const v = new URLSearchParams(window.location.search).get('guild');
            return v && /^[0-9]{{5,25}}$/.test(v) ? v : '';
        }} catch (e) {{ return ''; }}
    }})();

    // Preserve the guild context across both OAuth round-trips so an
    // unauth visitor who logs in lands back on this same per-guild URL.
    // The Discord login link bypasses our server-side `login()` shim and
    // hits the gateway directly with a per-guild `return_to`; the Twitch
    // link gets `?guild=<id>` so `twitch_login()` can stash it on the
    // oauth_state and the callback redirects back here with it intact.
    (function patchAuthLinks() {{
        if (!guildId) return;
        const discordLink = document.querySelector('#login a.btn-discord');
        if (discordLink) {{
            const returnTo = '/twitch-follower-role/verify?guild=' + encodeURIComponent(guildId);
            discordLink.href = '/auth/login?return_to=' + encodeURIComponent(returnTo);
        }}
        const twitchLink = document.querySelector('#link-twitch a.btn-twitch');
        if (twitchLink) {{
            twitchLink.href = '{base_url}/verify/twitch?guild=' + encodeURIComponent(guildId);
        }}
    }})();

    // Gateway-absolute API helper for /auth/* (cookie-authed via the
    // shared rl_session). Doesn't prefix with the plugin's base_url.
    async function gatewayApi(method, path, body) {{
        const opts = {{ method, headers: {{}}, credentials: 'include' }};
        if (body) {{
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(body);
        }}
        const res = await fetch(path, opts);
        const data = await res.json().catch(() => ({{}}));
        if (!res.ok) throw new Error(data.error || 'Request failed');
        return data;
    }}

    function showGuildCtx(text, isWarning) {{
        const el = document.getElementById('guild-ctx');
        document.getElementById('guild-ctx-text').innerHTML = text;
        el.classList.toggle('warn', !!isWarning);
        el.classList.add('show');
    }}

    // Resolve guildId → display name via the gateway, then clear any
    // opt-out blocking this plugin from assigning roles in that server.
    // Idempotent: clearing rows that don't exist is a no-op on the server.
    async function applyGuildContext() {{
        if (!guildId) return;
        let prefs;
        try {{
            prefs = await gatewayApi('GET', '/auth/preferences?ensure_guild=' + encodeURIComponent(guildId));
        }} catch (e) {{
            return;
        }}
        const g = (prefs.guilds || []).find(x => x.guild_id === guildId);
        if (!g) {{
            // Either the user isn't in that guild, or the gateway hasn't
            // refreshed their guild list yet. Surface it gently — verify
            // still works; the role just won't apply until they're a member.
            showGuildCtx("You're not in that server yet — join it on Discord, then refresh.", true);
            return;
        }}
        const safeName = (g.guild_name || '(unnamed server)')
            .replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}})[c]);
        const wasDisabled = g.master_optout || (g.plugin_optouts || []).includes(PLUGIN_SLUG);
        // Always clear both — the master toggle wins over per-plugin
        // overrides, so we need to remove it too even if only the
        // per-plugin row was set on this server.
        try {{
            if (g.master_optout) {{
                await gatewayApi('POST', '/auth/preferences', {{
                    guild_id: guildId, plugin: null, enabled: true,
                }});
            }}
            if ((g.plugin_optouts || []).includes(PLUGIN_SLUG)) {{
                await gatewayApi('POST', '/auth/preferences', {{
                    guild_id: guildId, plugin: PLUGIN_SLUG, enabled: true,
                }});
            }}
        }} catch (e) {{
            // Even if the clear failed, still show the banner so the user
            // knows where they are. The role will simply not apply until
            // they fix it manually via /auth/my_servers.
        }}
        const nameHtml = '<span class="gctx-name">' + safeName + '</span>';
        if (wasDisabled) {{
            showGuildCtx(isLinked
                ? 'Enabled Twitch roles for ' + nameHtml + ' — roles apply on the next sync.'
                : 'Enabled Twitch roles for ' + nameHtml + ' — finish linking below to receive roles.');
        }} else {{
            showGuildCtx(isLinked
                ? 'Twitch roles are active in ' + nameHtml + '.'
                : 'Once linked, Twitch roles will apply in ' + nameHtml + '.');
        }}
    }}

    function escHtml(s) {{
        return String(s).replace(/[&<>"']/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}})[c]);
    }}

    // Step 1 ("follow"): list the Twitch channel(s) this server's roles check
    // against so the user can follow straight from here. Public endpoint — runs
    // before sign-in. Best-effort; falls back to generic copy on any failure.
    async function loadChannels() {{
        const list = document.getElementById('channel-list');
        if (!list) return;
        try {{
            const url = '{base_url}/verify/channels' + (guildId ? ('?guild=' + encodeURIComponent(guildId)) : '');
            const res = await fetch(url, {{ cache: 'no-store' }});
            const d = await res.json().catch(() => ({{}}));
            renderChannels((d && d.channels) || []);
        }} catch (e) {{
            renderChannels([]);
        }}
    }}

    function renderChannels(channels) {{
        const list = document.getElementById('channel-list');
        if (!list) return;
        if (!channels.length) {{
            list.innerHTML = '<p style="color:#94a3b8; font-size:13px;">' + (guildId
                ? "This server hasn't connected its Twitch channel yet. You can still sign in and link below — your role applies once it does."
                : "Open the Twitch channel your server uses and follow (or subscribe to) it, then continue below.") + '</p>';
            return;
        }}
        list.innerHTML = channels.map(c => {{
            const login = c.login || '';
            const href = 'https://www.twitch.tv/' + encodeURIComponent(login);
            return '<div class="channel-row">' +
                '<span class="channel-meta"><span class="channel-name">' + escHtml(login) + '</span></span>' +
                '<a class="channel-open" href="' + href + '" target="_blank" rel="noopener">Follow &rarr;</a>' +
            '</div>';
        }}).join('');
    }}

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
                isLinked = true;
                document.getElementById('discord-name2').textContent = d.discord_name;
                document.getElementById('twitch-name').textContent = d.twitch_login;
                document.getElementById('linked').classList.remove('hidden');
                // Visiting the page re-checks your latest follow/sub status so
                // roles self-correct without an unlink/re-link. Best-effort.
                doRefresh(true);
            }}
            // Session is valid — apply the per-guild side effects (if any).
            if (d.discord_id) applyGuildContext();
        }} catch(e) {{
            document.getElementById('loading').classList.add('hidden');
            document.getElementById('login').classList.remove('hidden');
        }}
    }}
    // Nudge the server to re-fetch this user's Twitch data ahead of schedule.
    // `silent` is used for the automatic call on page load — it shows the
    // working/result note but stays quiet on transient errors. The explicit
    // "Re-check now" button passes false so failures surface.
    let refreshing = false;
    async function doRefresh(silent) {{
        const note = document.getElementById('refresh-note');
        if (!note || refreshing) return;
        refreshing = true;
        note.style.color = '#94a3b8';
        note.textContent = 'Checking your latest follow / subscription status…';
        try {{
            const r = await fetch('{base_url}/verify/refresh', {{method:'POST', credentials:'include'}});
            const d = await r.json().catch(() => ({{}}));
            if (!r.ok) throw new Error(d.error || 'Request failed');
            note.style.color = '#86efac';
            note.textContent = d.refreshed
                ? '✓ Re-checking now — your roles update within a minute.'
                : '✓ Your status is already up to date.';
        }} catch (e) {{
            if (silent) {{
                note.textContent = '';
            }} else {{
                note.style.color = '#fca5a5';
                note.textContent = 'Could not refresh right now — try again shortly.';
            }}
        }} finally {{
            refreshing = false;
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
    loadChannels();
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

pub async fn login() -> Result<Redirect, AppError> {
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

            // Keep the cached Discord display name fresh for the public users
            // list (and backfill members who linked before this was added).
            // One cheap write, only when the name actually changed.
            if twitch.is_some() {
                let _ = sqlx::query(
                    "UPDATE linked_accounts SET discord_name = $1 \
                     WHERE discord_id = $2 AND discord_name IS DISTINCT FROM $1",
                )
                .bind(&display_name)
                .bind(&discord_id)
                .execute(&state.pool)
                .await;
            }

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

#[derive(Deserialize)]
pub struct VerifyChannelsQuery {
    pub guild: Option<String>,
}

/// Public (no auth): the Twitch channel(s) this guild's roles check against, so
/// the verify page can render its "follow" step before the user signs in.
/// Returns only the broadcaster logins the admin already advertises ("follow
/// our channel") — nothing sensitive. An invalid or missing `guild` yields an
/// empty list, so the page falls back to generic copy without a wasted query.
pub async fn verify_channels(
    State(state): State<Arc<AppState>>,
    Query(q): Query<VerifyChannelsQuery>,
) -> Result<Json<Value>, AppError> {
    let guild_id = q.guild.unwrap_or_default();
    let valid =
        (5..=25).contains(&guild_id.len()) && guild_id.bytes().all(|b| b.is_ascii_digit());
    if !valid {
        return Ok(Json(json!({ "channels": [] })));
    }

    let logins: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT broadcaster_login FROM role_links \
         WHERE guild_id = $1 AND broadcaster_id IS NOT NULL \
           AND broadcaster_login IS NOT NULL \
         ORDER BY broadcaster_login LIMIT 50",
    )
    .bind(&guild_id)
    .fetch_all(&state.pool)
    .await?;

    let channels: Vec<Value> = logins
        .into_iter()
        .map(|login| json!({ "login": login }))
        .collect();
    Ok(Json(json!({ "channels": channels })))
}

#[derive(Deserialize)]
pub struct TwitchLoginQuery {
    pub guild: Option<String>,
}

pub async fn twitch_login(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<TwitchLoginQuery>,
) -> Result<Redirect, AppError> {
    let (discord_id, _) = get_session(&jar, &state.config.session_secret)?;

    // Carry `?guild=<id>` through the Twitch OAuth round-trip so the
    // verify page lands back on its per-guild URL and can apply the
    // opt-out clear / banner after the link succeeds. Validate it's a
    // Discord snowflake (digits only, 5-25 chars) before persisting.
    let guild_id = query.guild.as_deref().and_then(|g| {
        if (5..=25).contains(&g.len()) && g.chars().all(|c| c.is_ascii_digit()) {
            Some(g.to_string())
        } else {
            None
        }
    });

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
    .bind(json!({"type": "twitch_user", "discord_id": discord_id, "guild_id": guild_id}))
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
    jar: CookieJar,
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
    let guild_id = oauth_state.0["guild_id"].as_str().map(String::from);

    // Opportunistically capture the member's Discord display name from the
    // signed session cookie (for the public users list). Only trust it if the
    // session belongs to the same Discord user this link is bound to; otherwise
    // store nothing and let it backfill on the next verify-page visit.
    let discord_name: Option<String> = get_session(&jar, &state.config.session_secret)
        .ok()
        .filter(|(sid, _)| *sid == discord_id)
        .map(|(_, name)| name);

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
        // Already linked to this user, update login (+ refresh name if known).
        // COALESCE keeps any previously stored name when the cookie is absent.
        sqlx::query(
            "UPDATE linked_accounts SET twitch_login = $1, \
             discord_name = COALESCE($3, discord_name) WHERE discord_id = $2",
        )
        .bind(&twitch_user.login)
        .bind(&discord_id)
        .bind(&discord_name)
        .execute(&state.pool)
        .await?;
    } else {
        // Check if this Discord user already has a different Twitch linked
        sqlx::query("DELETE FROM linked_accounts WHERE discord_id = $1")
            .bind(&discord_id)
            .execute(&state.pool)
            .await?;

        sqlx::query(
            "INSERT INTO linked_accounts (discord_id, twitch_user_id, twitch_login, discord_name) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&discord_id)
        .bind(&twitch_user.id)
        .bind(&twitch_user.login)
        .bind(&discord_name)
        .execute(&state.pool)
        .await?;
    }

    // Populate cache entries for all active broadcasters in this user's guilds
    sync::populate_cache_for_user(&discord_id, &twitch_user.id, &state).await?;

    // Inline follow/subscription check using the broadcaster tokens we already
    // hold, so the role is granted within this request instead of waiting for
    // the single, rate-limited refresh worker to reach this user (which lags
    // badly when many people link at once). Best-effort per broadcaster: on any
    // API error we leave the freshly-seeded cache row (next_fetch_at = now()) for
    // the worker — which has full token-refresh handling — to retry.
    let broadcasters = sqlx::query_as::<_, (String, String)>(
        "SELECT DISTINCT ON (ucc.broadcaster_id) ucc.broadcaster_id, rl.broadcaster_access_token \
         FROM user_channel_cache ucc \
         JOIN role_links rl ON rl.broadcaster_id = ucc.broadcaster_id \
         WHERE ucc.twitch_user_id = $1 AND rl.broadcaster_access_token IS NOT NULL \
         ORDER BY ucc.broadcaster_id",
    )
    .bind(&twitch_user.id)
    .fetch_all(&state.pool)
    .await
    .unwrap_or_default();

    let mut inline_status_known = false;
    for (broadcaster_id, token) in &broadcasters {
        state.twitch_client.wait_for_permit().await;
        let follower = state
            .twitch_client
            .check_follower(broadcaster_id, &twitch_user.id, token)
            .await;
        state.twitch_client.wait_for_permit().await;
        let sub = state
            .twitch_client
            .check_subscription(broadcaster_id, &twitch_user.id, token)
            .await;

        if let (Ok(follower), Ok(sub)) = (follower, sub) {
            let is_following = follower.is_some();
            let followed_at = follower.map(|f| f.followed_at);
            let is_subscribed = sub.is_some();
            let sub_tier = sub.map(|s| s.tier).unwrap_or(0);
            // If they don't qualify yet (linked before following/subscribing, or
            // Twitch's API hasn't surfaced it), start the worker's fast re-check
            // cadence so the role still lands within a minute or two.
            let next_fetch = chrono::Utc::now()
                + chrono::Duration::seconds(if is_following || is_subscribed {
                    INITIAL_RECHECK_SECS
                } else {
                    crate::tasks::refresh_worker::FAST_RETRY_SECS
                });
            match sqlx::query(
                "UPDATE user_channel_cache SET \
                 is_following = $1, followed_at = $2, is_subscribed = $3, sub_tier = $4, \
                 fetched_at = now(), next_fetch_at = $5, fetch_failures = 0 \
                 WHERE twitch_user_id = $6 AND broadcaster_id = $7",
            )
            .bind(is_following)
            .bind(followed_at)
            .bind(is_subscribed)
            .bind(sub_tier)
            .bind(next_fetch)
            .bind(&twitch_user.id)
            .bind(broadcaster_id)
            .execute(&state.pool)
            .await
            {
                Ok(_) => inline_status_known = true,
                Err(e) => tracing::error!(discord_id, broadcaster_id, "Inline cache update failed: {e}"),
            }
        } else {
            tracing::warn!(
                discord_id, broadcaster_id,
                "Inline follow/sub check failed; deferring to worker"
            );
        }
    }

    // Apply roles now. When we know the live status, sync inline so the role is
    // granted before the page reloads and a burst of linkers parallelizes across
    // request tasks. Otherwise fall back to the worker event.
    if inline_status_known {
        if let Err(e) = sync::sync_for_user(&discord_id, &state).await {
            tracing::error!(discord_id, "Inline role sync after link failed: {e}");
            let _ = state
                .user_sync_tx
                .send(UserSyncEvent::AccountLinked {
                    discord_id: discord_id.clone(),
                })
                .await;
        }
    } else {
        let _ = state
            .user_sync_tx
            .send(UserSyncEvent::AccountLinked {
                discord_id: discord_id.clone(),
            })
            .await;
    }

    tracing::info!(
        discord_id,
        twitch_user_id = twitch_user.id,
        twitch_login = twitch_user.login,
        "Account linked"
    );

    let redirect_to = match guild_id {
        Some(g) => format!("/twitch-follower-role/verify?guild={}", g),
        None => "/twitch-follower-role/verify".to_string(),
    };
    Ok(Redirect::temporary(&redirect_to))
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

/// Per-user floor between member-triggered re-checks. The refresh worker
/// already rate-limits Twitch API calls; this just stops a page reload loop
/// from re-forcing a check the worker only just completed.
const REFRESH_COOLDOWN_SECS: f64 = 60.0;

/// Member-triggered "re-check my data now". When a linked user opens the
/// verify page the page calls this so their follow/subscription status across
/// every connected broadcaster gets re-fetched ahead of schedule and their
/// roles are corrected — no unlink/re-link needed. We don't fetch inline
/// (that would bypass the rate limiter); we just bring `next_fetch_at`
/// forward for the user's cache rows that aren't already fresh, and the
/// refresh worker re-syncs roles after it re-checks.
pub async fn refresh(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Json<Value>, AppError> {
    let (discord_id, _) = get_session(&jar, &state.config.session_secret)?;

    let affected = sqlx::query(
        "UPDATE user_channel_cache ucc SET next_fetch_at = now() \
         FROM linked_accounts la \
         WHERE la.discord_id = $1 \
           AND ucc.twitch_user_id = la.twitch_user_id \
           AND (ucc.fetched_at IS NULL OR ucc.fetched_at < now() - make_interval(secs => $2))",
    )
    .bind(&discord_id)
    .bind(REFRESH_COOLDOWN_SECS)
    .execute(&state.pool)
    .await?
    .rows_affected();

    Ok(Json(json!({ "refreshed": affected > 0 })))
}
