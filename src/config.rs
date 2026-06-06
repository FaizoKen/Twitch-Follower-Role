use std::env;

#[derive(Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub session_secret: String,
    pub twitch_client_id: String,
    pub twitch_client_secret: String,
    pub twitch_eventsub_secret: String,
    pub base_url: String,
    pub listen_addr: String,
    /// Base URL of the Auth Gateway (no trailing slash, no `/auth` suffix).
    /// Prod: usually the same origin as `BASE_URL` (derived if unset).
    /// Local dev: set to the gateway's local listener, e.g. http://localhost:8090
    pub auth_gateway_url: String,
    /// Shared secret for plugin → gateway /auth/internal/* calls
    /// (sent in the `X-Internal-Key` header). Must match INTERNAL_API_KEY on the gateway.
    pub internal_api_key: String,
    /// Origin allowed to embed the iframe role-config page (RoleLogic
    /// dashboard). Used to build `Content-Security-Policy: frame-ancestors …`.
    /// Unset → falls back to the production dashboard origin.
    pub rl_dashboard_origin: Option<String>,
    /// Origins permitted to drive cookie-authenticated state-changing admin
    /// requests. Source of truth for both the `CorsLayer` allowlist and the
    /// per-handler `csrf::verify_origin` check. Derived from `base_url` origin
    /// plus `rl_dashboard_origin`.
    pub allowed_origins: Vec<String>,
}

/// Extract the origin (scheme://host[:port]) from BASE_URL, dropping any path prefix.
pub(crate) fn derive_origin(base_url: &str) -> String {
    if let Some(scheme_end) = base_url.find("://") {
        let after_scheme = scheme_end + 3;
        if let Some(path_slash) = base_url[after_scheme..].find('/') {
            return base_url[..after_scheme + path_slash].to_string();
        }
    }
    base_url.to_string()
}

impl AppConfig {
    pub fn from_env() -> Self {
        let base_url = env::var("BASE_URL").expect("BASE_URL must be set");
        let auth_gateway_url = env::var("AUTH_GATEWAY_URL")
            .ok()
            .map(|s| s.trim_end_matches('/').to_string())
            .unwrap_or_else(|| derive_origin(&base_url));

        let rl_dashboard_origin = env::var("RL_DASHBOARD_ORIGIN")
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| Some("https://rolelogic.faizo.net".to_string()));

        // Origins allowed to issue cookie-authed state changes: this plugin's
        // own origin plus the dashboard that embeds the iframe.
        let mut allowed_origins = vec![derive_origin(&base_url)];
        if let Some(dash) = rl_dashboard_origin.as_deref() {
            if !allowed_origins.iter().any(|o| o == dash) {
                allowed_origins.push(dash.to_string());
            }
        }

        Self {
            database_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
            session_secret: env::var("SESSION_SECRET").expect("SESSION_SECRET must be set"),
            twitch_client_id: env::var("TWITCH_CLIENT_ID")
                .expect("TWITCH_CLIENT_ID must be set"),
            twitch_client_secret: env::var("TWITCH_CLIENT_SECRET")
                .expect("TWITCH_CLIENT_SECRET must be set"),
            twitch_eventsub_secret: env::var("TWITCH_EVENTSUB_SECRET")
                .expect("TWITCH_EVENTSUB_SECRET must be set"),
            base_url,
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
            auth_gateway_url,
            internal_api_key: env::var("INTERNAL_API_KEY")
                .expect("INTERNAL_API_KEY must be set (must match the Auth Gateway's value)"),
            rl_dashboard_origin,
            allowed_origins,
        }
    }

    pub fn twitch_user_oauth_redirect_uri(&self) -> String {
        format!("{}/verify/twitch/callback", self.base_url)
    }

    pub fn broadcaster_oauth_redirect_uri(&self) -> String {
        format!("{}/connect/callback", self.base_url)
    }
}
