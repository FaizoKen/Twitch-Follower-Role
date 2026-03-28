use std::env;

#[derive(Clone)]
pub struct AppConfig {
    pub database_url: String,
    pub discord_client_id: String,
    pub discord_client_secret: String,
    pub session_secret: String,
    pub twitch_client_id: String,
    pub twitch_client_secret: String,
    pub twitch_eventsub_secret: String,
    pub base_url: String,
    pub listen_addr: String,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            database_url: env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
            discord_client_id: env::var("DISCORD_CLIENT_ID")
                .expect("DISCORD_CLIENT_ID must be set"),
            discord_client_secret: env::var("DISCORD_CLIENT_SECRET")
                .expect("DISCORD_CLIENT_SECRET must be set"),
            session_secret: env::var("SESSION_SECRET").expect("SESSION_SECRET must be set"),
            twitch_client_id: env::var("TWITCH_CLIENT_ID")
                .expect("TWITCH_CLIENT_ID must be set"),
            twitch_client_secret: env::var("TWITCH_CLIENT_SECRET")
                .expect("TWITCH_CLIENT_SECRET must be set"),
            twitch_eventsub_secret: env::var("TWITCH_EVENTSUB_SECRET")
                .expect("TWITCH_EVENTSUB_SECRET must be set"),
            base_url: env::var("BASE_URL").expect("BASE_URL must be set"),
            listen_addr: env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string()),
        }
    }

    pub fn discord_oauth_redirect_uri(&self) -> String {
        format!("{}/verify/callback", self.base_url)
    }

    pub fn twitch_user_oauth_redirect_uri(&self) -> String {
        format!("{}/verify/twitch/callback", self.base_url)
    }

    pub fn broadcaster_oauth_redirect_uri(&self) -> String {
        format!("{}/connect/callback", self.base_url)
    }
}
