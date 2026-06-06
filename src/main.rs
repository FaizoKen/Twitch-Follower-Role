use std::sync::Arc;

use axum::http::{header, HeaderValue, Method};
use axum::middleware;
use axum::routing::{delete, get, post};
use axum::Router;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

mod config;
mod db;
mod error;
mod models;
mod routes;
mod schema;
mod services;
mod tasks;

use services::rolelogic::RoleLogicClient;
use services::security_headers;
use services::sync::{ConfigSyncEvent, UserSyncEvent};
use services::twitch::TwitchClient;

pub struct AppState {
    pub pool: PgPool,
    pub config: config::AppConfig,
    pub user_sync_tx: mpsc::Sender<UserSyncEvent>,
    pub config_sync_tx: mpsc::Sender<ConfigSyncEvent>,
    pub twitch_client: TwitchClient,
    pub rl_client: RoleLogicClient,
    pub http: reqwest::Client,
    pub verify_html: bytes::Bytes,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "twitch_follower_role=info,tower_http=info".into()),
        )
        .init();

    let app_config = config::AppConfig::from_env();
    let listen_addr = app_config.listen_addr.clone();

    let pool = db::create_pool(&app_config.database_url).await;
    db::run_migrations(&pool).await;
    tracing::info!("Database connected and migrations applied");

    let (user_sync_tx, user_sync_rx) = mpsc::channel::<UserSyncEvent>(512);
    let (config_sync_tx, config_sync_rx) = mpsc::channel::<ConfigSyncEvent>(64);

    let twitch_client = TwitchClient::new(
        &app_config.twitch_client_id,
        &app_config.twitch_client_secret,
        &app_config.twitch_eventsub_secret,
    );
    let rl_client = RoleLogicClient::new();
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("Failed to build HTTP client");
    let verify_html =
        bytes::Bytes::from(routes::verification::render_verify_page(&app_config.base_url));

    let state = Arc::new(AppState {
        pool,
        config: app_config,
        user_sync_tx,
        config_sync_tx,
        twitch_client,
        rl_client,
        http,
        verify_html,
    });

    // Spawn background workers
    tokio::spawn(tasks::refresh_worker::run(Arc::clone(&state)));
    tokio::spawn(tasks::user_sync_worker::run(user_sync_rx, Arc::clone(&state)));
    tokio::spawn(tasks::config_sync_worker::run(config_sync_rx, Arc::clone(&state)));
    tokio::spawn(tasks::cleanup_expired(Arc::clone(&state)));

    // CORS: explicit allowlist (this plugin's origin + the RoleLogic dashboard
    // that embeds the iframe). `allow_credentials(true)` requires explicit
    // origins (no wildcard), which is why we don't use `permissive()`.
    let cors_origins: Vec<HeaderValue> = state
        .config
        .allowed_origins
        .iter()
        .filter_map(|o| HeaderValue::from_str(o).ok())
        .collect();
    let cors_layer = CorsLayer::new()
        .allow_origin(cors_origins)
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION])
        .allow_credentials(true)
        .max_age(std::time::Duration::from_secs(600));

    let app = Router::new()
        .nest("/twitch-follower-role", Router::new()
            // Plugin endpoints (called by RoleLogic)
            .route("/register", post(routes::plugin::register))
            .route("/config", get(routes::plugin::get_config))
            .route("/config", post(routes::plugin::post_config))
            .route("/config", delete(routes::plugin::delete_config))
            // Admin — iframe role-config (deep-linked from the RoleLogic dashboard)
            .route("/admin/{guild_id}/role/{role_id}", get(routes::admin::role_config_page))
            .route("/admin/{guild_id}/role/{role_id}/data", get(routes::admin::role_config_data))
            .route("/admin/{guild_id}/role/{role_id}/save", post(routes::admin::role_config_save))
            .route(
                "/admin/{guild_id}/role/{role_id}/preview",
                get(routes::admin::role_config_preview).post(routes::admin::role_config_preview_edit),
            )
            .route("/admin/{guild_id}/role/{role_id}/connect", post(routes::admin::broadcaster_connect))
            .route("/admin/{guild_id}/role/{role_id}/disconnect", post(routes::admin::broadcaster_disconnect))
            // Verification endpoints (user-facing)
            .route("/verify", get(routes::verification::verify_page))
            .route("/verify/login", get(routes::verification::login))
            .route("/verify/status", get(routes::verification::status))
            .route("/verify/refresh", post(routes::verification::refresh))
            .route("/verify/twitch", get(routes::verification::twitch_login))
            .route("/verify/twitch/callback", get(routes::verification::twitch_callback))
            .route("/verify/unlink", post(routes::verification::unlink))
            .route("/verify/logout", post(routes::verification::logout))
            // Broadcaster connection
            .route("/connect", get(routes::broadcaster::connect))
            .route("/connect/callback", get(routes::broadcaster::connect_callback))
            // EventSub webhook
            .route("/webhooks/twitch", post(routes::webhooks::eventsub_handler))
            // Health & static
            .route("/favicon.ico", get(routes::health::favicon))
            .route("/health", get(routes::health::health))
        )
        .layer(TraceLayer::new_for_http())
        .layer(cors_layer)
        .layer(middleware::from_fn(security_headers::baseline))
        .with_state(state);

    tracing::info!("Server starting on {listen_addr}");

    let listener = tokio::net::TcpListener::bind(&listen_addr)
        .await
        .expect("Failed to bind listener");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("Shutdown signal received, draining connections...");
        })
        .await
        .expect("Server error");

    tracing::info!("Server stopped");
}
