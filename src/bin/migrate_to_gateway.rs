//! One-shot migration tool: copy `discord_tokens` and `user_guilds` rows
//! from this plugin's database into the centralized Auth Gateway database.
//!
//! Insert-only semantics (`ON CONFLICT DO NOTHING`): the gateway is the
//! source of truth — its existing rows are never overwritten. The plugin's
//! rows only fill gaps so that no historically-verified user disappears.
//!
//! Skips `oauth_states` entirely. Unlike the Genshin migration, the reason
//! here is different: this plugin's `oauth_states` is still in active use
//! by its own Twitch OAuth flow and must remain in the local DB.
//!
//! Usage (PowerShell):
//!   $env:DATABASE_URL="postgres://tcr:password@localhost:5432/twitch_follower_role"
//!   $env:GATEWAY_DATABASE_URL="postgres://app:password@localhost:5432/auth_gateway"
//!   cargo run --bin migrate_to_gateway
//!
//! Idempotent: safe to re-run; rerunning is a no-op once both DBs converge.

use std::env;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

const BATCH_SIZE: usize = 500;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let plugin_url = env::var("DATABASE_URL").expect("DATABASE_URL (Twitch plugin DB) must be set");
    let gateway_url = env::var("GATEWAY_DATABASE_URL")
        .expect("GATEWAY_DATABASE_URL (Auth Gateway DB) must be set");

    println!("Connecting to source (Twitch plugin)...");
    let src = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&plugin_url)
        .await
        .expect("failed to connect to Twitch plugin DB");

    println!("Connecting to destination (Auth Gateway)...");
    let dst = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&gateway_url)
        .await
        .expect("failed to connect to Auth Gateway DB");

    // Sanity-check that the destination tables exist; they should, since the
    // gateway runs its own 001_initial_schema + 002_manage_guild migrations.
    let dst_ok = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'user_guilds')",
    )
    .fetch_one(&dst)
    .await
    .expect("failed to introspect destination DB");
    if !dst_ok {
        eprintln!(
            "ERROR: destination DB has no `user_guilds` table. \
             Make sure the Auth Gateway has been deployed and its migrations \
             have run before running this script."
        );
        std::process::exit(1);
    }

    migrate_discord_tokens(&src, &dst).await;
    migrate_user_guilds(&src, &dst).await;

    println!(
        "\nDone. oauth_states was intentionally skipped — \
         it stays in the Twitch plugin DB for the Twitch OAuth flow."
    );
}

async fn migrate_discord_tokens(src: &PgPool, dst: &PgPool) {
    println!("\n=== discord_tokens ===");

    // Tolerate the source table being absent (e.g. someone runs this against
    // a freshly-bootstrapped plugin DB, or after the drop migration has run).
    let src_has_table = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'discord_tokens')",
    )
    .fetch_one(src)
    .await
    .expect("failed to introspect source DB");
    if !src_has_table {
        println!("Source has no discord_tokens table — nothing to migrate.");
        return;
    }

    let rows = sqlx::query(
        "SELECT discord_id, refresh_token, guilds_refreshed_at, created_at \
         FROM discord_tokens",
    )
    .fetch_all(src)
    .await
    .expect("failed to read discord_tokens from source");

    let total = rows.len();
    println!("Read {total} row(s) from source");

    let mut inserted: u64 = 0;
    for chunk in rows.chunks(BATCH_SIZE) {
        let mut tx = dst.begin().await.expect("failed to begin tx on dst");
        for row in chunk {
            let discord_id: String = row.get("discord_id");
            let refresh_token: String = row.get("refresh_token");
            let guilds_refreshed_at: chrono::DateTime<chrono::Utc> = row.get("guilds_refreshed_at");
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");

            let result = sqlx::query(
                "INSERT INTO discord_tokens (discord_id, refresh_token, guilds_refreshed_at, created_at) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (discord_id) DO NOTHING",
            )
            .bind(&discord_id)
            .bind(&refresh_token)
            .bind(guilds_refreshed_at)
            .bind(created_at)
            .execute(&mut *tx)
            .await
            .expect("insert into dst.discord_tokens failed");
            inserted += result.rows_affected();
        }
        tx.commit().await.expect("commit failed");
    }

    let skipped = total as u64 - inserted;
    println!("discord_tokens: read={total} inserted={inserted} skipped={skipped}");
}

async fn migrate_user_guilds(src: &PgPool, dst: &PgPool) {
    println!("\n=== user_guilds ===");

    let src_has_table = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'user_guilds')",
    )
    .fetch_one(src)
    .await
    .expect("failed to introspect source DB");
    if !src_has_table {
        println!("Source has no user_guilds table — nothing to migrate.");
        return;
    }

    // Detect optional columns on the source side. Older snapshots may
    // pre-date `guild_name` or `manage_guild`.
    let has_guild_name = column_exists(src, "user_guilds", "guild_name").await;
    let has_manage_guild = column_exists(src, "user_guilds", "manage_guild").await;

    let select_sql = format!(
        "SELECT discord_id, guild_id, {gn}, {mg}, updated_at FROM user_guilds",
        gn = if has_guild_name {
            "guild_name"
        } else {
            "NULL::text AS guild_name"
        },
        mg = if has_manage_guild {
            "manage_guild"
        } else {
            "FALSE::boolean AS manage_guild"
        },
    );

    let rows = sqlx::query(&select_sql)
        .fetch_all(src)
        .await
        .expect("failed to read user_guilds from source");

    let total = rows.len();
    println!("Read {total} row(s) from source");

    let mut inserted: u64 = 0;
    for chunk in rows.chunks(BATCH_SIZE) {
        let mut tx = dst.begin().await.expect("failed to begin tx on dst");
        for row in chunk {
            let discord_id: String = row.get("discord_id");
            let guild_id: String = row.get("guild_id");
            let guild_name: Option<String> = row.try_get("guild_name").ok();
            let manage_guild: bool = row.try_get("manage_guild").unwrap_or(false);
            let updated_at: chrono::DateTime<chrono::Utc> = row.get("updated_at");

            let result = sqlx::query(
                "INSERT INTO user_guilds (discord_id, guild_id, guild_name, manage_guild, updated_at) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (discord_id, guild_id) DO NOTHING",
            )
            .bind(&discord_id)
            .bind(&guild_id)
            .bind(&guild_name)
            .bind(manage_guild)
            .bind(updated_at)
            .execute(&mut *tx)
            .await
            .expect("insert into dst.user_guilds failed");
            inserted += result.rows_affected();
        }
        tx.commit().await.expect("commit failed");
    }

    let skipped = total as u64 - inserted;
    println!("user_guilds: read={total} inserted={inserted} skipped={skipped}");
}

async fn column_exists(pool: &PgPool, table: &str, column: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2)",
    )
    .bind(table)
    .bind(column)
    .fetch_one(pool)
    .await
    .unwrap_or(false)
}
