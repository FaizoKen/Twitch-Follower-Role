use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub async fn create_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(8)
        .min_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .idle_timeout(std::time::Duration::from_secs(600))
        .connect(database_url)
        .await
        .expect("Failed to connect to PostgreSQL")
}

pub async fn run_migrations(pool: &PgPool) {
    sqlx::raw_sql(include_str!("../migrations/001_initial_schema.sql"))
        .execute(pool)
        .await
        .expect("Failed to run migration 001");

    sqlx::raw_sql(include_str!("../migrations/002_add_indexes.sql"))
        .execute(pool)
        .await
        .expect("Failed to run migration 002");

    sqlx::raw_sql(include_str!("../migrations/003_drop_discord_tables.sql"))
        .execute(pool)
        .await
        .expect("Failed to run migration 003");

    sqlx::raw_sql(include_str!("../migrations/004_rule_tree.sql"))
        .execute(pool)
        .await
        .expect("Failed to run migration 004");

    sqlx::raw_sql(include_str!("../migrations/005_guild_settings.sql"))
        .execute(pool)
        .await
        .expect("Failed to run migration 005");

    sqlx::raw_sql(include_str!("../migrations/006_linked_discord_name.sql"))
        .execute(pool)
        .await
        .expect("Failed to run migration 006");
}
