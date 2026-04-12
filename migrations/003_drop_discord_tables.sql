-- Discord-related tables now live exclusively in the Auth Gateway database.
-- NOTE: oauth_states is intentionally NOT dropped — it's used by this plugin's
-- own Twitch OAuth flow (broadcaster onboarding + user linking).
-- Run `cargo run --bin migrate_to_gateway` BEFORE this migration ships.
DROP TABLE IF EXISTS user_guilds;
DROP TABLE IF EXISTS discord_tokens;
