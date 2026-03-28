CREATE INDEX IF NOT EXISTS idx_ucc_next_fetch ON user_channel_cache (next_fetch_at ASC);
CREATE INDEX IF NOT EXISTS idx_ucc_broadcaster ON user_channel_cache (broadcaster_id);
CREATE INDEX IF NOT EXISTS idx_linked_twitch ON linked_accounts (twitch_user_id);
CREATE INDEX IF NOT EXISTS idx_role_links_token ON role_links (api_token);
CREATE INDEX IF NOT EXISTS idx_role_links_broadcaster ON role_links (broadcaster_id) WHERE broadcaster_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_user_guilds_guild ON user_guilds (guild_id);
CREATE INDEX IF NOT EXISTS idx_eventsub_broadcaster ON eventsub_subscriptions (broadcaster_id);
CREATE INDEX IF NOT EXISTS idx_role_assignments_discord ON role_assignments (discord_id);
