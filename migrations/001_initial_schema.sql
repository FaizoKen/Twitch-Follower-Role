-- Role links with broadcaster connection
CREATE TABLE IF NOT EXISTS role_links (
    id                          BIGSERIAL PRIMARY KEY,
    guild_id                    TEXT NOT NULL,
    role_id                     TEXT NOT NULL,
    api_token                   TEXT NOT NULL,
    conditions                  JSONB NOT NULL DEFAULT '{}',
    broadcaster_id              TEXT,
    broadcaster_login           TEXT,
    broadcaster_access_token    TEXT,
    broadcaster_refresh_token   TEXT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (guild_id, role_id)
);

-- EventSub subscription tracking (per broadcaster)
CREATE TABLE IF NOT EXISTS eventsub_subscriptions (
    id                  TEXT PRIMARY KEY,
    broadcaster_id      TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (broadcaster_id, event_type)
);

-- Discord <-> Twitch user mapping
CREATE TABLE IF NOT EXISTS linked_accounts (
    id              BIGSERIAL PRIMARY KEY,
    discord_id      TEXT NOT NULL UNIQUE,
    twitch_user_id  TEXT NOT NULL UNIQUE,
    twitch_login    TEXT NOT NULL,
    linked_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Cached user-channel relationship data
CREATE TABLE IF NOT EXISTS user_channel_cache (
    twitch_user_id  TEXT NOT NULL,
    broadcaster_id  TEXT NOT NULL,
    is_following    BOOLEAN NOT NULL DEFAULT false,
    followed_at     TIMESTAMPTZ,
    is_subscribed   BOOLEAN NOT NULL DEFAULT false,
    sub_tier        INTEGER NOT NULL DEFAULT 0,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_fetch_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    fetch_failures  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (twitch_user_id, broadcaster_id)
);

-- Role assignments: tracks which users currently have which roles (local mirror)
CREATE TABLE IF NOT EXISTS role_assignments (
    guild_id    TEXT NOT NULL,
    role_id     TEXT NOT NULL,
    discord_id  TEXT NOT NULL,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (guild_id, role_id, discord_id),
    FOREIGN KEY (guild_id, role_id) REFERENCES role_links (guild_id, role_id) ON DELETE CASCADE
);

-- OAuth states: CSRF protection for Discord/Twitch OAuth flows
CREATE TABLE IF NOT EXISTS oauth_states (
    state           TEXT PRIMARY KEY,
    redirect_data   JSONB,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- User guild memberships (captured via Discord OAuth)
CREATE TABLE IF NOT EXISTS user_guilds (
    discord_id  TEXT NOT NULL,
    guild_id    TEXT NOT NULL,
    guild_name  TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (discord_id, guild_id)
);

-- Discord refresh tokens for guild list refresh
CREATE TABLE IF NOT EXISTS discord_tokens (
    discord_id          TEXT PRIMARY KEY,
    refresh_token       TEXT NOT NULL,
    guilds_refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
