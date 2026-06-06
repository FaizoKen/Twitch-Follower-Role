-- Cache the member's Discord display name on their linked account so the
-- public users list can show a real name instead of a raw snowflake. Sourced
-- from the signed `rl_session` cookie (minted by the Auth Gateway), captured
-- at link time and refreshed whenever the member reopens the verify page — no
-- extra Discord API calls. NULL until first seen; the UI falls back to the
-- Discord ID.

ALTER TABLE linked_accounts ADD COLUMN IF NOT EXISTS discord_name TEXT;
