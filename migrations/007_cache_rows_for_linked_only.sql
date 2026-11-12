-- `user_channel_cache` only ever means something for a linked member: the
-- refresh worker, the role sync and the users page all reach it through
-- `linked_accounts`. The EventSub handlers used to insert a row for every new
-- follower or subscriber of every connected channel, linked or not, and
-- unlinking left a member's rows behind. By 2026-11 production held 46,168 of
-- those rows against 3,322 real ones, and because the worker sized its
-- re-check interval from the table's row count, an unfollow took close to
-- seven hours to notice instead of thirty minutes.
--
-- The foreign key makes that state unrepresentable: a cache row cannot exist
-- without its linked account, and unlinking (or re-linking to another Twitch
-- account) takes the rows with it.
--
-- Guarded on the constraint so the delete runs once; migrations re-run on
-- every boot.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'user_channel_cache_linked_fk'
    ) THEN
        DELETE FROM user_channel_cache ucc
        WHERE NOT EXISTS (
            SELECT 1 FROM linked_accounts la WHERE la.twitch_user_id = ucc.twitch_user_id
        );

        ALTER TABLE user_channel_cache
            ADD CONSTRAINT user_channel_cache_linked_fk
            FOREIGN KEY (twitch_user_id) REFERENCES linked_accounts (twitch_user_id)
            ON DELETE CASCADE;
    END IF;
END $$;

-- When the row last had a full check: follow and subscription both fetched
-- from Twitch, and the member's roles re-evaluated whether or not anything
-- changed. The quick re-check between full ones only asks Twitch about the
-- follow. NULL means "never", so every row gets one full check after this
-- ships. Deliberately not backfilled: a re-run would stamp rows that have
-- never been checked at all.
ALTER TABLE user_channel_cache ADD COLUMN IF NOT EXISTS full_fetched_at TIMESTAMPTZ;
