-- Migrate role_links from the legacy AND-only `conditions` blob to a DNF
-- `rule_tree` (OR of AND-groups), matching the iframe rule-builder UI.
--
-- `rule_tree_version` backs optimistic locking so two dashboard tabs can't
-- silently clobber each other on save.

ALTER TABLE role_links
    ADD COLUMN IF NOT EXISTS rule_tree JSONB NOT NULL
        DEFAULT '{"grant_on_any_relation": false, "groups": []}';
ALTER TABLE role_links
    ADD COLUMN IF NOT EXISTS rule_tree_version INTEGER NOT NULL DEFAULT 0;

-- One-time backfill + drop. Guarded on the still-existing `conditions` column
-- so this whole block is a no-op on every boot after the first (migrations
-- re-run on each startup). Converting here, then dropping `conditions`, makes
-- the legacy column vestigial and prevents a later "nobody" rule from being
-- silently resurrected from stale legacy data.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'role_links' AND column_name = 'conditions'
    ) THEN
        UPDATE role_links SET rule_tree = jsonb_build_object(
            'grant_on_any_relation', false,
            'groups',
            CASE WHEN cond_array = '[]'::jsonb THEN '[]'::jsonb
                 ELSE jsonb_build_array(jsonb_build_object('conditions', cond_array))
            END
        )
        FROM (
            SELECT id,
                (
                    (CASE WHEN COALESCE((conditions->>'require_follower')::boolean, false)
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'is_follower', 'operator', 'eq', 'value', true))
                          ELSE '[]'::jsonb END)
                    ||
                    (CASE WHEN COALESCE((conditions->>'require_follower')::boolean, false)
                           AND COALESCE((conditions->>'min_follow_days')::int, 0) > 0
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'follow_age_days', 'operator', 'gte',
                                   'value', (conditions->>'min_follow_days')::int))
                          ELSE '[]'::jsonb END)
                    ||
                    (CASE WHEN COALESCE((conditions->>'require_subscriber')::boolean, false)
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'is_subscriber', 'operator', 'eq', 'value', true))
                          ELSE '[]'::jsonb END)
                    ||
                    (CASE WHEN COALESCE((conditions->>'require_subscriber')::boolean, false)
                           AND COALESCE((conditions->>'min_sub_tier')::int, 1) > 1
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'sub_tier', 'operator', 'gte',
                                   'value', (conditions->>'min_sub_tier')::int))
                          ELSE '[]'::jsonb END)
                ) AS cond_array
            FROM role_links
        ) AS computed
        WHERE role_links.id = computed.id;

        ALTER TABLE role_links DROP COLUMN conditions;
    END IF;
END $$;
