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
--
-- Numeric fields are extracted defensively: the legacy `min_follow_days` was an
-- i64 and could hold values far beyond int4 range (a production row had 1e16),
-- so we (a) only cast clean 1-18 digit integers (≤ i64 range), (b) cast to
-- bigint, and (c) clamp to int4 max. Anything malformed/huge collapses to the
-- default, which is behaviourally identical to the old evaluator (a follow-age
-- bound nobody can satisfy already matched no one).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'role_links' AND column_name = 'conditions'
    ) THEN
        UPDATE role_links SET rule_tree = jsonb_build_object(
            'grant_on_any_relation', false,
            'groups',
            CASE WHEN computed.cond_array = '[]'::jsonb THEN '[]'::jsonb
                 ELSE jsonb_build_array(jsonb_build_object('conditions', computed.cond_array))
            END
        )
        FROM (
            SELECT id,
                (
                    (CASE WHEN req_follower
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'is_follower', 'operator', 'eq', 'value', true))
                          ELSE '[]'::jsonb END)
                    ||
                    (CASE WHEN req_follower AND min_days > 0
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'follow_age_days', 'operator', 'gte', 'value', min_days))
                          ELSE '[]'::jsonb END)
                    ||
                    (CASE WHEN req_subscriber
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'is_subscriber', 'operator', 'eq', 'value', true))
                          ELSE '[]'::jsonb END)
                    ||
                    (CASE WHEN req_subscriber AND min_tier > 1
                          THEN jsonb_build_array(jsonb_build_object(
                                   'target', 'sub_tier', 'operator', 'gte', 'value', min_tier))
                          ELSE '[]'::jsonb END)
                ) AS cond_array
            FROM (
                SELECT id,
                    COALESCE((conditions->>'require_follower')::boolean, false)   AS req_follower,
                    COALESCE((conditions->>'require_subscriber')::boolean, false) AS req_subscriber,
                    CASE WHEN conditions->>'min_follow_days' ~ '^[0-9]{1,18}$'
                         THEN LEAST((conditions->>'min_follow_days')::bigint, 2147483647)
                         ELSE 0 END AS min_days,
                    CASE WHEN conditions->>'min_sub_tier' ~ '^[0-9]{1,18}$'
                         THEN LEAST((conditions->>'min_sub_tier')::bigint, 2147483647)
                         ELSE 1 END AS min_tier
                FROM role_links
            ) cleaned
        ) AS computed
        WHERE role_links.id = computed.id;

        ALTER TABLE role_links DROP COLUMN conditions;
    END IF;
END $$;
