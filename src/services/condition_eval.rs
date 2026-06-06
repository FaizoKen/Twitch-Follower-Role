//! Rust-side condition evaluation. Sync, fast, no I/O.
//!
//! Used by `services::sync::sync_for_user` to produce add/remove decisions for
//! a single (user, role-link) pair. The bulk per-role-link path uses
//! [services::rule_sql::build_rule_where] instead — it pushes the same
//! predicates down into Postgres, and the two MUST agree.

use chrono::{DateTime, Utc};

use crate::models::condition::{Condition, ConditionOperator, ConditionTarget};
use crate::models::rule::RuleTree;

/// Plain-data view of a viewer's relationship to one connected channel, as
/// cached in `user_channel_cache`. `sub_tier` is Twitch's raw tier scale
/// (1000 / 2000 / 3000; 0 when not subscribed).
pub struct CacheData {
    pub is_following: bool,
    pub followed_at: Option<DateTime<Utc>>,
    pub is_subscribed: bool,
    pub sub_tier: i32,
}

/// Evaluate the rule tree against a viewer's cached facts.
///
/// - `grant_on_any_relation = true` short-circuits to `true`.
/// - Otherwise an empty `groups` slice returns `false` (unconfigured ⇒ nobody).
/// - Otherwise: ANY group matches (OR) and each group requires ALL of its
///   conditions to match (AND). Empty groups are FALSE (defensive; the
///   validator already rejects them at save).
pub fn evaluate(tree: &RuleTree, facts: &CacheData) -> bool {
    if tree.grant_on_any_relation {
        return true;
    }
    if tree.groups.is_empty() {
        return false;
    }
    tree.groups
        .iter()
        .any(|g| !g.conditions.is_empty() && g.conditions.iter().all(|c| evaluate_single(c, facts)))
}

/// Tier on the friendly 1/2/3 scale (0 when not subscribed). Matches the SQL
/// builder's `sub_tier / 1000`.
fn tier(facts: &CacheData) -> i64 {
    (facts.sub_tier / 1000) as i64
}

fn days_since(ts: Option<DateTime<Utc>>) -> Option<i64> {
    ts.map(|t| (Utc::now() - t).num_days())
}

fn evaluate_single(c: &Condition, f: &CacheData) -> bool {
    use ConditionTarget::*;
    match c.target {
        IsFollower => bool_match(c, f.is_following),
        IsSubscriber => bool_match(c, f.is_subscribed),
        FollowAgeDays => int_match(c, days_since(f.followed_at)),
        SubTier => int_match(c, Some(tier(f))),
    }
}

fn bool_match(c: &Condition, actual: bool) -> bool {
    if !matches!(c.operator, ConditionOperator::Eq) {
        return false;
    }
    c.value.as_bool().map(|v| v == actual).unwrap_or(false)
}

fn int_match(c: &Condition, actual: Option<i64>) -> bool {
    let Some(a) = actual else {
        return false; // missing data ⇒ fail-closed
    };
    let v = c.value.as_i64();
    match c.operator {
        ConditionOperator::Eq => v.map(|n| a == n).unwrap_or(false),
        ConditionOperator::Neq => v.map(|n| a != n).unwrap_or(false),
        ConditionOperator::Gt => v.map(|n| a > n).unwrap_or(false),
        ConditionOperator::Gte => v.map(|n| a >= n).unwrap_or(false),
        ConditionOperator::Lt => v.map(|n| a < n).unwrap_or(false),
        ConditionOperator::Lte => v.map(|n| a <= n).unwrap_or(false),
        ConditionOperator::Between => {
            let lo = v;
            let hi = c.value_end.as_ref().and_then(|x| x.as_i64());
            match (lo, hi) {
                (Some(lo), Some(hi)) => a >= lo && a <= hi,
                _ => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::condition::ConditionTarget as T;
    use crate::models::rule::{ConditionGroup, RuleTree};
    use chrono::Duration;
    use serde_json::{json, Value};

    fn c(target: T, op: ConditionOperator, value: Value) -> Condition {
        Condition {
            target,
            operator: op,
            value,
            value_end: None,
        }
    }

    fn one_group(conds: Vec<Condition>) -> RuleTree {
        RuleTree {
            grant_on_any_relation: false,
            groups: vec![ConditionGroup { conditions: conds }],
        }
    }

    fn or_groups(g: Vec<Vec<Condition>>) -> RuleTree {
        RuleTree {
            grant_on_any_relation: false,
            groups: g
                .into_iter()
                .map(|cs| ConditionGroup { conditions: cs })
                .collect(),
        }
    }

    fn facts() -> CacheData {
        CacheData {
            is_following: false,
            followed_at: None,
            is_subscribed: false,
            sub_tier: 0,
        }
    }

    #[test]
    fn unconfigured_grants_nobody() {
        assert!(!evaluate(&RuleTree::default(), &facts()));
    }

    #[test]
    fn grant_on_any_short_circuits_true() {
        let t = RuleTree {
            grant_on_any_relation: true,
            groups: vec![],
        };
        assert!(evaluate(&t, &facts()));
    }

    #[test]
    fn empty_group_is_false_defensive() {
        let t = RuleTree {
            grant_on_any_relation: false,
            groups: vec![ConditionGroup { conditions: vec![] }],
        };
        assert!(!evaluate(&t, &facts()));
    }

    #[test]
    fn and_all_conditions_required() {
        let t = one_group(vec![
            c(T::IsSubscriber, ConditionOperator::Eq, json!(true)),
            c(T::SubTier, ConditionOperator::Gte, json!(2)),
        ]);
        let mut f = facts();
        f.is_subscribed = true;
        f.sub_tier = 3000;
        assert!(evaluate(&t, &f));
        f.sub_tier = 1000;
        assert!(!evaluate(&t, &f));
    }

    #[test]
    fn or_any_group_satisfies() {
        // (follower AND followed >=30d) OR (subscriber tier >=2)
        let t = or_groups(vec![
            vec![
                c(T::IsFollower, ConditionOperator::Eq, json!(true)),
                c(T::FollowAgeDays, ConditionOperator::Gte, json!(30)),
            ],
            vec![c(T::SubTier, ConditionOperator::Gte, json!(2))],
        ]);

        let mut f = facts();
        f.is_following = true;
        f.followed_at = Some(Utc::now() - Duration::days(45));
        assert!(evaluate(&t, &f));

        // follow path fails (too recent); sub path matches
        f.followed_at = Some(Utc::now() - Duration::days(5));
        f.is_subscribed = true;
        f.sub_tier = 2000;
        assert!(evaluate(&t, &f));

        // both fail
        f.sub_tier = 1000;
        assert!(!evaluate(&t, &f));
    }

    #[test]
    fn follow_age_missing_fails_closed() {
        let t = one_group(vec![c(T::FollowAgeDays, ConditionOperator::Gte, json!(0))]);
        let f = facts(); // followed_at = None
        assert!(!evaluate(&t, &f));
    }

    #[test]
    fn tier_scale_division() {
        let t = one_group(vec![c(T::SubTier, ConditionOperator::Eq, json!(3))]);
        let mut f = facts();
        f.is_subscribed = true;
        f.sub_tier = 3000;
        assert!(evaluate(&t, &f));
        f.sub_tier = 2000;
        assert!(!evaluate(&t, &f));
    }

    #[test]
    fn between_inclusive_on_follow_age() {
        let mut cond = c(T::FollowAgeDays, ConditionOperator::Between, json!(30));
        cond.value_end = Some(json!(90));
        let t = one_group(vec![cond]);
        let mut f = facts();
        f.followed_at = Some(Utc::now() - Duration::days(60));
        assert!(evaluate(&t, &f));
        f.followed_at = Some(Utc::now() - Duration::days(120));
        assert!(!evaluate(&t, &f));
    }
}
