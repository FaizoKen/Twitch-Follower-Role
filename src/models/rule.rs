//! The rule tree: OR of AND-groups (DNF).
//!
//! Stored verbatim as the JSONB `rule_tree` column on `role_links`. Two-level
//! structure keeps validation, SQL translation, and the iframe rule-builder
//! UI simple while still expressing every boolean rule (any boolean
//! expression has a DNF form).
//!
//! Invariant: an unconfigured role link grants the role to nobody.
//! `grant_on_any_relation = false` AND `groups.is_empty()` means "match
//! nobody" — both [services::condition_eval::evaluate] and the SQL builder
//! enforce this BEFORE inspecting groups.

use serde::{Deserialize, Serialize};

use crate::models::condition::Condition;

/// Maximum top-level groups. Generous: a 3-tier role hierarchy with an
/// "OR catch-all" still fits comfortably.
pub const MAX_GROUPS: usize = 8;
/// Maximum conditions per group. Twitch only has four facts, so a group can
/// never need more than four distinct conditions — 8 leaves slack for
/// duplicated bounds (e.g. follow_age between two values).
pub const MAX_CONDITIONS_PER_GROUP: usize = 8;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuleTree {
    #[serde(default)]
    pub grant_on_any_relation: bool,
    #[serde(default)]
    pub groups: Vec<ConditionGroup>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConditionGroup {
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::condition::{ConditionOperator, ConditionTarget};

    /// The exact JSON shape migration 004 writes when backfilling a fully
    /// configured legacy role link (follower + min-days + subscriber + tier).
    /// This locks the contract between the SQL backfill and the Rust model: if
    /// either drifts, this fails.
    #[test]
    fn deserializes_migration_backfill_shape() {
        let raw = serde_json::json!({
            "grant_on_any_relation": false,
            "groups": [{
                "conditions": [
                    {"target": "is_follower", "operator": "eq", "value": true},
                    {"target": "follow_age_days", "operator": "gte", "value": 30},
                    {"target": "is_subscriber", "operator": "eq", "value": true},
                    {"target": "sub_tier", "operator": "gte", "value": 2}
                ]
            }]
        });
        let tree: RuleTree = serde_json::from_value(raw).expect("migration shape must parse");
        assert!(!tree.grant_on_any_relation);
        assert_eq!(tree.groups.len(), 1);
        let cs = &tree.groups[0].conditions;
        assert_eq!(cs.len(), 4);
        assert_eq!(cs[0].target, ConditionTarget::IsFollower);
        assert_eq!(cs[0].operator, ConditionOperator::Eq);
        assert_eq!(cs[1].target, ConditionTarget::FollowAgeDays);
        assert_eq!(cs[1].value.as_i64(), Some(30));
        assert_eq!(cs[3].target, ConditionTarget::SubTier);
        assert_eq!(cs[3].operator, ConditionOperator::Gte);
        assert_eq!(cs[3].value.as_i64(), Some(2));
    }

    /// The default/unconfigured shape the migration writes for an empty legacy
    /// `conditions` blob — and the column DEFAULT for freshly registered links.
    #[test]
    fn deserializes_unconfigured_shape() {
        let raw = serde_json::json!({"grant_on_any_relation": false, "groups": []});
        let tree: RuleTree = serde_json::from_value(raw).unwrap();
        assert!(!tree.grant_on_any_relation);
        assert!(tree.groups.is_empty());
    }
}
