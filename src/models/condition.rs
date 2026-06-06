//! Condition target / operator types used in the rule tree.
//!
//! - `ConditionTarget` names a fact we can read about a (viewer × channel)
//!   pair from `user_channel_cache`.
//! - `ConditionOperator` names a comparison.
//! - Validity of a (target, operator) combination is enforced at save time
//!   in [services::rule_validator] using each target's `kind()`.
//!
//! Twitch only exposes four facts (follow + sub status), so the catalog is
//! deliberately small — but the DNF rule tree built from them still lets
//! admins express any OR-of-AND combination.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What kind of data this target produces. Drives which operators are valid
/// and how the rule_validator coerces literal values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    Bool,
    Int,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionTarget {
    /// Currently follows the connected channel.
    IsFollower,
    /// Whole days since the viewer first followed.
    FollowAgeDays,
    /// Has an active paid subscription right now.
    IsSubscriber,
    /// Subscription tier: 1, 2, or 3 (0 when not subscribed).
    SubTier,
}

impl ConditionTarget {
    pub fn kind(self) -> TargetKind {
        use ConditionTarget::*;
        match self {
            IsFollower | IsSubscriber => TargetKind::Bool,
            FollowAgeDays | SubTier => TargetKind::Int,
        }
    }

    pub fn as_str(self) -> &'static str {
        use ConditionTarget::*;
        match self {
            IsFollower => "is_follower",
            FollowAgeDays => "follow_age_days",
            IsSubscriber => "is_subscriber",
            SubTier => "sub_tier",
        }
    }

    pub fn from_key(s: &str) -> Option<Self> {
        use ConditionTarget::*;
        Some(match s {
            "is_follower" => IsFollower,
            "follow_age_days" => FollowAgeDays,
            "is_subscriber" => IsSubscriber,
            "sub_tier" => SubTier,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOperator {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
}

impl ConditionOperator {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Neq => "neq",
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Lt => "lt",
            Self::Lte => "lte",
            Self::Between => "between",
        }
    }

    pub fn from_key(s: &str) -> Option<Self> {
        Some(match s {
            "eq" => Self::Eq,
            "neq" => Self::Neq,
            "gt" => Self::Gt,
            "gte" => Self::Gte,
            "lt" => Self::Lt,
            "lte" => Self::Lte,
            "between" => Self::Between,
            _ => return None,
        })
    }

    /// Operators that produce a meaningful predicate on each target kind.
    /// Save-time validation rejects mismatches.
    pub fn valid_for(self, kind: TargetKind) -> bool {
        use ConditionOperator::*;
        match kind {
            TargetKind::Bool => matches!(self, Eq),
            TargetKind::Int => matches!(self, Eq | Neq | Gt | Gte | Lt | Lte | Between),
        }
    }
}

/// A single condition row inside an AND-group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub target: ConditionTarget,
    pub operator: ConditionOperator,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_end: Option<Value>,
}
