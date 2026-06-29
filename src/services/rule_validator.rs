//! Parse and validate the rule-tree payload sent by the iframe UI on save.
//!
//! Returns a clean `RuleTree` ready to persist as `role_links.rule_tree`
//! JSONB. The channel a relation rule checks against is the broadcaster
//! connected to the role link (set via OAuth), not part of this payload.

use serde::Deserialize;
use serde_json::Value;

use crate::error::AppError;
use crate::models::condition::{Condition, ConditionOperator, ConditionTarget, TargetKind};
use crate::models::rule::{ConditionGroup, RuleTree, MAX_CONDITIONS_PER_GROUP, MAX_GROUPS};

#[derive(Debug, Deserialize)]
pub struct RuleTreeBody {
    #[serde(default)]
    pub grant_on_any_relation: bool,
    #[serde(default)]
    pub groups: Vec<ConditionGroupInput>,
}

#[derive(Debug, Deserialize)]
pub struct ConditionGroupInput {
    #[serde(default)]
    pub conditions: Vec<ConditionInput>,
}

#[derive(Debug, Deserialize)]
pub struct ConditionInput {
    pub target: String,
    pub operator: String,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub value_end: Option<Value>,
}

pub struct ParsedRule {
    pub rule_tree: RuleTree,
}

pub fn parse_rule_tree(body: RuleTreeBody) -> Result<ParsedRule, AppError> {
    if !body.grant_on_any_relation {
        if body.groups.is_empty() {
            return Err(AppError::BadRequest(
                "Add at least one rule group, or pick \"anyone who linked their Twitch\".".into(),
            ));
        }
        if body.groups.len() > MAX_GROUPS {
            return Err(AppError::BadRequest(format!(
                "At most {MAX_GROUPS} OR-groups per role."
            )));
        }
    }

    let mut groups: Vec<ConditionGroup> = Vec::with_capacity(body.groups.len());
    if !body.grant_on_any_relation {
        for (gi, raw_group) in body.groups.into_iter().enumerate() {
            let group_num = gi + 1;
            if raw_group.conditions.is_empty() {
                return Err(AppError::BadRequest(format!(
                    "Group #{group_num}: add at least one condition (or remove the group)."
                )));
            }
            if raw_group.conditions.len() > MAX_CONDITIONS_PER_GROUP {
                return Err(AppError::BadRequest(format!(
                    "Group #{group_num}: at most {MAX_CONDITIONS_PER_GROUP} conditions per group."
                )));
            }
            let mut conditions: Vec<Condition> = Vec::with_capacity(raw_group.conditions.len());
            for (ci, raw) in raw_group.conditions.into_iter().enumerate() {
                conditions.push(validate_condition(group_num, ci + 1, raw)?);
            }
            groups.push(ConditionGroup { conditions });
        }
    }

    Ok(ParsedRule {
        rule_tree: RuleTree {
            grant_on_any_relation: body.grant_on_any_relation,
            groups,
        },
    })
}

fn validate_condition(
    group_num: usize,
    cond_num: usize,
    raw: ConditionInput,
) -> Result<Condition, AppError> {
    let where_ = format!("Group #{group_num}, condition #{cond_num}");

    let target = ConditionTarget::from_key(raw.target.trim()).ok_or_else(|| {
        AppError::BadRequest(format!("{where_}: unknown target '{}'.", raw.target))
    })?;

    let operator = ConditionOperator::from_key(raw.operator.trim()).ok_or_else(|| {
        AppError::BadRequest(format!("{where_}: unknown operator '{}'.", raw.operator))
    })?;

    if !operator.valid_for(target.kind()) {
        return Err(AppError::BadRequest(format!(
            "{where_}: operator '{}' is not valid for '{}'.",
            operator.as_str(),
            target.as_str()
        )));
    }

    let value = normalize_value(&where_, target, operator, raw.value)?;
    let value_end = match (operator, raw.value_end) {
        (ConditionOperator::Between, Some(end)) => {
            Some(normalize_value(&where_, target, operator, end)?)
        }
        (ConditionOperator::Between, None) => {
            return Err(AppError::BadRequest(format!(
                "{where_}: \"between\" needs both a min and a max value."
            )));
        }
        _ => None,
    };

    Ok(Condition {
        target,
        operator,
        value,
        value_end,
    })
}

fn normalize_value(
    where_: &str,
    target: ConditionTarget,
    _op: ConditionOperator,
    raw: Value,
) -> Result<Value, AppError> {
    match target.kind() {
        TargetKind::Bool => match &raw {
            Value::Bool(_) => Ok(raw),
            Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(AppError::BadRequest(format!(
                    "{where_}: boolean value required (got {raw})."
                ))),
            },
            _ => Err(AppError::BadRequest(format!(
                "{where_}: boolean value required (got {raw})."
            ))),
        },
        TargetKind::Int => {
            let n = match &raw {
                Value::Number(num) => num.as_i64().or_else(|| num.as_f64().map(|f| f as i64)),
                Value::String(s) => s.trim().parse::<i64>().ok(),
                _ => None,
            };
            let n = n.ok_or_else(|| {
                AppError::BadRequest(format!(
                    "{where_}: whole-number value required (got {raw})."
                ))
            })?;
            // Twitch sub-tier is 1..=3 on the friendly scale; follow-age can't
            // be negative. Reject obvious nonsense early so a typo surfaces as
            // a clear error instead of a rule that silently matches nobody.
            if matches!(target, ConditionTarget::SubTier) && !(1..=3).contains(&n) {
                return Err(AppError::BadRequest(format!(
                    "{where_}: sub tier must be 1, 2, or 3."
                )));
            }
            if matches!(target, ConditionTarget::FollowAgeDays) && n < 0 {
                return Err(AppError::BadRequest(format!(
                    "{where_}: days must be 0 or greater."
                )));
            }
            Ok(Value::from(n))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input(target: &str, operator: &str, value: Value) -> ConditionInput {
        ConditionInput {
            target: target.into(),
            operator: operator.into(),
            value,
            value_end: None,
        }
    }

    fn one_group(conds: Vec<ConditionInput>) -> RuleTreeBody {
        RuleTreeBody {
            grant_on_any_relation: false,
            groups: vec![ConditionGroupInput { conditions: conds }],
        }
    }

    #[test]
    fn grant_on_any_no_groups_ok() {
        let body = RuleTreeBody {
            grant_on_any_relation: true,
            groups: vec![],
        };
        let parsed = parse_rule_tree(body).unwrap();
        assert!(parsed.rule_tree.grant_on_any_relation);
        assert!(parsed.rule_tree.groups.is_empty());
    }

    #[test]
    fn rejects_no_groups_no_grant() {
        let body = RuleTreeBody {
            grant_on_any_relation: false,
            groups: vec![],
        };
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_unknown_target() {
        let body = one_group(vec![input("not_a_target", "eq", json!(true))]);
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_operator_target_mismatch() {
        // gt against a bool is nonsense
        let body = one_group(vec![input("is_follower", "gt", json!(0))]);
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_empty_group() {
        let body = RuleTreeBody {
            grant_on_any_relation: false,
            groups: vec![ConditionGroupInput { conditions: vec![] }],
        };
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn bool_coerces_from_string() {
        let body = one_group(vec![input("is_subscriber", "eq", json!("true"))]);
        let parsed = parse_rule_tree(body).unwrap();
        assert_eq!(
            parsed.rule_tree.groups[0].conditions[0].value,
            Value::Bool(true)
        );
    }

    #[test]
    fn int_coerces_from_string() {
        let body = one_group(vec![input("follow_age_days", "gte", json!("30"))]);
        let parsed = parse_rule_tree(body).unwrap();
        assert_eq!(parsed.rule_tree.groups[0].conditions[0].value, json!(30));
    }

    #[test]
    fn sub_tier_out_of_range_rejected() {
        let body = one_group(vec![input("sub_tier", "gte", json!(4))]);
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn negative_follow_age_rejected() {
        let body = one_group(vec![input("follow_age_days", "gte", json!(-5))]);
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn between_requires_value_end() {
        let body = RuleTreeBody {
            grant_on_any_relation: false,
            groups: vec![ConditionGroupInput {
                conditions: vec![ConditionInput {
                    target: "follow_age_days".into(),
                    operator: "between".into(),
                    value: json!(30),
                    value_end: None,
                }],
            }],
        };
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }

    #[test]
    fn between_with_value_end_ok() {
        let body = RuleTreeBody {
            grant_on_any_relation: false,
            groups: vec![ConditionGroupInput {
                conditions: vec![ConditionInput {
                    target: "follow_age_days".into(),
                    operator: "between".into(),
                    value: json!(30),
                    value_end: Some(json!(90)),
                }],
            }],
        };
        let parsed = parse_rule_tree(body).unwrap();
        assert_eq!(
            parsed.rule_tree.groups[0].conditions[0].value_end,
            Some(json!(90))
        );
    }

    #[test]
    fn caps_max_groups() {
        let mut groups = Vec::new();
        for _ in 0..(MAX_GROUPS + 1) {
            groups.push(ConditionGroupInput {
                conditions: vec![input("is_follower", "eq", json!(true))],
            });
        }
        let body = RuleTreeBody {
            grant_on_any_relation: false,
            groups,
        };
        assert!(matches!(
            parse_rule_tree(body),
            Err(AppError::BadRequest(_))
        ));
    }
}
