//! SQL WHERE-clause builder for bulk per-role-link sync.
//!
//! Pushes the same DNF semantics as [services::condition_eval::evaluate] down
//! into Postgres so `sync_for_role_link` filters server-side instead of
//! loading every viewer's facts into memory.
//!
//! The clause references one alias the caller must provide:
//!   * `ucc` — user_channel_cache (LEFT JOINed; columns may be NULL)
//!
//! NULL-handling matches the Rust evaluator's fail-closed behavior: a viewer
//! with no `user_channel_cache` row for this broadcaster (COALESCEd to
//! false / 0, or NULL for follow-age) is treated identically to one whose
//! facts are all default.

use crate::models::condition::{Condition, ConditionOperator, ConditionTarget};
use crate::models::rule::RuleTree;

#[derive(Debug, Clone)]
pub enum Bind {
    Bool(bool),
    Int(i64),
}

/// Returns ("clause", binds). Binds use parameter indices starting at
/// `bind_offset + 1`. Unconfigured (`grant_on_any_relation = false` AND no
/// groups) ⇒ "FALSE" (match nobody). `grant_on_any_relation = true` ⇒ "TRUE".
pub fn build_rule_where(tree: &RuleTree, bind_offset: usize) -> (String, Vec<Bind>) {
    if tree.grant_on_any_relation {
        return ("TRUE".to_string(), vec![]);
    }
    if tree.groups.is_empty() {
        return ("FALSE".to_string(), vec![]);
    }

    let mut binds: Vec<Bind> = Vec::new();
    let mut group_clauses: Vec<String> = Vec::new();

    for group in &tree.groups {
        if group.conditions.is_empty() {
            group_clauses.push("FALSE".to_string());
            continue;
        }
        let mut cond_clauses: Vec<String> = Vec::new();
        for c in &group.conditions {
            cond_clauses.push(build_condition(c, bind_offset, &mut binds));
        }
        group_clauses.push(format!("({})", cond_clauses.join(" AND ")));
    }

    (format!("({})", group_clauses.join(" OR ")), binds)
}

/// SQL expression for a target. Bools COALESCE to false and the tier divides
/// a COALESCEd-0 column, so they're never NULL; the follow-age expression
/// stays NULL-able so comparisons fail closed when `followed_at` is unset.
fn target_expr(target: ConditionTarget) -> &'static str {
    use ConditionTarget::*;
    match target {
        IsFollower => "COALESCE(ucc.is_following, false)",
        FollowAgeDays => "FLOOR(EXTRACT(EPOCH FROM (now() - ucc.followed_at)) / 86400)",
        IsSubscriber => "COALESCE(ucc.is_subscribed, false)",
        SubTier => "(COALESCE(ucc.sub_tier, 0) / 1000)",
    }
}

fn build_condition(c: &Condition, bind_offset: usize, binds: &mut Vec<Bind>) -> String {
    use ConditionOperator::*;
    let expr = target_expr(c.target);
    let next = |binds: &Vec<Bind>| bind_offset + binds.len() + 1;

    match c.operator {
        Eq => {
            if let Some(b) = c.value.as_bool() {
                let i = next(binds);
                binds.push(Bind::Bool(b));
                format!("{expr} = ${i}")
            } else {
                let n = c.value.as_i64().unwrap_or(0);
                let i = next(binds);
                binds.push(Bind::Int(n));
                format!("({expr}) = ${i}")
            }
        }
        Neq => {
            let n = c.value.as_i64().unwrap_or(0);
            let i = next(binds);
            binds.push(Bind::Int(n));
            // Plain <> so a NULL int (missing follow-age) is NOT matched —
            // matches the Rust evaluator's fail-closed int behavior.
            format!("({expr}) <> ${i}")
        }
        Gt | Gte | Lt | Lte => {
            let n = c.value.as_i64().unwrap_or(0);
            let i = next(binds);
            binds.push(Bind::Int(n));
            let op = match c.operator {
                Gt => ">",
                Gte => ">=",
                Lt => "<",
                Lte => "<=",
                _ => unreachable!(),
            };
            format!("({expr}) {op} ${i}")
        }
        Between => {
            let lo = c.value.as_i64().unwrap_or(0);
            let hi = c.value_end.as_ref().and_then(|v| v.as_i64()).unwrap_or(lo);
            let ia = next(binds);
            binds.push(Bind::Int(lo));
            let ib = next(binds);
            binds.push(Bind::Int(hi));
            format!("(({expr}) >= ${ia} AND ({expr}) <= ${ib})")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::condition::{Condition, ConditionOperator as Op, ConditionTarget as T};
    use crate::models::rule::{ConditionGroup, RuleTree};
    use serde_json::json;

    fn cond(t: T, op: Op, v: serde_json::Value) -> Condition {
        Condition {
            target: t,
            operator: op,
            value: v,
            value_end: None,
        }
    }

    #[test]
    fn grant_on_any_is_true() {
        let t = RuleTree {
            grant_on_any_relation: true,
            groups: vec![],
        };
        let (sql, binds) = build_rule_where(&t, 2);
        assert_eq!(sql, "TRUE");
        assert!(binds.is_empty());
    }

    #[test]
    fn unconfigured_empty_is_false() {
        let t = RuleTree::default();
        let (sql, _) = build_rule_where(&t, 2);
        assert_eq!(sql, "FALSE");
    }

    #[test]
    fn single_group_ands() {
        let t = RuleTree {
            grant_on_any_relation: false,
            groups: vec![ConditionGroup {
                conditions: vec![
                    cond(T::IsSubscriber, Op::Eq, json!(true)),
                    cond(T::SubTier, Op::Gte, json!(2)),
                ],
            }],
        };
        let (sql, binds) = build_rule_where(&t, 2);
        assert!(sql.contains(" AND "));
        assert!(sql.contains("COALESCE(ucc.is_subscribed, false) = $3"));
        assert!(sql.contains(">= $4"));
        assert_eq!(binds.len(), 2);
        assert!(matches!(binds[0], Bind::Bool(true)));
        assert!(matches!(binds[1], Bind::Int(2)));
    }

    #[test]
    fn multi_group_ors() {
        let t = RuleTree {
            grant_on_any_relation: false,
            groups: vec![
                ConditionGroup {
                    conditions: vec![cond(T::IsSubscriber, Op::Eq, json!(true))],
                },
                ConditionGroup {
                    conditions: vec![cond(T::IsFollower, Op::Eq, json!(true))],
                },
            ],
        };
        let (sql, binds) = build_rule_where(&t, 2);
        assert!(sql.contains(" OR "));
        assert_eq!(binds.len(), 2);
    }

    #[test]
    fn between_emits_two_binds() {
        let mut c = cond(T::FollowAgeDays, Op::Between, json!(30));
        c.value_end = Some(json!(90));
        let t = RuleTree {
            grant_on_any_relation: false,
            groups: vec![ConditionGroup {
                conditions: vec![c],
            }],
        };
        let (sql, binds) = build_rule_where(&t, 0);
        assert!(sql.contains(">= $1") && sql.contains("<= $2"));
        assert_eq!(binds.len(), 2);
    }
}
