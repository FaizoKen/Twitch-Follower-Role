use serde_json::{json, Value};
use std::collections::HashMap;

use crate::error::AppError;
use crate::models::condition::TwitchConditions;

pub fn build_config_schema(
    conditions: &TwitchConditions,
    broadcaster_info: Option<(&str, &str)>,
    verify_url: &str,
    connect_url: &str,
) -> Value {
    let connection_text = match broadcaster_info {
        Some((id, login)) => format!(
            "Connected as: {login} (ID: {id})\n\
             To reconnect with a different account, visit:\n\
             {connect_url}"
        ),
        None => format!(
            "No Twitch channel connected.\n\
             The channel owner must authorize this plugin:\n\
             {connect_url}"
        ),
    };

    json!({
        "version": 1,
        "name": "Twitch Follower Role",
        "description": "Assign Discord roles based on Twitch channel follow/sub status.",
        "sections": [
            {
                "title": "Channel Connection",
                "fields": [
                    {
                        "type": "display",
                        "key": "connection_status",
                        "label": "Broadcaster Account",
                        "value": connection_text
                    }
                ]
            },
            {
                "title": "User Verification",
                "fields": [
                    {
                        "type": "display",
                        "key": "verify_info",
                        "label": "How members link accounts",
                        "value": format!(
                            "Members link their Discord and Twitch accounts at:\n\
                             {verify_url}\n\
                             \n\
                             Once linked, their Twitch follow/subscription status is checked \
                             automatically and roles are updated in real-time."
                        )
                    }
                ]
            },
            {
                "title": "Conditions",
                "description": "Set the requirements for earning this role. All enabled conditions must be met (AND logic).",
                "fields": [
                    {
                        "type": "toggle",
                        "key": "require_follower",
                        "label": "Require Follower",
                        "description": "User must be following the channel."
                    },
                    {
                        "type": "number",
                        "key": "min_follow_days",
                        "label": "Minimum Follow Days",
                        "description": "Minimum number of days the user must have been following. 0 means just following is enough.",
                        "validation": { "min": 0 },
                        "condition": { "field": "require_follower", "equals": true }
                    },
                    {
                        "type": "toggle",
                        "key": "require_subscriber",
                        "label": "Require Subscriber",
                        "description": "User must be subscribed to the channel."
                    },
                    {
                        "type": "select",
                        "key": "min_sub_tier",
                        "label": "Minimum Sub Tier",
                        "description": "Minimum subscription tier required.",
                        "options": [
                            {"label": "Tier 1 ($4.99)", "value": "1"},
                            {"label": "Tier 2 ($9.99)", "value": "2"},
                            {"label": "Tier 3 ($24.99)", "value": "3"}
                        ],
                        "condition": { "field": "require_subscriber", "equals": true }
                    }
                ]
            },
            {
                "title": "Examples",
                "collapsible": true,
                "default_collapsed": true,
                "fields": [
                    {
                        "type": "display",
                        "key": "examples",
                        "label": "Common setups",
                        "value": "Follower only  \u{2192}  Anyone following the channel gets the role\n\
                                  Follower + 30 days  \u{2192}  Must have followed for at least 30 days\n\
                                  Subscriber (any tier)  \u{2192}  Any active subscriber\n\
                                  Subscriber Tier 2+  \u{2192}  Only Tier 2 and Tier 3 subscribers\n\
                                  Follower + Subscriber  \u{2192}  Must be both following AND subscribed"
                    }
                ]
            }
        ],
        "values": {
            "require_follower": conditions.require_follower,
            "min_follow_days": conditions.min_follow_days,
            "require_subscriber": conditions.require_subscriber,
            "min_sub_tier": format!("{}", conditions.min_sub_tier)
        }
    })
}

pub fn parse_config(config: &HashMap<String, Value>) -> Result<TwitchConditions, AppError> {
    let require_follower = config
        .get("require_follower")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let min_follow_days = if require_follower {
        config
            .get("min_follow_days")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    } else {
        0
    };

    if min_follow_days < 0 {
        return Err(AppError::BadRequest(
            "Minimum follow days must be 0 or greater".into(),
        ));
    }

    let require_subscriber = config
        .get("require_subscriber")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let min_sub_tier = if require_subscriber {
        let raw = config
            .get("min_sub_tier")
            .and_then(|v| v.as_str().or_else(|| v.as_i64().map(|_| "")).and_then(|s| {
                if s.is_empty() {
                    v.as_i64().map(|n| n as i32)
                } else {
                    s.parse::<i32>().ok()
                }
            }))
            .unwrap_or(1);
        if !(1..=3).contains(&raw) {
            return Err(AppError::BadRequest(
                "Minimum sub tier must be 1, 2, or 3".into(),
            ));
        }
        raw
    } else {
        1
    };

    if !require_follower && !require_subscriber {
        return Err(AppError::BadRequest(
            "At least one condition (follower or subscriber) must be enabled".into(),
        ));
    }

    Ok(TwitchConditions {
        require_follower,
        min_follow_days,
        require_subscriber,
        min_sub_tier,
    })
}
