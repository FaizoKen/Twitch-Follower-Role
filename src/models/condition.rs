use serde::{Deserialize, Serialize};

fn default_tier() -> i32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TwitchConditions {
    #[serde(default)]
    pub require_follower: bool,
    #[serde(default)]
    pub min_follow_days: i64,
    #[serde(default)]
    pub require_subscriber: bool,
    #[serde(default = "default_tier")]
    pub min_sub_tier: i32,
}
