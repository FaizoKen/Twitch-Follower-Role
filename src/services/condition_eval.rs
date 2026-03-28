use chrono::{DateTime, Utc};

use crate::models::condition::TwitchConditions;

pub struct CacheData {
    pub is_following: bool,
    pub followed_at: Option<DateTime<Utc>>,
    pub is_subscribed: bool,
    pub sub_tier: i32,
}

/// Evaluate whether a user meets the configured conditions.
/// This function is synchronous, pure, and fast (no I/O).
pub fn evaluate(conditions: &TwitchConditions, cache: &CacheData) -> bool {
    if conditions.require_follower {
        if !cache.is_following {
            return false;
        }
        if conditions.min_follow_days > 0 {
            let followed = cache.followed_at.unwrap_or_else(Utc::now);
            let days = (Utc::now() - followed).num_days();
            if days < conditions.min_follow_days {
                return false;
            }
        }
    }
    if conditions.require_subscriber {
        if !cache.is_subscribed {
            return false;
        }
        if conditions.min_sub_tier > 0 {
            let tier = cache.sub_tier / 1000; // 1000->1, 2000->2, 3000->3
            if tier < conditions.min_sub_tier {
                return false;
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_cache() -> CacheData {
        CacheData {
            is_following: true,
            followed_at: Some(Utc::now() - chrono::Duration::days(60)),
            is_subscribed: true,
            sub_tier: 2000,
        }
    }

    #[test]
    fn empty_conditions_always_true() {
        let conditions = TwitchConditions::default();
        let cache = default_cache();
        assert!(evaluate(&conditions, &cache));
    }

    #[test]
    fn require_follower_passes() {
        let conditions = TwitchConditions {
            require_follower: true,
            ..Default::default()
        };
        let cache = default_cache();
        assert!(evaluate(&conditions, &cache));
    }

    #[test]
    fn require_follower_fails_when_not_following() {
        let conditions = TwitchConditions {
            require_follower: true,
            ..Default::default()
        };
        let cache = CacheData {
            is_following: false,
            ..default_cache()
        };
        assert!(!evaluate(&conditions, &cache));
    }

    #[test]
    fn min_follow_days_passes() {
        let conditions = TwitchConditions {
            require_follower: true,
            min_follow_days: 30,
            ..Default::default()
        };
        let cache = default_cache(); // followed 60 days ago
        assert!(evaluate(&conditions, &cache));
    }

    #[test]
    fn min_follow_days_fails_too_recent() {
        let conditions = TwitchConditions {
            require_follower: true,
            min_follow_days: 90,
            ..Default::default()
        };
        let cache = default_cache(); // followed 60 days ago
        assert!(!evaluate(&conditions, &cache));
    }

    #[test]
    fn require_subscriber_passes() {
        let conditions = TwitchConditions {
            require_subscriber: true,
            min_sub_tier: 1,
            ..Default::default()
        };
        let cache = default_cache(); // tier 2000
        assert!(evaluate(&conditions, &cache));
    }

    #[test]
    fn require_subscriber_fails_when_not_subscribed() {
        let conditions = TwitchConditions {
            require_subscriber: true,
            min_sub_tier: 1,
            ..Default::default()
        };
        let cache = CacheData {
            is_subscribed: false,
            sub_tier: 0,
            ..default_cache()
        };
        assert!(!evaluate(&conditions, &cache));
    }

    #[test]
    fn min_sub_tier_fails_too_low() {
        let conditions = TwitchConditions {
            require_subscriber: true,
            min_sub_tier: 3,
            ..Default::default()
        };
        let cache = default_cache(); // tier 2000 (tier 2)
        assert!(!evaluate(&conditions, &cache));
    }

    #[test]
    fn combined_conditions() {
        let conditions = TwitchConditions {
            require_follower: true,
            min_follow_days: 30,
            require_subscriber: true,
            min_sub_tier: 1,
        };
        let cache = default_cache();
        assert!(evaluate(&conditions, &cache));
    }
}
