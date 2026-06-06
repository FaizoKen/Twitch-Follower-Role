use governor::{Quota, RateLimiter};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::num::NonZeroU32;
use std::sync::Arc;

use crate::error::AppError;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, serde::Deserialize)]
pub struct TwitchUser {
    pub id: String,
    pub login: String,
    pub display_name: String,
}

#[derive(Debug)]
pub struct FollowerInfo {
    pub followed_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
pub struct SubInfo {
    pub tier: i32,
}

#[derive(Debug, serde::Deserialize)]
struct TwitchTokenResponse {
    access_token: String,
    refresh_token: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct HelixResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, serde::Deserialize)]
struct FollowerData {
    followed_at: String,
}

#[derive(Debug, serde::Deserialize)]
struct SubData {
    tier: String,
}

#[derive(Debug, serde::Deserialize)]
struct EventSubCreateResponse {
    data: Vec<EventSubSubscription>,
}

#[derive(Debug, serde::Deserialize)]
pub struct EventSubSubscription {
    pub id: String,
    pub status: String,
}

pub struct TwitchClient {
    http: reqwest::Client,
    pub client_id: String,
    client_secret: String,
    pub eventsub_secret: String,
    rate_limiter: Arc<
        RateLimiter<
            governor::state::NotKeyed,
            governor::state::InMemoryState,
            governor::clock::DefaultClock,
        >,
    >,
}

impl TwitchClient {
    pub fn new(client_id: &str, client_secret: &str, eventsub_secret: &str) -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("Failed to build HTTP client");

        // ~8 requests per second — still well within Twitch's 800 points/min,
        // with headroom so a verify spike (each linker triggers an inline
        // follow + sub check) drains quickly instead of serializing.
        let quota = Quota::per_second(NonZeroU32::new(8).unwrap());
        let rate_limiter = Arc::new(RateLimiter::direct(quota));

        Self {
            http,
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            eventsub_secret: eventsub_secret.to_string(),
            rate_limiter,
        }
    }

    pub async fn wait_for_permit(&self) {
        self.rate_limiter.until_ready().await;
    }

    // --- OAuth methods ---

    /// Get an app access token via client credentials grant.
    pub async fn get_app_access_token(&self) -> Result<String, AppError> {
        let resp: TwitchTokenResponse = self
            .http
            .post("https://id.twitch.tv/oauth2/token")
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("App token request failed: {e}")))?
            .json()
            .await
            .map_err(|e| AppError::TwitchApi(format!("App token parse failed: {e}")))?;

        Ok(resp.access_token)
    }

    /// Exchange an OAuth authorization code for tokens.
    pub async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
    ) -> Result<(String, Option<String>), AppError> {
        let resp: TwitchTokenResponse = self
            .http
            .post("https://id.twitch.tv/oauth2/token")
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("code", code),
                ("grant_type", "authorization_code"),
                ("redirect_uri", redirect_uri),
            ])
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Token exchange failed: {e}")))?
            .json()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Token exchange parse failed: {e}")))?;

        Ok((resp.access_token, resp.refresh_token))
    }

    /// Refresh a user/broadcaster token. Returns (new_access_token, new_refresh_token).
    pub async fn refresh_token(
        &self,
        refresh_token: &str,
    ) -> Result<(String, String), AppError> {
        let resp: TwitchTokenResponse = self
            .http
            .post("https://id.twitch.tv/oauth2/token")
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("grant_type", "refresh_token"),
                ("refresh_token", refresh_token),
            ])
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Token refresh failed: {e}")))?
            .json()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Token refresh parse failed: {e}")))?;

        let new_refresh = resp
            .refresh_token
            .ok_or_else(|| AppError::TwitchApi("Token refresh returned no refresh_token".into()))?;

        Ok((resp.access_token, new_refresh))
    }

    // --- Helix API methods ---

    /// Get the authenticated user's info. Returns TwitchUser.
    pub async fn get_user_by_token(&self, access_token: &str) -> Result<TwitchUser, AppError> {
        let resp: HelixResponse<TwitchUser> = self
            .http
            .get("https://api.twitch.tv/helix/users")
            .header("Authorization", format!("Bearer {access_token}"))
            .header("Client-Id", &self.client_id)
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Get user failed: {e}")))?
            .json()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Get user parse failed: {e}")))?;

        resp.data
            .into_iter()
            .next()
            .ok_or_else(|| AppError::TwitchApi("No user data returned".into()))
    }

    /// Check if a user follows a broadcaster. Returns Some(FollowerInfo) if following, None if not.
    /// Requires broadcaster token with moderator:read:followers scope.
    pub async fn check_follower(
        &self,
        broadcaster_id: &str,
        user_id: &str,
        broadcaster_token: &str,
    ) -> Result<Option<FollowerInfo>, AppError> {
        let resp = self
            .http
            .get("https://api.twitch.tv/helix/channels/followers")
            .query(&[
                ("broadcaster_id", broadcaster_id),
                ("user_id", user_id),
            ])
            .header("Authorization", format!("Bearer {broadcaster_token}"))
            .header("Client-Id", &self.client_id)
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Check follower failed: {e}")))?;

        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::TwitchApi(format!(
                "401 Unauthorized checking follower: {body}"
            )));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::TwitchApi(format!(
                "Check follower failed: {status} - {body}"
            )));
        }

        let data: HelixResponse<FollowerData> = resp
            .json()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Check follower parse failed: {e}")))?;

        match data.data.into_iter().next() {
            Some(follower) => {
                let followed_at = chrono::DateTime::parse_from_rfc3339(&follower.followed_at)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .map_err(|e| {
                        AppError::TwitchApi(format!("Failed to parse followed_at: {e}"))
                    })?;
                Ok(Some(FollowerInfo { followed_at }))
            }
            None => Ok(None),
        }
    }

    /// Check if a user is subscribed to a broadcaster. Returns Some(SubInfo) if subscribed, None if not.
    /// Requires broadcaster token with channel:read:subscriptions scope.
    /// Uses GET /helix/subscriptions (broadcaster endpoint), not /subscriptions/user (user endpoint).
    pub async fn check_subscription(
        &self,
        broadcaster_id: &str,
        user_id: &str,
        broadcaster_token: &str,
    ) -> Result<Option<SubInfo>, AppError> {
        let resp = self
            .http
            .get("https://api.twitch.tv/helix/subscriptions")
            .query(&[
                ("broadcaster_id", broadcaster_id),
                ("user_id", user_id),
            ])
            .header("Authorization", format!("Bearer {broadcaster_token}"))
            .header("Client-Id", &self.client_id)
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Check subscription failed: {e}")))?;

        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::TwitchApi(format!(
                "401 Unauthorized checking subscription: {body}"
            )));
        }
        // 404 means not subscribed
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::TwitchApi(format!(
                "Check subscription failed: {status} - {body}"
            )));
        }

        let data: HelixResponse<SubData> = resp
            .json()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Check subscription parse failed: {e}")))?;

        match data.data.into_iter().next() {
            Some(sub) => {
                let tier: i32 = sub.tier.parse().unwrap_or(0);
                Ok(Some(SubInfo { tier }))
            }
            None => Ok(None),
        }
    }

    // --- EventSub methods ---

    /// Create an EventSub webhook subscription.
    pub async fn create_eventsub_subscription(
        &self,
        event_type: &str,
        version: &str,
        condition: serde_json::Value,
        callback_url: &str,
        app_token: &str,
    ) -> Result<EventSubSubscription, AppError> {
        let body = serde_json::json!({
            "type": event_type,
            "version": version,
            "condition": condition,
            "transport": {
                "method": "webhook",
                "callback": callback_url,
                "secret": self.eventsub_secret
            }
        });

        let resp = self
            .http
            .post("https://api.twitch.tv/helix/eventsub/subscriptions")
            .header("Authorization", format!("Bearer {app_token}"))
            .header("Client-Id", &self.client_id)
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Create EventSub subscription failed: {e}")))?;

        let status = resp.status();
        if !status.is_success() && status != reqwest::StatusCode::ACCEPTED {
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::TwitchApi(format!(
                "Create EventSub subscription failed: {status} - {body}"
            )));
        }

        let data: EventSubCreateResponse = resp
            .json()
            .await
            .map_err(|e| AppError::TwitchApi(format!("EventSub response parse failed: {e}")))?;

        data.data
            .into_iter()
            .next()
            .ok_or_else(|| AppError::TwitchApi("No subscription data returned".into()))
    }

    /// Delete an EventSub subscription by ID.
    pub async fn delete_eventsub_subscription(
        &self,
        subscription_id: &str,
        app_token: &str,
    ) -> Result<(), AppError> {
        let resp = self
            .http
            .delete("https://api.twitch.tv/helix/eventsub/subscriptions")
            .query(&[("id", subscription_id)])
            .header("Authorization", format!("Bearer {app_token}"))
            .header("Client-Id", &self.client_id)
            .send()
            .await
            .map_err(|e| AppError::TwitchApi(format!("Delete EventSub subscription failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::warn!("Failed to delete EventSub subscription {subscription_id}: {status} - {body}");
        }

        Ok(())
    }

    // --- EventSub signature verification ---

    /// Verify the HMAC signature of an incoming EventSub webhook request.
    /// Returns true if the signature is valid and the timestamp is within 10 minutes.
    pub fn verify_eventsub_signature(
        message_id: &str,
        timestamp: &str,
        body: &[u8],
        secret: &str,
        signature_header: &str,
    ) -> bool {
        // Check timestamp is within 10 minutes
        if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(timestamp) {
            let age = chrono::Utc::now().signed_duration_since(ts);
            if age.num_minutes().abs() > 10 {
                return false;
            }
        } else {
            return false;
        }

        // Construct HMAC message: message_id + timestamp + body
        let expected_prefix = "sha256=";
        let sig_hex = match signature_header.strip_prefix(expected_prefix) {
            Some(hex) => hex,
            None => return false,
        };

        let mut mac =
            HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
        mac.update(message_id.as_bytes());
        mac.update(timestamp.as_bytes());
        mac.update(body);

        let computed = hex::encode(mac.finalize().into_bytes());

        // Constant-time comparison
        computed.len() == sig_hex.len()
            && computed
                .as_bytes()
                .iter()
                .zip(sig_hex.as_bytes())
                .all(|(a, b)| a == b)
    }
}
