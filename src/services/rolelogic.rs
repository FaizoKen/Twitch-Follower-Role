use std::time::Duration;

use crate::error::AppError;

/// Hard cap for a single `PUT /users` request (server-side limit).
const PUT_MAX_USERS: usize = 100_000;
/// Per-chunk cap for the chunked upload flow (server-side limit).
const CHUNK_SIZE: usize = 100_000;
/// Per-request timeout for a single chunk POST. Chunks are short
/// transactions but the payload can be ~2 MB of JSON, so allow headroom.
const CHUNK_TIMEOUT: Duration = Duration::from_secs(120);
/// Per-request timeout for the commit POST. The server may take up to
/// 30 minutes to perform the atomic swap on very large staging sets.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// Body substring RoleLogic returns when our token isn't found server-side.
/// Because `RoleLinkToken` rows cascade on `RoleLink` delete, getting this
/// reliably signals the role link has been deleted upstream.
const RL_LINK_GONE_ERROR_MSG: &str = "Invalid or revoked token";
/// Body substring RoleLogic returns when the role link exists but its owner
/// toggled it off. NOT a deletion (that is `RL_LINK_GONE_ERROR_MSG`): leave the
/// link + config intact and skip syncing it this cycle instead of erroring and
/// retrying forever. Source: `role-link-token.guard.ts`.
const RL_LINK_DISABLED_ERROR_MSG: &str = "This role link is disabled";
/// Body substring for a link paused because its guild is over the
/// integration-slot quota. RoleLogic treats it exactly like a disabled link
/// (every call rejects until the owner frees a slot or upgrades), so it maps
/// to the same skip. Source: `role-link-token.guard.ts`.
const RL_LINK_PAUSED_ERROR_MSG: &str = "This role link is paused";
/// 409 body marker for a list write held by RoleLogic's shrink gate until an
/// owner confirms the removals. Source: `RoleLinkService.assertShrinkAllowed`.
const RL_CONFIRM_REQUIRED: &str = "confirm_required";

/// A 403 for a link that still exists but is switched off or paused.
fn link_inactive(status: reqwest::StatusCode, body: &str) -> bool {
    status == reqwest::StatusCode::FORBIDDEN
        && (body.contains(RL_LINK_DISABLED_ERROR_MSG) || body.contains(RL_LINK_PAUSED_ERROR_MSG))
}

/// A 409 for a list write the shrink gate is holding.
fn removal_held(status: reqwest::StatusCode, body: &str) -> bool {
    status == reqwest::StatusCode::CONFLICT && body.contains(RL_CONFIRM_REQUIRED)
}

/// Pause before each resend of an idempotent request; its length is the
/// number of resends.
const RESEND_BACKOFF: [Duration; 2] = [Duration::from_millis(500), Duration::from_secs(2)];

/// A send that failed before RoleLogic answered: the connection was refused
/// or reset, DNS or TLS failed. Prod 2026-11-02 lost a removal to one of
/// these ("error sending request for url"), and the request never reached
/// the API. A timeout is excluded: the request may still be running there,
/// and the 30 s already spent is the worst case to repeat.
fn send_failed_transiently(e: &reqwest::Error) -> bool {
    e.is_request() && !e.is_timeout()
}

/// An answer from the edge rather than from RoleLogic: Cloudflare or the
/// tunnel could not reach the API, or it was restarting. Besides 502–504 that
/// is Cloudflare's own 520–523 and 530 (prod Nov 2026: three adds and removes
/// in a month ended on a 520 the API never saw). 524 is left out for the
/// reason a timeout is: the API may still be running the request.
fn edge_unavailable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 502..=504 | 520..=523 | 530)
}

/// What happened to a notice sent to RoleLogic.
#[derive(Debug, PartialEq, Eq)]
pub enum NoticeOutcome {
    /// RoleLogic now shows (or no longer shows) the notice.
    Applied,
    /// This RoleLogic predates notices (the route answers 404).
    Unsupported,
    /// The link is switched off or paused; every call is refused until the
    /// owner turns it back on.
    Inactive,
    /// The link was deleted in RoleLogic.
    LinkGone,
}

/// Classify a notice call's answer. Anything not listed is an error.
fn notice_outcome(status: reqwest::StatusCode, body: &str) -> Option<NoticeOutcome> {
    if status.is_success() {
        return Some(NoticeOutcome::Applied);
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return Some(NoticeOutcome::Unsupported);
    }
    if status == reqwest::StatusCode::FORBIDDEN && body.contains(RL_LINK_GONE_ERROR_MSG) {
        return Some(NoticeOutcome::LinkGone);
    }
    if link_inactive(status, body) {
        return Some(NoticeOutcome::Inactive);
    }
    None
}

/// A request that got no usable answer. reqwest's own message stops at
/// "error sending request for url (…)"; what actually failed (DNS, connect,
/// reset, timeout, a body that is not JSON) is in its source chain.
fn request_failed(e: reqwest::Error) -> AppError {
    AppError::RoleLogic(with_causes(&e))
}

fn with_causes(e: &dyn std::error::Error) -> String {
    let mut msg = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        let text = c.to_string();
        // Some layers repeat the message of the error they wrap.
        if !msg.ends_with(&text) {
            msg.push_str(": ");
            msg.push_str(&text);
        }
        cause = c.source();
    }
    msg
}

#[derive(Clone)]
pub struct RoleLogicClient {
    http: reqwest::Client,
    base_url: String,
}

impl RoleLogicClient {
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            http,
            base_url: "https://api-rolelogic.faizo.net".to_string(),
        }
    }

    /// Send a request that is safe to repeat, resending it while the failure
    /// is the connection's or the edge's rather than RoleLogic's answer. The
    /// single-user writes qualify: adding a user already on the list answers
    /// `added: false`, removing one already gone answers `removed: false`.
    async fn send_idempotent(
        &self,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut resends = RESEND_BACKOFF.iter();
        loop {
            let result = build().send().await;
            let cause = match &result {
                Ok(resp) if edge_unavailable(resp.status()) => resp.status().to_string(),
                Err(e) if send_failed_transiently(e) => e.to_string(),
                _ => return result,
            };
            let Some(&wait) = resends.next() else {
                return result;
            };
            tracing::warn!(
                wait_ms = wait.as_millis() as u64,
                "RoleLogic request failed before an answer, resending: {cause}"
            );
            tokio::time::sleep(wait).await;
        }
    }

    /// Show a notice on this role link in the RoleLogic dashboard, replacing
    /// any earlier one. `code` is one RoleLogic knows (`reconnect_required`,
    /// `action_required`); `message` is shown under it as plain text.
    pub async fn set_notice(
        &self,
        guild_id: &str,
        role_id: &str,
        token: &str,
        code: &str,
        message: &str,
    ) -> Result<NoticeOutcome, AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/notice",
            self.base_url, guild_id, role_id
        );
        let body = serde_json::json!({ "code": code, "message": message });
        let resp = self
            .send_idempotent(|| {
                self.http
                    .put(&url)
                    .header("Authorization", format!("Token {token}"))
                    .json(&body)
            })
            .await
            .map_err(request_failed)?;
        Self::read_notice_answer(resp, "Set notice").await
    }

    /// Remove this role link's notice from the RoleLogic dashboard. Clearing
    /// a link that has none is not an error.
    pub async fn clear_notice(
        &self,
        guild_id: &str,
        role_id: &str,
        token: &str,
    ) -> Result<NoticeOutcome, AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/notice",
            self.base_url, guild_id, role_id
        );
        let resp = self
            .send_idempotent(|| {
                self.http
                    .delete(&url)
                    .header("Authorization", format!("Token {token}"))
            })
            .await
            .map_err(request_failed)?;
        Self::read_notice_answer(resp, "Clear notice").await
    }

    async fn read_notice_answer(
        resp: reqwest::Response,
        what: &str,
    ) -> Result<NoticeOutcome, AppError> {
        let status = resp.status();
        let body = if status.is_success() {
            String::new()
        } else {
            resp.text().await.unwrap_or_default()
        };
        notice_outcome(status, &body)
            .ok_or_else(|| AppError::RoleLogic(format!("{what} failed: {status} - {body}")))
    }

    /// Get user count and limit for a role link.
    pub async fn get_user_info(
        &self,
        guild_id: &str,
        role_id: &str,
        token: &str,
    ) -> Result<(usize, usize), AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users",
            self.base_url, guild_id, role_id
        );

        let resp = self
            .http
            .get(&url)
            .header("Authorization", format!("Token {token}"))
            .send()
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::FORBIDDEN && body.contains(RL_LINK_GONE_ERROR_MSG) {
                return Err(AppError::RoleLinkNotFound);
            }
            if link_inactive(status, &body) {
                return Err(AppError::RoleLinkDisabled);
            }
            return Err(AppError::RoleLogic(format!(
                "Get user info failed: {status} - {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(request_failed)?;

        let user_count = body["data"]["user_count"].as_u64().unwrap_or(0) as usize;
        let user_limit = body["data"]["user_limit"].as_u64().unwrap_or(100) as usize;

        Ok((user_count, user_limit))
    }

    pub async fn add_user(
        &self,
        guild_id: &str,
        role_id: &str,
        user_id: &str,
        token: &str,
    ) -> Result<bool, AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users/{}",
            self.base_url, guild_id, role_id, user_id
        );

        let resp = self
            .send_idempotent(|| {
                self.http
                    .post(&url)
                    .header("Authorization", format!("Token {token}"))
            })
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();

            if status == reqwest::StatusCode::FORBIDDEN && body.contains(RL_LINK_GONE_ERROR_MSG) {
                return Err(AppError::RoleLinkNotFound);
            }
            if link_inactive(status, &body) {
                return Err(AppError::RoleLinkDisabled);
            }

            if (status == reqwest::StatusCode::BAD_REQUEST
                || status == reqwest::StatusCode::FORBIDDEN)
                && body.contains("limit")
            {
                let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                let limit = parsed["data"]["user_limit"].as_u64().unwrap_or(100) as usize;
                return Err(AppError::UserLimitReached { limit });
            }

            return Err(AppError::RoleLogic(format!(
                "Add user failed: {status} - {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(request_failed)?;

        Ok(body["data"]["added"].as_bool().unwrap_or(false))
    }

    pub async fn remove_user(
        &self,
        guild_id: &str,
        role_id: &str,
        user_id: &str,
        token: &str,
    ) -> Result<bool, AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users/{}",
            self.base_url, guild_id, role_id, user_id
        );

        let resp = self
            .send_idempotent(|| {
                self.http
                    .delete(&url)
                    .header("Authorization", format!("Token {token}"))
            })
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::FORBIDDEN && body.contains(RL_LINK_GONE_ERROR_MSG) {
                return Err(AppError::RoleLinkNotFound);
            }
            if link_inactive(status, &body) {
                return Err(AppError::RoleLinkDisabled);
            }
            return Err(AppError::RoleLogic(format!(
                "Remove user failed: {status} - {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(request_failed)?;

        Ok(body["data"]["removed"].as_bool().unwrap_or(false))
    }

    /// Replace the full user set in a single atomic `PUT /users` request.
    /// Server rejects anything over `PUT_MAX_USERS`. For larger sets,
    /// callers should use [`upload_users`] which routes to the chunked flow.
    pub async fn replace_users(
        &self,
        guild_id: &str,
        role_id: &str,
        user_ids: &[String],
        token: &str,
    ) -> Result<usize, AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users",
            self.base_url, guild_id, role_id
        );

        let resp = self
            .http
            .put(&url)
            .header("Authorization", format!("Token {token}"))
            .json(user_ids)
            .send()
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::FORBIDDEN && body.contains(RL_LINK_GONE_ERROR_MSG) {
                return Err(AppError::RoleLinkNotFound);
            }
            if link_inactive(status, &body) {
                return Err(AppError::RoleLinkDisabled);
            }
            if removal_held(status, &body) {
                return Err(AppError::RemovalHeld);
            }
            return Err(AppError::RoleLogic(format!(
                "Replace users failed: {status} - {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(request_failed)?;

        Ok(body["data"]["user_count"].as_u64().unwrap_or(0) as usize)
    }

    /// High-level user-set upload. Picks the right transport for the size:
    /// - `len <= 100_000`: single atomic `PUT /users`.
    /// - `len > 100_000`: chunked flow (init → chunks → commit).
    pub async fn upload_users(
        &self,
        guild_id: &str,
        role_id: &str,
        user_ids: &[String],
        token: &str,
    ) -> Result<usize, AppError> {
        if user_ids.len() <= PUT_MAX_USERS {
            return self.replace_users(guild_id, role_id, user_ids, token).await;
        }

        let total = user_ids.len();
        tracing::info!(
            guild_id,
            role_id,
            total,
            "Bulk user set exceeds PUT cap; using chunked upload"
        );

        let upload_id = self.start_upload(guild_id, role_id, token).await?;
        let chunk_count = user_ids.chunks(CHUNK_SIZE).count();

        for (i, chunk) in user_ids.chunks(CHUNK_SIZE).enumerate() {
            if let Err(e) = self
                .upload_chunk(guild_id, role_id, &upload_id, chunk, token)
                .await
            {
                tracing::error!(
                    guild_id,
                    role_id,
                    upload_id,
                    chunk_idx = i,
                    chunk_count,
                    "Chunk upload failed; cancelling session: {e}"
                );
                if let Err(cancel_err) = self
                    .cancel_upload(guild_id, role_id, &upload_id, token)
                    .await
                {
                    tracing::warn!(
                        guild_id,
                        role_id,
                        upload_id,
                        "Cancel after chunk failure also failed: {cancel_err}"
                    );
                }
                return Err(e);
            }
        }

        let final_count = self
            .commit_upload(guild_id, role_id, &upload_id, token)
            .await?;
        tracing::info!(
            guild_id,
            role_id,
            upload_id,
            chunks = chunk_count,
            final_count,
            "Chunked upload committed"
        );
        Ok(final_count)
    }

    async fn start_upload(
        &self,
        guild_id: &str,
        role_id: &str,
        token: &str,
    ) -> Result<String, AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users/upload",
            self.base_url, guild_id, role_id
        );

        let resp = self
            .http
            .post(&url)
            .header("Authorization", format!("Token {token}"))
            .send()
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::FORBIDDEN && body.contains(RL_LINK_GONE_ERROR_MSG) {
                return Err(AppError::RoleLinkNotFound);
            }
            if link_inactive(status, &body) {
                return Err(AppError::RoleLinkDisabled);
            }
            return Err(AppError::RoleLogic(format!(
                "Start upload failed: {status} - {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(request_failed)?;

        body["data"]["upload_id"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| AppError::RoleLogic("Start upload response missing upload_id".into()))
    }

    async fn upload_chunk(
        &self,
        guild_id: &str,
        role_id: &str,
        upload_id: &str,
        user_ids: &[String],
        token: &str,
    ) -> Result<(), AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users/upload/{}/chunk",
            self.base_url, guild_id, role_id, upload_id
        );

        let resp = self
            .http
            .post(&url)
            .timeout(CHUNK_TIMEOUT)
            .header("Authorization", format!("Token {token}"))
            .json(user_ids)
            .send()
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::RoleLogic(format!(
                "Upload chunk failed: {status} - {body}"
            )));
        }

        Ok(())
    }

    async fn commit_upload(
        &self,
        guild_id: &str,
        role_id: &str,
        upload_id: &str,
        token: &str,
    ) -> Result<usize, AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users/upload/{}/commit",
            self.base_url, guild_id, role_id, upload_id
        );

        let resp = self
            .http
            .post(&url)
            .timeout(COMMIT_TIMEOUT)
            .header("Authorization", format!("Token {token}"))
            .send()
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if removal_held(status, &body) {
                return Err(AppError::RemovalHeld);
            }
            return Err(AppError::RoleLogic(format!(
                "Commit upload failed: {status} - {body}"
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(request_failed)?;

        Ok(body["data"]["user_count"].as_u64().unwrap_or(0) as usize)
    }

    async fn cancel_upload(
        &self,
        guild_id: &str,
        role_id: &str,
        upload_id: &str,
        token: &str,
    ) -> Result<(), AppError> {
        let url = format!(
            "{}/api/role-link/{}/{}/users/upload/{}",
            self.base_url, guild_id, role_id, upload_id
        );

        let resp = self
            .http
            .delete(&url)
            .header("Authorization", format!("Token {token}"))
            .send()
            .await
            .map_err(request_failed)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(AppError::RoleLogic(format!(
                "Cancel upload failed: {status} - {body}"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    // Bodies as RoleLogic sends them (role-link-token.guard.ts and
    // RoleLinkService.assertShrinkAllowed).
    const PAUSED: &str = r#"{"errors":"This role link is paused — integration slots quota exceeded. Upgrade for more integration slots or delete other role links.","requestId":"r"}"#;
    const DISABLED: &str = r#"{"errors":"This role link is disabled","requestId":"r"}"#;
    const GONE: &str = r#"{"errors":"Invalid or revoked token","requestId":"r"}"#;
    const HELD: &str = r#"{"errors":{"error":"confirm_required","code":"shrink","change_id":"c","previous":1,"next":0,"removals":1}}"#;

    #[test]
    fn a_failed_request_names_its_cause() {
        #[derive(Debug)]
        struct Wrap(&'static str, std::io::Error);
        impl std::fmt::Display for Wrap {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Wrap {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.1)
            }
        }
        let refused =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let e = Wrap("error sending request for url (https://x/users)", refused);
        assert_eq!(
            with_causes(&e),
            "error sending request for url (https://x/users): connection refused"
        );
        // A layer that repeats the message it wraps is not said twice.
        let echo = Wrap(
            "connection refused",
            std::io::Error::other("connection refused"),
        );
        assert_eq!(with_causes(&echo), "connection refused");
    }

    #[test]
    fn edge_failures_are_resent_and_answers_are_not() {
        for code in [502, 503, 504, 520, 521, 522, 523, 530] {
            assert!(
                edge_unavailable(StatusCode::from_u16(code).unwrap()),
                "{code}"
            );
        }
        // 524: the API may still be running it. The rest are RoleLogic's answer.
        for code in [400, 403, 404, 409, 429, 500, 524] {
            assert!(
                !edge_unavailable(StatusCode::from_u16(code).unwrap()),
                "{code}"
            );
        }
    }

    #[test]
    fn paused_and_disabled_links_are_inactive() {
        assert!(link_inactive(StatusCode::FORBIDDEN, PAUSED));
        assert!(link_inactive(StatusCode::FORBIDDEN, DISABLED));
        // A deleted link is a different outcome: the caller removes its row.
        assert!(!link_inactive(StatusCode::FORBIDDEN, GONE));
        assert!(!link_inactive(StatusCode::INTERNAL_SERVER_ERROR, PAUSED));
    }

    #[test]
    fn a_shrink_hold_is_only_a_409_confirm_required() {
        assert!(removal_held(StatusCode::CONFLICT, HELD));
        assert!(!removal_held(
            StatusCode::CONFLICT,
            r#"{"errors":"Upload already in progress"}"#
        ));
        assert!(!removal_held(StatusCode::FORBIDDEN, HELD));
    }

    #[test]
    fn notice_answers_are_classified() {
        assert_eq!(
            notice_outcome(StatusCode::OK, ""),
            Some(NoticeOutcome::Applied)
        );
        // An API from before notices existed: retry later, not an error.
        assert_eq!(
            notice_outcome(StatusCode::NOT_FOUND, r#"{"message":"Cannot PUT"}"#),
            Some(NoticeOutcome::Unsupported)
        );
        assert_eq!(
            notice_outcome(StatusCode::FORBIDDEN, GONE),
            Some(NoticeOutcome::LinkGone)
        );
        assert_eq!(
            notice_outcome(StatusCode::FORBIDDEN, DISABLED),
            Some(NoticeOutcome::Inactive)
        );
        assert_eq!(
            notice_outcome(StatusCode::FORBIDDEN, PAUSED),
            Some(NoticeOutcome::Inactive)
        );
        assert_eq!(notice_outcome(StatusCode::BAD_REQUEST, "{}"), None);
        assert_eq!(notice_outcome(StatusCode::INTERNAL_SERVER_ERROR, ""), None);
    }

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream};

    /// Read one request's head off `sock` (these requests carry no body).
    async fn read_head(sock: &TcpStream) {
        let mut head = Vec::new();
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            sock.readable().await.unwrap();
            let mut buf = [0u8; 1024];
            match sock.try_read(&mut buf) {
                Ok(0) => return,
                Ok(n) => head.extend_from_slice(&buf[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => return,
            }
        }
    }

    async fn write_all(sock: &TcpStream, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            sock.writable().await.unwrap();
            match sock.try_write(bytes) {
                Ok(n) => bytes = &bytes[n..],
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => return,
            }
        }
    }

    /// A local server answering the n-th connection with `statuses[n]`, one
    /// request per connection. Returns its client and how many it served.
    async fn serve(statuses: Vec<u16>) -> (RoleLogicClient, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let served = Arc::new(AtomicUsize::new(0));
        let count = served.clone();
        tokio::spawn(async move {
            for status in statuses {
                let (sock, _) = listener.accept().await.unwrap();
                read_head(&sock).await;
                count.fetch_add(1, Ordering::SeqCst);
                let body = r#"{"data":{"added":true,"removed":true}}"#;
                let reply = format!(
                    "HTTP/1.1 {status} Test\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                write_all(&sock, reply.as_bytes()).await;
            }
        });
        let client = RoleLogicClient {
            http: reqwest::Client::new(),
            base_url,
        };
        (client, served)
    }

    #[tokio::test]
    async fn an_edge_failure_is_resent_and_the_answer_used() {
        let (client, served) = serve(vec![502, 200]).await;
        let added = client.add_user("g", "r", "u", "t").await.unwrap();
        assert!(added);
        assert_eq!(served.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_answer_from_rolelogic_is_not_resent() {
        let (client, served) = serve(vec![400, 200]).await;
        assert!(client.remove_user("g", "r", "u", "t").await.is_err());
        assert_eq!(served.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resending_stops_after_the_last_backoff() {
        let (client, served) = serve(vec![503, 503, 503, 200]).await;
        assert!(client.remove_user("g", "r", "u", "t").await.is_err());
        assert_eq!(served.load(Ordering::SeqCst), 1 + RESEND_BACKOFF.len());
    }

    #[tokio::test]
    async fn a_refused_connection_counts_as_transient() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let err = reqwest::Client::new()
            .get(format!("http://{addr}"))
            .send()
            .await
            .unwrap_err();
        assert!(send_failed_transiently(&err), "{err:?}");
    }

    #[tokio::test]
    async fn a_timeout_is_not_resent() {
        // Accepts, then never answers.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _held = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        let err = reqwest::Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap()
            .get(format!("http://{addr}"))
            .send()
            .await
            .unwrap_err();
        assert!(err.is_timeout());
        assert!(!send_failed_transiently(&err));
    }
}
