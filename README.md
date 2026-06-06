# Twitch Follower Role

A [RoleLogic](https://rolelogic.faizo.net) plugin that assigns Discord roles based on a member's relationship to a Twitch channel — follower, follow tenure, subscriber, and sub tier — composed into a **DNF rule tree** (OR of AND-groups) through an in-dashboard iframe rule builder. Admins can express rules like _"(follower for ≥30 days) OR (subscriber at Tier 2+)"_ without nesting.

> **Requires [Auth Gateway](https://github.com/FaizoKen/Auth-Gateway)** — Discord login is handled by the centralized Auth Gateway. This plugin reads the shared `rl_session` cookie set by the gateway. Twitch OAuth for account linking is handled directly by this plugin.

## How it works

1. **Registers** guild/role pairs via the RoleLogic plugin API
2. **Configures** rules through an iframe rule-builder embedded in the RoleLogic dashboard (preset chooser + advanced AND/OR builder, live match-count preview, in-iframe channel connect)
3. **Authenticates** admins via a dual-mode gate: a RoleLogic `rl_token` JWT (iframe) or the `rl_session` cookie + an Auth-Gateway manager check (direct navigation)
4. **Links** member Twitch accounts via Twitch OAuth
5. **Connects** the channel broadcaster to enable EventSub webhooks
6. **Monitors** follow/subscribe events in real-time via Twitch EventSub
7. **Syncs** role assignments to RoleLogic by evaluating the rule tree (a Rust evaluator for per-user updates, a pushdown-SQL builder for bulk per-role-link re-sync)

## Setup

```bash
cp .env.example .env
# Edit .env with your values
```

### Environment Variables

| Variable                 | Required | Default                                     | Description                                                               |
| ------------------------ | -------- | ------------------------------------------- | ------------------------------------------------------------------------- |
| `DATABASE_URL`           | Yes      | --                                          | PostgreSQL connection string                                              |
| `SESSION_SECRET`         | Yes      | --                                          | HMAC key for `rl_session` cookie (must match Auth Gateway)                |
| `TWITCH_CLIENT_ID`       | Yes      | --                                          | Twitch API app client ID                                                  |
| `TWITCH_CLIENT_SECRET`   | Yes      | --                                          | Twitch API app client secret                                              |
| `TWITCH_EVENTSUB_SECRET` | Yes      | --                                          | HMAC secret for EventSub webhooks                                         |
| `BASE_URL`               | Yes      | --                                          | Full URL with prefix, e.g. `https://your-domain.com/twitch-follower-role` |
| `INTERNAL_API_KEY`       | Yes      | --                                          | Shared secret for plugin → Auth Gateway `/auth/internal/*` calls           |
| `AUTH_GATEWAY_URL`       | No       | derived from `BASE_URL` origin              | Auth Gateway base URL (set explicitly in local dev)                       |
| `RL_DASHBOARD_ORIGIN`    | No       | `https://rolelogic.faizo.net`               | Origin allowed to embed the iframe (CSP `frame-ancestors`, CORS/CSRF)     |
| `LISTEN_ADDR`            | No       | `0.0.0.0:8080`                              | Server bind address                                                       |
| `RUST_LOG`               | No       | `twitch_follower_role=info,tower_http=info` | Log level                                                                 |

## Run

### Docker (recommended)

```bash
docker compose up -d
```

### From source

```bash
cargo run              # development
cargo build --release  # production
```

## Endpoints

All routes are nested under `/twitch-follower-role`:

| Method   | Path                                       | Description                                          |
| -------- | ------------------------------------------ | ---------------------------------------------------- |
| `GET`    | `/health`                                  | Health check                                         |
| `POST`   | `/register`                                | Register a guild/role pair                           |
| `GET`    | `/config`                                  | Returns the iframe embed config (UI mode)            |
| `POST`   | `/config`                                  | No-op (iframe mode); token-verified for compliance   |
| `DELETE` | `/config`                                  | Delete a registration                                |
| `GET`    | `/admin/{guild}/role/{role}`               | Iframe rule-builder page (dual-mode auth)            |
| `GET`    | `/admin/{guild}/role/{role}/data`          | Rule-builder data (config, channel, catalogs)        |
| `POST`   | `/admin/{guild}/role/{role}/save`          | Save the rule tree (optimistic-locked)               |
| `GET`/`POST` | `/admin/{guild}/role/{role}/preview`   | Dry-run match count (saved / proposed rule)          |
| `POST`   | `/admin/{guild}/role/{role}/connect`       | Start broadcaster OAuth for this role link           |
| `POST`   | `/admin/{guild}/role/{role}/disconnect`    | Detach the broadcaster from this role link           |
| `POST`   | `/admin/{guild}/view-permission`           | Set who can view the public users list                |
| `GET`    | `/users/{guild}`                           | Public linked-users list page                         |
| `GET`    | `/users/{guild}/data`                      | Users-list data (gated by `view_permission`)          |
| `GET`    | `/verify`                                  | Member verification page                             |
| `GET`    | `/verify/login`                            | Redirects to Auth Gateway for Discord login          |
| `GET`    | `/verify/status`                           | Check linked account status                          |
| `POST`   | `/verify/refresh`                          | Member-triggered re-check of follow/sub status       |
| `GET`    | `/verify/twitch`                           | Twitch OAuth login                                   |
| `GET`    | `/verify/twitch/callback`                  | Twitch OAuth callback                                |
| `POST`   | `/verify/unlink`                           | Unlink Twitch account                                |
| `POST`   | `/verify/logout`                           | Logout session                                       |
| `GET`    | `/connect/callback`                        | Broadcaster OAuth callback                            |
| `POST`   | `/webhooks/twitch`                         | Twitch EventSub webhook receiver                     |

## Rule builder

Configuration happens inside the RoleLogic dashboard, in an embedded iframe. A
preset chooser covers the common cases; an **Advanced rule** option exposes the
full DNF builder (OR of AND-groups). A rule matches a member if they satisfy
**any** group, and within a group **all** conditions must hold.

**Targets** (facts about a member's relationship to the connected channel):

| Target              | Type | Meaning                                  |
| ------------------- | ---- | ---------------------------------------- |
| `is_follower`       | bool | Currently follows the channel            |
| `follow_age_days`   | int  | Whole days since they first followed     |
| `is_subscriber`     | bool | Has an active paid subscription          |
| `sub_tier`          | int  | Subscription tier (1, 2, or 3)           |

**Operators**: `equals`, `not equals`, `greater than`, `at least`, `less than`,
`at most`, `between` (bool targets support `equals` only).

The **"Anyone who linked their Twitch"** preset is channel-agnostic — it grants
the role to every member who has linked a Twitch account, no channel required.
Every other rule needs a connected channel; without one it grants to nobody.

## Public users page

The config iframe also exposes an optional, shareable **`/users/{guild}`** page:
a read-only, searchable/filterable list of every linked member in the server and
their relationship (follower / subscriber / tier) to the connected channel.
Visibility is set per guild via `guild_settings.view_permission` — `disabled`,
`managers` (Manage-Server only, the default), or `members` (any member). The
page itself authenticates with the `rl_session` cookie; on 401 it renders an
in-page "Sign in with Discord" prompt rather than auto-redirecting.

## Usage

1. Ensure the [Auth Gateway](https://github.com/FaizoKen/Auth-Gateway) is running on `your-domain.com/auth/*`
2. In the RoleLogic dashboard, create a Role Link and set the **Custom Plugin URL** to `https://your-domain.com/twitch-follower-role`
3. Open the role's plugin tab in the dashboard, pick who should get the role, and **Connect Twitch channel** right from the iframe
4. Share the verify link (shown in the iframe) so members sign in with Discord and link their Twitch account
5. Roles are assigned automatically by evaluating your rule, in real time as follow/sub events arrive

## API Reference

- [RoleLogic Role Link API](https://docs-rolelogic.faizo.net/reference/role-link-api)
- [Twitch Helix API](https://dev.twitch.tv/docs/api)
- [Twitch EventSub](https://dev.twitch.tv/docs/eventsub)

## License

[MIT](LICENSE)
