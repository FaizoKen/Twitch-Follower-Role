# Twitch Follower Role

A [RoleLogic](https://rolelogic.faizo.net) plugin that assigns Discord roles based on Twitch channel follow and subscription status. Users link their Discord and Twitch accounts, then roles are automatically assigned based on configurable conditions (follower status, follow tenure, subscription tier).

> **Requires [Auth Gateway](https://github.com/FaizoKen/Auth-Gateway)** — Discord login is handled by the centralized Auth Gateway. This plugin reads the shared `rl_session` cookie set by the gateway. Twitch OAuth for account linking is handled directly by this plugin.

## How it works

1. **Registers** guild/role pairs via the RoleLogic plugin API
2. **Authenticates** users through the centralized Auth Gateway (Discord OAuth)
3. **Links** Twitch accounts via Twitch OAuth
4. **Connects** the channel broadcaster to enable EventSub webhooks
5. **Monitors** follow/subscribe events in real-time via Twitch EventSub
6. **Syncs** role assignments to RoleLogic based on configurable conditions

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

| Method   | Path                      | Description                                 |
| -------- | ------------------------- | ------------------------------------------- |
| `GET`    | `/health`                 | Health check                                |
| `POST`   | `/register`               | Register a guild/role pair                  |
| `GET`    | `/config`                 | Get plugin configuration schema             |
| `POST`   | `/config`                 | Update role link conditions                 |
| `DELETE` | `/config`                 | Delete a registration                       |
| `GET`    | `/verify`                 | Verification page                           |
| `GET`    | `/verify/login`           | Redirects to Auth Gateway for Discord login |
| `GET`    | `/verify/status`          | Check linked account status                 |
| `GET`    | `/verify/twitch`          | Twitch OAuth login                          |
| `GET`    | `/verify/twitch/callback` | Twitch OAuth callback                       |
| `POST`   | `/verify/unlink`          | Unlink Twitch account                       |
| `POST`   | `/verify/logout`          | Logout session                              |
| `GET`    | `/connect`                | Broadcaster connection page                 |
| `GET`    | `/connect/callback`       | Broadcaster OAuth callback                  |
| `POST`   | `/webhooks/twitch`        | Twitch EventSub webhook receiver            |

## Conditions

Admins can configure these conditions per role link (all enabled conditions must be met):

| Condition              | Description                                          |
| ---------------------- | ---------------------------------------------------- |
| **Require Follower**   | User must be following the channel                   |
| **Min Follow Days**    | Minimum days the user has been following (0+)        |
| **Require Subscriber** | User must have an active subscription                |
| **Min Sub Tier**       | Minimum subscription tier (1 = any, 2 = T2+, 3 = T3) |

## Usage

1. Ensure the [Auth Gateway](https://github.com/FaizoKen/Auth-Gateway) is running on `your-domain.com/auth/*`
2. In the RoleLogic dashboard, create a Role Link and set the **Custom Plugin URL** to `https://your-domain.com/twitch-follower-role`
3. The channel owner connects their Twitch account via the `/connect` page
4. Users visit the verification page, sign in with Discord (via Auth Gateway), and link their Twitch account
5. Roles are assigned automatically based on the conditions you configure

## API Reference

- [RoleLogic Role Link API](https://docs-rolelogic.faizo.net/reference/role-link-api)
- [Twitch Helix API](https://dev.twitch.tv/docs/api)
- [Twitch EventSub](https://dev.twitch.tv/docs/eventsub)

## License

[MIT](LICENSE)
