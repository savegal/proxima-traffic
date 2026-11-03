# Proxy reliability and MTProto feed

## Supported routes

- HTTP(S), SOCKS5 and SOCKS5H: first verified via the configured HTTPS IP-check endpoint (a valid returned IP is required). If Telegram API credentials are configured, the same route must also establish a disposable Telegram transport connection; IP-check success alone is not enough. SOCKS5 uses remote DNS. With no API credentials, HTTPS is not marked usable because Telethon's legacy HTTP CONNECT transport is not a TLS-to-proxy client.
- MTProto legacy and `dd`: verified by connecting to Telegram over the MTProto proxy. An open TCP port alone does not mark a proxy as working.
- MTProto `ee` Fake-TLS: **not currently supported by Telethon 1.44**. The bot can recognise these links but does not mark them working or assign them to accounts. The server-side Fake-TLS handshake is not equivalent to ordinary intermediate MTProto.

Proxy secrets and passwords stay encrypted at rest. A proxy operator can still observe client IP/connection metadata; public proxies must not be treated as equivalent to owned residential proxies.

## Automatic route recovery

1. The currently assigned endpoint is checked before use.
2. If it is unhealthy, the system tests enabled alternatives with at most eight network probes at the same time.
3. Only a freshly verified, enabled endpoint can replace the current assignment.
4. The account's connection generation is incremented; cached Telegram clients reconnect using the new route.
5. When no alternative works, the account **does not fall back to direct networking**.
6. An out-of-date probe never updates the status of an endpoint changed/disabled while testing.

Optional slow-link failover uses `PROXIMA_PROXY_SLOW_FAILOVER_ENABLED=true`. The previous recorded delay and a new check must both exceed `PROXIMA_PROXY_SLOW_FAILOVER_LATENCY_MS` (default 6000 ms). A replacement must be substantially faster (below both the configured threshold and about 65% of the slow link). If none exists, the still-working route stays assigned. This option is **off by default** to protect stable account/IP assignments.

## Public MTProto channel

In the bot open **Прокси → Свежие из канала → Подключиться и проверить**.

- The operator explicitly confirms the action before any account joins `@ProxyMTProto`.
- One already-authorised, owned, active Telegram account with a **working assigned proxy** is selected. The bot does not connect directly to bootstrap itself and does not join using every account.
- The service checks membership and joins the public channel only when necessary, then reads the most recent messages through the existing account operation locks.
- Telegram text-link entities are inspected; untrusted prose, duplicate links, unsupported `ee` routes, private IPs and stale messages are excluded.
- Only a bounded number of candidates are probed. Healthy endpoints are imported into group `MTProto · ProxyMTProto`; existing duplicates are rechecked.
- Manually disabled endpoints are not re-enabled by channel refresh.
- The public feed is retained as a bounded backup: keep the newest `PROXIMA_MTPROTO_FEED_POOL_LIMIT` enabled/unassigned entries (default 120). Older unassigned entries are deleted only from the dedicated feed group. Assigned endpoints and manually disabled entries are always preserved.
- If no working reader-account proxy exists, feed refresh fails safely. Keep at least one independent reliable bootstrap proxy.

To enable **periodic** refresh, configure:

```ini
PROXIMA_MTPROTO_FEED_ENABLED=true
PROXIMA_MTPROTO_FEED_INTERVAL_SECONDS=3600
PROXIMA_MTPROTO_FEED_MAX_MESSAGES=60
PROXIMA_MTPROTO_FEED_MAX_CANDIDATES=20
PROXIMA_MTPROTO_FEED_POOL_LIMIT=120
PROXIMA_ACCOUNT_REQUIRE_PROXY=true
```

Automatic feed refresh is **disabled by default**. It is also disabled during DRY_RUN. Do not enable it without accepting that one of your accounts may join a public channel and access untrusted third-party servers. This is a *proxy supply source*, not a guarantee of permanent availability, confidentiality or speed.

## Acceptance

Before enabling on production accounts, run `ruff check` and the targeted proxy `pytest` suite, and do a manual smoke test using a dedicated expendable Telegram account. The public CI checkout does not include the checksum-protected `migrations/data/global_mention_blacklist_ids.txt` asset required by migration 0051; proxy unit tests therefore set `PROXIMA_PROXY_MODEL_SCHEMA_ONLY=1` to create a model-only SQLite schema. This **does not test the production migration path**. A full migration smoke test needs the authentic separately-managed asset, not a fabricated or empty replacement. Check MTProto with a real `dd` link, a dead address, a reserved/private address, a duplicate link, a manually disabled endpoint, and unavailable feed access. Live endpoints and join operations cannot be verified solely from mocked unit tests.
