# Development Mode (`development.enabled: true`)

This document describes every behavioral change that occurs when development mode is enabled in the configuration.

## Configuration

Development mode is controlled by the `development` section of the config YAML:

```yaml
development:
  enabled: true
  env_var: V4VAPP_DEV_MODE
  allowed_hive_accounts:
    - devser.v4vapp
    - devtre.v4vapp
    - v4vapp-test
    # ... other dev accounts
```

The underlying model is `DevelopmentConfig` in `src/v4vapp_backend_v2/config/setup.py`:

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `False` | Master switch for development mode |
| `env_var` | `str` | `V4VAPP_DEV_MODE` | Environment variable name set when dev mode is active |
| `allowed_hive_accounts` | `List[str]` | `[]` | Whitelist of Hive accounts permitted to transact in dev mode |

Production configs (e.g. `hive.config.yaml`) omit the `development` section entirely, which defaults `enabled` to `False`.

---

## Effects of Enabling Development Mode

### 1. Hive Account Whitelisting (Transaction Gating)

**Files:** `src/v4vapp_backend_v2/helpers/bad_actors_list.py`, `src/v4vapp_backend_v2/process/process_hive.py`, `src/v4vapp_backend_v2/hive/hive_extras.py`

When dev mode is enabled, **only accounts listed in `allowed_hive_accounts` may participate in transfers**. Any transfer involving an account not on the whitelist is rejected.

- `check_not_development_accounts()` returns `True` (meaning "block this") if dev mode is on and any participating account is missing from the allowed list.
- In `process_hive.py`, this causes `TransferBase` operations to be silently dropped with an error log.
- In `hive_extras.py`, it raises `HiveDevelopmentAccountError`, preventing the transfer from being broadcast.

**Ramification:** In production (`enabled: false`), this check always returns `False` and imposes no restriction — all valid Hive accounts can transact. Turning on dev mode **locks the system down** to only the explicitly listed accounts.

---

### 2. Custom JSON IDs Use Dev Prefixes

**File:** `src/v4vapp_backend_v2/process/hive_notification.py`

When sending custom JSON operations to the Hive blockchain, the `id` field changes based on dev mode:

| Scenario | Production ID | Development ID |
|---|---|---|
| Transfer with sats > 0 | `v4vapp_transfer` | `v4vapp_dev_transfer` |
| Notification (sats = 0) | `v4vapp_notification` | `v4vapp_dev_notification` |

This means:

- Dev custom JSON operations are **completely separated** from production ones on-chain.
- The Hive monitor must be configured to listen for the dev IDs. In the dev config, this is done under `custom_json_ids_tracked`:

```yaml
custom_json_ids_tracked:
  - "v4vapp_dev_transfer"
  - "v4vapp_dev_notification"
```

- Hardcoded dev IDs are also used in `send_notification_custom_json()` and `send_transfer_custom_json()` in the same file, which always emit `v4vapp_dev_notification` and `v4vapp_dev_transfer` respectively (these functions are only called in dev/testing paths).

**Ramification:** Development transactions are invisible to production monitors, and production transactions are invisible to development monitors. This provides full on-chain isolation.

---

### 3. Binance API Testnet/Mainnet Selection (Decoupled)

**Note:** Binance testnet vs mainnet is **not** controlled by `development.enabled`. It is controlled solely by the `exchange_mode` setting in the `exchange_config` section of the config:

```yaml
exchange_config:
  default_exchange: binance
  binance:
    exchange_mode: testnet   # or "mainnet"
```

The `get_client()` function in `binance_extras.py` reads `exchange_mode` from the config directly. This setting is independent of development mode so you can run in development mode against mainnet, or run in production mode against testnet, as needed.

---

### 4. Crypto Price Cache Times Are Extended

**File:** `src/v4vapp_backend_v2/helpers/crypto_prices.py`

When dev mode is enabled, Redis cache TTLs for price quotes use `TESTING_CACHE_TIMES` instead of `CACHE_TIMES`:

| Source | Production TTL (seconds) | Development TTL (seconds) |
|---|---|---|
| CoinGecko | 180 | 360 |
| Binance | 120 | 360 |
| CoinMarketCap | 1800 | 1800 |
| HiveInternalMarket | 10 | 10 |
| Global (aggregated quote) | 60 | 180 |

This applies in two places:
- `AllQuotes.get_all_quotes()` when caching the global aggregated quote.
- `QuoteService.set_cache()` when caching individual quote source results.

**Ramification:** Price data is refreshed less frequently in dev mode. This reduces API call volume to external price services during development and testing, avoiding rate limits. However, it means prices shown in dev may be stale compared to production.

---

### 5. MongoDB Connection Timeouts Are Extended

**File:** `src/v4vapp_backend_v2/database/db_pymongo.py`

The synchronous `MongoClient` uses dramatically different timeout values based on dev mode:

| Setting | Production | Development |
|---|---|---|
| `connectTimeoutMS` | 10,000 ms (10s) | 600,000 ms (10 min) |
| `serverSelectionTimeoutMS` | 10,000 ms (10s) | 600,000 ms (10 min) |

```python
timeout_ms = 600_000 if InternalConfig().config.development.enabled else 10_000
```

**Ramification:** In development, MongoDB operations will wait up to **10 minutes** before timing out. This accommodates scenarios where:
- The database is running on a remote/slow machine (e.g. via Tailscale).
- The developer is stepping through code in a debugger and the DB connection would otherwise time out.
- The MongoDB replica set is slow to become available (e.g. Docker startup time).

In production, the tight 10-second timeout ensures fast failure detection.

---

## Summary Table

| Area | Production Behavior | Development Behavior |
|---|---|---|
| **Account Access** | All valid Hive accounts | Only `allowed_hive_accounts` |
| **Custom JSON IDs** | `v4vapp_transfer` / `v4vapp_notification` | `v4vapp_dev_transfer` / `v4vapp_dev_notification` |
| **Binance API** | Controlled by `exchange_config.binance.exchange_mode` (independent of dev mode) | Same |
| **Price Cache TTLs** | Short (60–180s) | Long (180–360s) |
| **MongoDB Timeouts** | 10 seconds | 10 minutes |
| **Environment Variable** | Not set | `V4VAPP_DEV_MODE` available |

## Safety Considerations

- **Never enable `development.enabled: true` in a production config.** It will restrict all transactions to the allowed accounts list, potentially blocking real user transactions.
- The `allowed_hive_accounts` whitelist should only contain test/development accounts. If a production account is accidentally omitted, its transactions will be silently dropped.
- The extended MongoDB timeout in dev mode can mask connection issues that would be caught quickly in production.
- Price cache extension means trading decisions in dev may be based on stale data — acceptable for testing but not for live operations.
- Dev custom JSON IDs ensure on-chain separation, but test accounts still write real (if harmless) data to the Hive blockchain.

## Config Files Using Development Mode

| Config File | `development.enabled` |
|---|---|
| `config/devhive.config.yaml` | `true` |
| `config/devdocker.config.yaml` | `true` |
| `tests/data/config/config.yaml` | `true` (test fixtures) |
| All other production configs | `false` (default) |
