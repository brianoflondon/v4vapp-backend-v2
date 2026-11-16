"""Telegram outbox: retry a failed send later, and dedupe on a stable key.

A network failure is stored in Redis (hash ``telegram:outbox`` plus a due-time
sorted set) instead of being dropped after the short inline retries. One
process drains the outbox. While ``egress:down`` is set the drainer sends
nothing. When the path returns, messages queued during the outage become one
digest, not a replay of each alert.

The dedupe key is the invoice r_hash plus state, or the error_code. The last
20 characters of the text are not an identity.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import random
import re
import sys
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from telegram.error import NetworkError, RetryAfter

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.egress_health import egress_is_down

OUTBOX_HASH = "telegram:outbox"
OUTBOX_DUE = "telegram:outbox:due"
OUTBOX_DEDUPE_PREFIX = "telegram:outbox:dedupe:"
OUTBOX_LOCK = "telegram:outbox:lock"

OUTBOX_BACKOFF_CAP_S = 300
OUTBOX_STALE_AFTER_S = 30 * 60
OUTBOX_MAX_AGE_S = 6 * 60 * 60
DRAIN_POLL_S = 5.0
LOCK_TTL_S = 120
DEDUPE_TTL_INVOICE_S = 24 * 60 * 60
DEDUPE_TTL_ERROR_S = 60 * 60
DEDUPE_TTL_TEXT_S = 10 * 60
NOT_READY_DELAY_S = 60

BYPASS = "bypass"
_PERSISTED_KWARGS = ("parse_mode", "disable_web_page_preview", "disable_notification")

_HASH_RE = re.compile(r"\b[0-9a-fA-F]{64}\b")
_STATE_RE = re.compile(r"\b(OPEN|SETTLED|CANCELED|CANCELLED|ACCEPTED)\b", re.IGNORECASE)

_memory: MemoryRedis | None = None
_client_override: Any = None
_drainer_task: asyncio.Task | None = None
_last_dependency_warn = 0.0
_stale_logged_id = ""


class OutboxNotReady(Exception):
    """The bot config is missing. Keep the item and try again later."""


class MemoryRedis:
    """In-process stand-in for the commands the outbox uses. Tests use this."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.expiry: dict[str, int | None] = {}

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value, ex: int | None = None, nx: bool = False):
        if nx and key in self.values:
            return None
        self.values[key] = value if isinstance(value, str) else str(value)
        self.expiry[key] = ex
        return True

    def delete(self, *keys: str):
        removed = 0
        for key in keys:
            if self.values.pop(key, None) is not None:
                removed += 1
            self.hashes.pop(key, None)
            self.zsets.pop(key, None)
            self.expiry.pop(key, None)
        return removed

    def expire(self, key: str, ttl: int):
        if key not in self.values and key not in self.hashes and key not in self.zsets:
            return False
        self.expiry[key] = ttl
        return True

    def hset(self, name: str, key: str, value: str):
        self.hashes.setdefault(name, {})[key] = value
        return 1

    def hget(self, name: str, key: str):
        return self.hashes.get(name, {}).get(key)

    def hgetall(self, name: str):
        return dict(self.hashes.get(name, {}))

    def hdel(self, name: str, *keys: str):
        bucket = self.hashes.get(name, {})
        removed = 0
        for key in keys:
            if bucket.pop(key, None) is not None:
                removed += 1
        return removed

    def zadd(self, name: str, mapping: dict[str, float]):
        bucket = self.zsets.setdefault(name, {})
        for member, score in mapping.items():
            bucket[member] = float(score)
        return len(mapping)

    def zrangebyscore(self, name: str, minimum, maximum):
        bucket = self.zsets.get(name, {})
        low = float("-inf") if minimum in {"-inf", "-infinity"} else float(minimum)
        high = float("inf") if maximum in {"+inf", "+infinity", "inf"} else float(maximum)
        return [member for member, score in bucket.items() if low <= score <= high]

    def zrem(self, name: str, *members: str):
        bucket = self.zsets.get(name, {})
        removed = 0
        for member in members:
            if bucket.pop(member, None) is not None:
                removed += 1
        return removed


def reset_for_tests() -> None:
    """Drop the in-memory outbox, the injected client, and the stale-log gate."""
    global _memory, _client_override, _drainer_task, _last_dependency_warn, _stale_logged_id
    _memory = MemoryRedis()
    _client_override = None
    _drainer_task = None
    _last_dependency_warn = 0.0
    _stale_logged_id = ""


def set_outbox_client(client: Any) -> None:
    """Tests inject a Redis double. Production leaves this unset."""
    global _client_override
    _client_override = client


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return "" if value is None else str(value)


def _pytest_running() -> bool:
    return "pytest" in sys.modules or bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _memory_client() -> MemoryRedis:
    global _memory
    if _memory is None:
        _memory = MemoryRedis()
    return _memory


def redis_client():
    """The outbox Redis. Under pytest this is in-memory unless a test injects one."""
    if _client_override is not None:
        return _client_override
    if _pytest_running():
        return _memory_client()
    from v4vapp_backend_v2.config.setup import InternalConfig

    client = getattr(InternalConfig, "redis_decoded", None)
    if client is None:
        raise RuntimeError("Redis is not configured")
    return client


def drainer_enabled() -> bool:
    """The background drainer stays off under pytest and when TELEGRAM_OUTBOX=0."""
    flag = os.environ.get("TELEGRAM_OUTBOX", "1").lower()
    if flag in {"0", "false", "no"}:
        return False
    return not _pytest_running()


def _warn_dependency_once(message: str) -> None:
    global _last_dependency_warn
    now = time.monotonic()
    if now - _last_dependency_warn < 60:
        return
    _last_dependency_warn = now
    logger.warning(message, extra={"notification": False})


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def _category_for(text: str, logger_name: str, error_code: str) -> str:
    blob = f"{text} {logger_name} {error_code}".lower()
    if "binance" in blob:
        return "binance"
    return "other"


def classify_notification(
    text: str,
    *,
    error_code: str | None = None,
    r_hash: str | None = None,
    state: str | None = None,
    logger_name: str = "",
) -> tuple[str, str]:
    """Return ``(dedupe_key, category)``. Category is invoice, binance, or other."""
    error = error_code.strip() if isinstance(error_code, str) else ""
    hash_value = ""
    if isinstance(r_hash, str) and _HASH_RE.fullmatch(r_hash):
        hash_value = r_hash.lower()
    else:
        found = _HASH_RE.search(text)
        if found:
            hash_value = found.group(0).lower()
    invoice_state = ""
    if isinstance(state, str) and state.strip():
        invoice_state = state.strip().upper()
    else:
        found_state = _STATE_RE.search(text)
        if found_state:
            invoice_state = found_state.group(1).upper()
    is_invoice = "invoice" in text.lower() or (
        isinstance(r_hash, str) and bool(_HASH_RE.fullmatch(r_hash))
    )
    if hash_value and is_invoice:
        return f"invoice:{hash_value}:{invoice_state or 'unknown'}", "invoice"
    if error:
        return f"error:{error}", _category_for(text, logger_name, error)
    if hash_value:
        category = _category_for(text, logger_name, error)
        return f"event:{hash_value}:{invoice_state or 'unknown'}", category
    digest = hashlib.sha256(_normalize(text).encode()).hexdigest()[:16]
    return f"text:{digest}", _category_for(text, logger_name, error)


def _record_state(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    enum_value = getattr(value, "value", None)
    if isinstance(enum_value, str) and enum_value.strip():
        return enum_value.strip()
    return None


def identity_from_record(text: str, record: Any) -> tuple[str, str]:
    """Stable identity from a log record. Invoice hash and state win over error_code."""
    error_code = getattr(record, "error_code", None)
    if not isinstance(error_code, str) or not error_code.strip():
        error_code = None
    r_hash = None
    state = None
    invoice = getattr(record, "invoice", None)
    if isinstance(invoice, dict):
        raw_hash = invoice.get("r_hash")
        if isinstance(raw_hash, str):
            r_hash = raw_hash
        state = _record_state(invoice.get("state"))
    elif invoice is not None:
        raw_hash = getattr(invoice, "r_hash", None)
        if isinstance(raw_hash, str):
            r_hash = raw_hash
        state = _record_state(getattr(invoice, "state", None))
    if r_hash is None:
        group_id = getattr(record, "group_id", None)
        if isinstance(group_id, str):
            r_hash = group_id
    logger_name = getattr(record, "name", "") or ""
    if not isinstance(logger_name, str):
        logger_name = ""
    return classify_notification(
        text,
        error_code=error_code,
        r_hash=r_hash,
        state=state,
        logger_name=logger_name,
    )


def dedupe_ttl_s(key: str) -> int:
    if key.startswith("invoice:"):
        return DEDUPE_TTL_INVOICE_S
    if key.startswith("error:"):
        return DEDUPE_TTL_ERROR_S
    return DEDUPE_TTL_TEXT_S


def queued_ttl_s(key: str) -> int:
    """Keep a queued key until the item itself would be dropped."""
    return max(dedupe_ttl_s(key), OUTBOX_MAX_AGE_S)


def backoff_seconds(attempt: int) -> float:
    """Exponential delay with a little jitter, capped at about 5 minutes."""
    step = 2 ** max(1, attempt)
    return min(float(OUTBOX_BACKOFF_CAP_S), step + random.random())


def persistable_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    saved: dict[str, Any] = {}
    for name in _PERSISTED_KWARGS:
        if name in kwargs:
            saved[name] = kwargs[name]
    return saved


def format_outbox_digest(counts: dict[str, int]) -> str:
    """One line for everything queued while the outbound path was down."""
    invoice = int(counts.get("invoice") or 0)
    binance = int(counts.get("binance") or 0)
    other = sum(int(value) for key, value in counts.items() if key not in {"invoice", "binance"})
    total = invoice + binance + other
    noun = "notification" if total == 1 else "notifications"
    parts: list[str] = []
    if invoice:
        parts.append(f"{invoice} invoice")
    if binance:
        parts.append(f"{binance} Binance")
    if other:
        parts.append(f"{other} other")
    detail = ", ".join(parts) if parts else "0 other"
    return f"{total} {noun} queued during the outage: {detail}"


def _dedupe_redis_key(key: str) -> str:
    return f"{OUTBOX_DEDUPE_PREFIX}{key}"


class Outbox:
    def __init__(self, client: Any) -> None:
        self.client = client

    def claim(self, key: str, token: str, ttl: int) -> bool:
        claimed = self.client.set(_dedupe_redis_key(key), token, nx=True, ex=ttl)
        return bool(claimed)

    def rebind(self, key: str, token: str, ttl: int) -> None:
        self.client.set(_dedupe_redis_key(key), token, ex=ttl)

    def touch(self, key: str, token: str, ttl: int) -> None:
        current = self.client.get(_dedupe_redis_key(key))
        if _text(current) == token:
            self.client.set(_dedupe_redis_key(key), token, ex=ttl)

    def enqueue(
        self,
        *,
        text: str,
        chat_id: int,
        bot_name: str,
        kwargs: dict[str, Any],
        dedupe_key: str,
        category: str,
        hold_for_digest: bool,
        created_at: datetime | None = None,
        next_attempt_at: datetime | None = None,
        attempts: int = 0,
    ) -> str:
        item_id = uuid.uuid4().hex
        created = created_at or datetime.now(tz=UTC)
        if next_attempt_at is None:
            if hold_for_digest:
                due = created
            else:
                due = created + timedelta(seconds=backoff_seconds(max(attempts, 1)))
        else:
            due = next_attempt_at
        item = {
            "id": item_id,
            "text": text,
            "chat_id": chat_id,
            "bot_name": bot_name,
            "kwargs": kwargs,
            "dedupe_key": dedupe_key,
            "category": category or "other",
            "created_at": created.isoformat(),
            "next_attempt_at": due.isoformat(),
            "attempts": attempts,
            "hold_for_digest": hold_for_digest,
            "stale_logged": False,
        }
        self.client.hset(OUTBOX_HASH, item_id, json.dumps(item))
        self.client.zadd(OUTBOX_DUE, {item_id: due.timestamp()})
        return item_id

    def all_items(self) -> list[dict[str, Any]]:
        raw = self.client.hgetall(OUTBOX_HASH) or {}
        items: list[dict[str, Any]] = []
        for item_id, payload in raw.items():
            try:
                item = json.loads(_text(payload))
            except (json.JSONDecodeError, TypeError):
                self.client.hdel(OUTBOX_HASH, _text(item_id))
                continue
            if isinstance(item, dict) and item.get("id"):
                items.append(item)
        return items

    def due_items(self, now: datetime) -> list[dict[str, Any]]:
        ids = self.client.zrangebyscore(OUTBOX_DUE, "-inf", now.timestamp()) or []
        items: list[dict[str, Any]] = []
        for item_id in ids:
            raw = self.client.hget(OUTBOX_HASH, _text(item_id))
            if raw is None:
                self.client.zrem(OUTBOX_DUE, _text(item_id))
                continue
            try:
                item = json.loads(_text(raw))
            except (json.JSONDecodeError, TypeError):
                self.remove(_text(item_id), sent=False)
                continue
            if isinstance(item, dict):
                items.append(item)
        return items

    def save(self, item: dict[str, Any], when: datetime) -> None:
        item["next_attempt_at"] = when.isoformat()
        self.client.hset(OUTBOX_HASH, item["id"], json.dumps(item))
        self.client.zadd(OUTBOX_DUE, {item["id"]: when.timestamp()})

    def remove(self, item_id: str, *, sent: bool) -> None:
        raw = self.client.hget(OUTBOX_HASH, item_id)
        item: dict[str, Any] | None = None
        if raw is not None:
            try:
                loaded = json.loads(_text(raw))
            except (json.JSONDecodeError, TypeError):
                loaded = None
            if isinstance(loaded, dict):
                item = loaded
        self.client.hdel(OUTBOX_HASH, item_id)
        self.client.zrem(OUTBOX_DUE, item_id)
        if sent and item and item.get("dedupe_key"):
            key = str(item["dedupe_key"])
            self.client.set(_dedupe_redis_key(key), "sent", ex=dedupe_ttl_s(key))

    def mark_all_held(self) -> None:
        for item in self.all_items():
            if item.get("hold_for_digest"):
                continue
            item["hold_for_digest"] = True
            due = _parse_time(item.get("next_attempt_at")) or datetime.now(tz=UTC)
            self.save(item, due)


def claim_send(key: str) -> str | None:
    """Claim the dedupe key. None means a live or recent send already owns it.

    A Redis fault returns ``BYPASS`` so the send still happens.
    """
    token = uuid.uuid4().hex
    try:
        if Outbox(redis_client()).claim(key, token, dedupe_ttl_s(key)):
            return token
    except Exception:
        _warn_dependency_once("Telegram dedupe unavailable; sending this notification")
        return BYPASS
    logger.debug(
        f"Skipping duplicate notification {key}",
        extra={"notification": False, "dedupe_key": key},
    )
    return None


def finish_send(key: str, token: str) -> None:
    if not token or token == BYPASS:
        return
    try:
        Outbox(redis_client()).touch(key, token, dedupe_ttl_s(key))
    except Exception:
        _warn_dependency_once("Telegram dedupe unavailable after a successful send")


def queue_failed_send(
    *,
    text: str,
    chat_id: int,
    bot_name: str,
    kwargs: dict[str, Any],
    dedupe_key: str,
    dedupe_token: str,
    category: str,
    hold_for_digest: bool,
    attempt: int,
    retries: int,
) -> str | None:
    """Store one failed send. Logs a single WARNING that includes the outbox id."""
    try:
        box = Outbox(redis_client())
        item_id = box.enqueue(
            text=text,
            chat_id=chat_id,
            bot_name=bot_name,
            kwargs=kwargs,
            dedupe_key=dedupe_key,
            category=category,
            hold_for_digest=hold_for_digest,
            attempts=attempt,
        )
        if dedupe_token and dedupe_token != BYPASS:
            box.rebind(dedupe_key, item_id, queued_ttl_s(dedupe_key))
    except Exception:
        _warn_dependency_once("Telegram send failed; outbox unavailable")
        return None
    logger.warning(
        f"Telegram send failed; queued {item_id}",
        extra={
            "notification": False,
            "outbox_id": item_id,
            "attempt": attempt,
            "retries": retries,
            "category": category,
        },
    )
    return item_id


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _age_s(item: dict[str, Any], now: datetime) -> float | None:
    created = _parse_time(item.get("created_at"))
    if created is None:
        return None
    return (now - created).total_seconds()


def _note_stale(items: list[dict[str, Any]], now: datetime) -> None:
    """One ERROR when the oldest waiting item is past the stale limit."""
    global _stale_logged_id
    old = []
    for item in items:
        age = _age_s(item, now)
        if age is not None and age >= OUTBOX_STALE_AFTER_S:
            old.append(item)
    if not old:
        return
    oldest = min(old, key=lambda item: item.get("created_at") or "")
    oldest_id = str(oldest.get("id") or "")
    if not oldest_id or oldest_id == _stale_logged_id:
        return
    _stale_logged_id = oldest_id
    logger.error(
        f"Telegram outbox {oldest_id} is older than 30 min ({len(old)} waiting)",
        extra={"notification": False, "outbox_id": oldest_id, "outbox_waiting": len(old)},
    )


def _drop_expired(box: Outbox, items: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    kept: list[dict[str, Any]] = []
    dropped = 0
    first_id = ""
    for item in items:
        age = _age_s(item, now)
        if age is not None and age >= OUTBOX_MAX_AGE_S:
            dropped += 1
            first_id = first_id or str(item.get("id") or "")
            box.remove(str(item["id"]), sent=False)
            continue
        kept.append(item)
    if dropped:
        logger.warning(
            f"Dropping {dropped} Telegram outbox items older than 6h",
            extra={"notification": False, "outbox_id": first_id},
        )
    return kept


def _reschedule(box: Outbox, item: dict[str, Any], when: datetime) -> None:
    item["attempts"] = int(item.get("attempts") or 0) + 1
    box.save(item, when)


async def _send_digest(box: Outbox, held: list[dict[str, Any]], deliver) -> int:
    """Replace a down-path batch with one message per chat. 0 if the send fails."""
    groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for item in held:
        chat_id = int(item.get("chat_id") or 0)
        groups.setdefault((str(item.get("bot_name") or ""), chat_id), []).append(item)
    sent_messages = 0
    for (bot_name, chat_id), group in groups.items():
        counts = Counter(str(item.get("category") or "other") for item in group)
        digest = {
            "id": "digest",
            "text": format_outbox_digest(dict(counts)),
            "chat_id": chat_id,
            "bot_name": bot_name,
            "kwargs": {},
            "append_machine": True,
            "category": "other",
        }
        try:
            await deliver(digest)
        except (NetworkError, RetryAfter, OutboxNotReady):
            logger.debug(
                "Telegram outbox digest delayed; outbound send failed",
                extra={"notification": False, "outbox_waiting": len(group)},
            )
            return sent_messages
        except Exception:
            logger.exception(
                "Telegram outbox digest failed",
                extra={"notification": False, "outbox_waiting": len(group)},
            )
            return sent_messages
        for item in group:
            box.remove(str(item["id"]), sent=True)
        sent_messages += len(group)
    return sent_messages


async def drain_once(
    box: Outbox,
    *,
    deliver,
    now: datetime | None = None,
    egress_down: bool | None = None,
) -> int:
    """Retry due items, or fold an outage batch into one digest.

    Returns how many queued notifications were cleared. The drainer does not
    send while the outbound path is down.
    """
    moment = now or datetime.now(tz=UTC)
    down = egress_is_down() if egress_down is None else egress_down
    if down:
        box.mark_all_held()
        _note_stale(_drop_expired(box, box.all_items(), moment), moment)
        return 0
    waiting = _drop_expired(box, box.all_items(), moment)
    _note_stale(waiting, moment)
    cleared = 0
    held = [item for item in waiting if item.get("hold_for_digest")]
    if held:
        cleared += await _send_digest(box, held, deliver)
    for item in box.due_items(moment):
        if item.get("hold_for_digest"):
            continue
        try:
            await deliver(item)
        except RetryAfter as exc:
            delay = float(getattr(exc, "retry_after", NOT_READY_DELAY_S) or NOT_READY_DELAY_S)
            _reschedule(box, item, moment + timedelta(seconds=delay))
        except NetworkError:
            _reschedule(
                box,
                item,
                moment + timedelta(seconds=backoff_seconds(int(item.get("attempts") or 0) + 1)),
            )
        except OutboxNotReady:
            _reschedule(box, item, moment + timedelta(seconds=NOT_READY_DELAY_S))
            _warn_dependency_once("Telegram outbox waiting; notification bot is not set up")
            break
        except Exception:
            logger.exception(
                f"Dropping Telegram outbox {item.get('id')}",
                extra={"notification": False, "outbox_id": item.get("id")},
            )
            box.remove(str(item["id"]), sent=False)
        else:
            box.remove(str(item["id"]), sent=True)
            cleared += 1
        # One live send per pass. A burst of retries would trip Telegram's flood limit,
        # and a slow send must finish before the outbox lock expires.
        break
    if cleared:
        logger.info(
            f"outbox drained {cleared} messages",
            extra={"notification": False, "outbox_drained": cleared},
        )
    return cleared


def _hold_lock(client: Any, token: str) -> bool:
    try:
        current = client.get(OUTBOX_LOCK)
    except Exception:
        return False
    if current is not None:
        if _text(current) == token:
            try:
                client.expire(OUTBOX_LOCK, LOCK_TTL_S)
            except Exception:
                return False
            return True
        return False
    try:
        return bool(client.set(OUTBOX_LOCK, token, nx=True, ex=LOCK_TTL_S))
    except TypeError:
        try:
            client.set(OUTBOX_LOCK, token, ex=LOCK_TTL_S)
        except Exception:
            return False
        return True
    except Exception:
        return False


async def default_deliver(item: dict[str, Any]) -> None:
    """Send one outbox item with the bot that queued it. Not used for the first try."""
    from v4vapp_backend_v2.config.setup import InternalConfig
    from v4vapp_backend_v2.helpers.notification_bot import (
        NotificationBot,
        NotificationNotSetupError,
    )

    name = str(item.get("bot_name") or "")
    try:
        bot = NotificationBot(name=name) if name else NotificationBot()
    except NotificationNotSetupError as exc:
        raise OutboxNotReady(str(exc)) from exc
    text = str(item.get("text") or "")
    if item.get("append_machine"):
        text = f"{text} {InternalConfig().local_machine_name}"
    kwargs = dict(item.get("kwargs") or {})
    await bot.bot.send_message(chat_id=item.get("chat_id"), text=text, **kwargs)


async def notification_outbox_loop(shutdown_event: asyncio.Event | None = None) -> None:
    """Drain on a lock so one process per server delivers the shared outbox."""
    token = uuid.uuid4().hex
    while True:
        if shutdown_event is not None and shutdown_event.is_set():
            return
        try:
            client = redis_client()
            if _hold_lock(client, token):
                await drain_once(Outbox(client), deliver=default_deliver)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _warn_dependency_once(f"Telegram outbox drain failed: {exc}")
        try:
            if shutdown_event is None:
                await asyncio.sleep(DRAIN_POLL_S)
            else:
                await asyncio.wait_for(shutdown_event.wait(), DRAIN_POLL_S)
                return
        except TimeoutError:
            continue
        except asyncio.CancelledError:
            raise


def start_notification_outbox(
    shutdown_event: asyncio.Event | None = None,
) -> asyncio.Task | None:
    """Start one drainer task in this process. Other processes only enqueue."""
    global _drainer_task
    if not drainer_enabled():
        return None
    if _drainer_task is not None and not _drainer_task.done():
        return _drainer_task
    _drainer_task = asyncio.create_task(
        notification_outbox_loop(shutdown_event),
        name="telegram_outbox",
    )
    return _drainer_task
