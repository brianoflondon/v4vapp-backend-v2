import asyncio
import json
import random
import re
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from telegram import Bot
from telegram.error import BadRequest, InvalidToken, RetryAfter, TimedOut

from v4vapp_backend_v2.config.setup import InternalConfig, NotificationBotConfig, logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    is_markdown,
    sanitize_filename,
    sanitize_markdown_v1,
    sanitize_markdown_v2,
    truncate_text,
)

BOT_CONFIG_EXTENSION = "_n_bot_config.json"

shutdown_event = asyncio.Event()


class NotificationNotSetupError(Exception):
    pass


class NotificationBadTokenError(NotificationNotSetupError):
    pass


class NotificationBot:
    bot: Bot
    config: NotificationBotConfig
    _message_history: deque = deque(maxlen=1000)
    _logged_patterns: set = set()  # Track patterns that have been logged as ignored

    def __init__(
        self,
        token: str = "",
        name: str = "",
    ):
        if token:
            self.config = NotificationBotConfig(token=token)
            self.bot = Bot(token=token)
            return
        if name:
            self.name = name
            self.load_config()
            self.bot = Bot(token=self.config.token)
            return
        if self.names_list():
            if InternalConfig().config.logging.default_notification_bot_name:
                self.name = InternalConfig().config.logging.default_notification_bot_name
            else:
                self.name = self.names_list()[0]
            self.load_config()
            self.bot = Bot(token=self.config.token)
            return
        raise NotificationNotSetupError(f"No token or name set for bot. {name} not found")

    @property
    def n_bot_config_file(self) -> Path:
        return Path(InternalConfig.base_config_path, f"{self.name}{BOT_CONFIG_EXTENSION}")

    @classmethod
    def names(cls) -> str:
        return ", ".join(cls.names_list())

    @classmethod
    def config_paths(cls) -> list[Path]:
        """Get all config paths in the base config path."""
        config_paths = [
            f
            for f in Path(InternalConfig.base_config_path).glob(f"*{BOT_CONFIG_EXTENSION}")
            if f.is_file()
        ]
        return config_paths

    @classmethod
    def names_list(cls) -> list:
        return [config.name.replace(BOT_CONFIG_EXTENSION, "") for config in cls.config_paths()]

    @classmethod
    def ids_list(cls) -> list:
        chat_ids: set = set()
        for config in cls.config_paths():
            try:
                with open(config, "r") as f:
                    config_data = json.load(f)
                    if "chat_id" in config_data:
                        chat_ids.add(config_data["chat_id"])
            except (json.JSONDecodeError, KeyError):
                logger.error(f"Error reading chat_id from {config}")
                continue
        return list(chat_ids)

    @classmethod
    def ids_names(cls) -> dict[int, str]:
        chat_ids: dict[int, str] = {}
        for config in cls.config_paths():
            try:
                with open(config, "r") as f:
                    config_data = json.load(f)
                    if "chat_id" in config_data and "name" in config_data:
                        chat_ids[config_data["chat_id"]] = config_data["name"]
            except (json.JSONDecodeError, KeyError):
                logger.error(f"Error reading chat_id from {config}")
                continue
        return chat_ids

    async def get_bot_name(self):
        try:
            async with self.bot:
                return self.bot.name
        except InvalidToken as e:
            raise NotificationBadTokenError(e)
        except Exception as e:
            raise NotificationNotSetupError(e)

    def _clean_message_history(self):
        """Remove messages older than 60 seconds from the history and update logged patterns."""
        now = datetime.now(tz=timezone.utc)
        sixty_seconds_ago = now - timedelta(seconds=60)
        # Collect patterns of messages that will remain
        remaining_patterns = set()
        # Remove old messages and track which patterns are still in history
        while self._message_history and self._message_history[0]["timestamp"] < sixty_seconds_ago:
            self._message_history.popleft()
        # Add patterns of remaining messages to remaining_patterns
        for msg in self._message_history:
            remaining_patterns.add(msg["pattern"])
        # Update _logged_patterns to only include patterns still in history
        self._logged_patterns.intersection_update(remaining_patterns)

    def _check_message_pattern(self, text: str) -> bool:
        """
        Check if the last 40 characters of the message have been sent more than 5 times
        in the last 60 seconds. Returns True if the message should be sent, False if it should be ignored.
        Logs a warning only the first time a pattern is ignored.
        """
        num_chars = 20
        self._clean_message_history()
        last_n_chars = text[-num_chars:] if len(text) >= num_chars else text

        # Count messages with the same last 20 characters in the last 60 seconds
        pattern_count = sum(1 for msg in self._message_history if msg["pattern"] == last_n_chars)

        if pattern_count >= 5:
            # Log only if this pattern hasn't been logged before
            if last_n_chars not in self._logged_patterns:
                logger.warning(
                    f"Ignoring message with pattern '{last_n_chars}' - already sent {pattern_count} times in last 60s",
                    extra={"notification": True, "pattern": last_n_chars},
                )
                self._logged_patterns.add(last_n_chars)
            return False
        return True

    @staticmethod
    def strip_ansi(text: str) -> str:
        """Strip ANSI escape sequences from text."""
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        return ansi_escape.sub("", text)

    async def send_message(self, text: str, retries: int = 3, **kwargs: Any) -> None:
        """Send text messages, with pattern-based filtering and rate limiting."""
        if not self.bot or not self.config.chat_id:
            raise NotificationNotSetupError(
                "No chat ID set. Please start the bot first by sending /start"
            )

        text_original = text
        text = self.strip_ansi(text)
        text = truncate_text(text, 300)
        text = text + f"- {InternalConfig().local_machine_name}"

        # Check if the message should be sent based on pattern frequency
        if not self._check_message_pattern(text):
            return

        # Add the message to history before attempting to send
        self._message_history.append(
            {
                "timestamp": datetime.now(tz=timezone.utc),
                "pattern": text[-20:] if len(text) >= 20 else text,
            }
        )

        text_v2 = None
        if re.search(r"no_preview", text):
            kwargs["disable_web_page_preview"] = True
            text = text.rstrip("no_preview").strip()
        if is_markdown(text):
            kwargs["parse_mode"] = "Markdown"
            text = sanitize_markdown_v1(text)
        attempt = 0
        while attempt < retries:
            try:
                ans = await self.bot.send_message(chat_id=self.config.chat_id, text=text, **kwargs)
                return
            except TimedOut as e:
                attempt += 1
                if attempt >= retries:
                    logger.error(
                        f"Error sending [ {text} ] after {retries} retries: {e}",
                        extra={"notification": False, "error": e},
                    )
                    return
                logger.warning(
                    f"Timed out while sending message {text}. Retrying {attempt}/{retries}...",
                    extra={"notification": False},
                )
                await asyncio.sleep(2**attempt + random.random())
            except RetryAfter as e:
                retry_after = int(e.retry_after)
                logger.warning(
                    f"Flood control exceeded. Retrying in {retry_after} seconds...",
                    extra={"notification": False},
                )
                await asyncio.sleep(retry_after)
            except BadRequest:
                attempt += 1
                try:
                    text_v2 = sanitize_markdown_v2(text_original)
                    await self.bot.send_message(
                        chat_id=self.config.chat_id, text=text_v2, parse_mode="MarkdownV2"
                    )
                    logger.info(
                        "Using Markdown v2 for message",
                        extra={
                            "text_original": text_original,
                            "sanitized_v2": text_v2,
                            "notification_text": text,
                        },
                    )
                    return
                except Exception as e:
                    attempt += 1
                    text_v2 = text_v2 or "text_v2 not created"
                    text_original = text_original or "text_original not available"
                    logger.exception(
                        f"Second Error sending [ {text} ]: {e} with Markdown v2",
                        extra={
                            "error": e,
                            "notification_text": text,
                            "notification": False,
                            "text_original": text_original,
                            "sanitized_v2": text_v2,
                        },
                    )
                    logger.info("Problem in Notification bot Markdown V2")
                    return
            except Exception as e:
                attempt += 1
                text_v2 = text_v2 or "text_v2 not created"
                text_original = text_original or "text_original not available"
                logger.exception(
                    f"Error sending [ {text} ]: {e}",
                    extra={
                        "notification": False,
                        "error": e,
                        "notification_text": text,
                        "text_original": text_original,
                        "sanitized_v2": text_v2,
                    },
                )
                logger.info(f"Problem in Notification bot {e}")
                return
        return

    async def handle_update(self, update):
        if update.message:
            logger.info(f"Received message from chat ID: {update.message.chat_id}")
            logger.info(f"Chat ID: {update.message.chat_id}")
            logger.info(f"Message: {update.message.text}")
            new_config = None
            if update.message.chat.title:
                logger.info(f"Group chat name: {update.message.chat.title}")
                if update.message.chat_id in NotificationBot.ids_list():
                    logger.info(
                        f"Chat ID: {update.message.chat_id} already exists in config files"
                    )
                    old_name = NotificationBot.ids_names().get(update.message.chat_id)
                    if old_name != update.message.chat.title:
                        logger.info(
                            f"Chat name has changed from {old_name} to {update.message.chat.title}"
                        )
                        new_config = NotificationBotConfig(
                            token=self.config.token,
                            chat_id=update.message.chat_id,
                            name=update.message.chat.title,
                        )
                else:
                    new_config = NotificationBotConfig(
                        token=self.config.token,
                        chat_id=update.message.chat_id,
                        name=update.message.chat.title,
                    )
                    logger.info(f"Adding chat ID: {update.message.chat_id} to config files")

                if new_config:
                    self.save_config(new_config)
                    new_bot_name = sanitize_filename(new_config.name)
                    logger.info(
                        f"Sending a notification to {update.message.chat.title} new bot name: {new_bot_name}",
                        extra={"notification": True, "bot_name": new_bot_name},
                    )
                else:
                    bot_name = sanitize_filename(update.message.chat.title)
                    logger.info(
                        f"Chat ID: {bot_name} already exists in config files",
                        extra={"notification": True, "bot_name": bot_name},
                    )
            else:
                logger.info("No group chat name available")
            if self.config.chat_id == 0:
                self.config.chat_id = update.message.chat_id
                self.save_config()

            if update.message.text == "/start":
                await self.send_menu()
            elif update.message.text == "/menu":
                await self.send_menu()
            elif update.message.text == "/status":
                await self.send_message("Bot is running")

    async def send_menu(self):
        menu_text = """
        Welcome to the Bot Menu!
        Available commands:
        /start - Start the bot
        /menu - Show this menu
        /status - Get bot status
        """
        await self.bot.send_message(chat_id=self.config.chat_id, text=menu_text)

    async def run_bot(self):
        try:
            async with self.bot:
                offset = None
                while True:
                    updates = await self.bot.get_updates(offset=offset, timeout=60)
                    for update in updates:
                        await self.handle_update(update)
                        offset = update.update_id + 1
        except InvalidToken as e:
            raise NotificationBadTokenError(e)
        except asyncio.CancelledError:
            logger.info("Bot shutdown gracefully.")
        except Exception as e:
            raise NotificationNotSetupError(e)

    def load_config(self) -> None:
        config_file = self.n_bot_config_file
        if config_file.exists():
            with open(config_file, "r") as f:
                self.config = NotificationBotConfig.model_validate(json.load(f))
        else:
            raise NotificationNotSetupError(
                f"No configuration file found. {self.name=} {config_file=} missing"
            )

    def save_config(self, new_config: NotificationBotConfig | None = None) -> None:
        if new_config:
            new_config_file = f"{new_config.name}{BOT_CONFIG_EXTENSION}"
            new_config_file = sanitize_filename(new_config_file)
            new_config_path = Path(InternalConfig.base_config_path, new_config_file)
            if new_config_path.exists():
                logger.info(f"Config file {new_config_file} already exists.")
                return
            with open(new_config_path, "w") as f:
                json.dump(new_config.model_dump(), f)
            return

        if not self.config.name:
            raise NotificationNotSetupError("No name set for bot.")
        self.name = self.config.name
        with open(self.n_bot_config_file, "w") as f:
            json.dump(self.config.model_dump(), f)
        return
