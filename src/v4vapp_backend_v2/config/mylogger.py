import asyncio
import datetime as dt
import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, override

import httpx

from v4vapp_backend_v2.config import InternalConfig, logger

LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class MyJSONFormatter(logging.Formatter):
    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: (
                msg_val
                if (msg_val := always_fields.pop(val, None)) is not None
                else getattr(record, val, None)
            )
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


@dataclass
class ErrorCode:
    code: Any
    start_time: datetime = datetime.now(tz=timezone.utc)

    @property
    def elapsed_time(self) -> timedelta:
        return datetime.now(tz=timezone.utc) - self.start_time


class CustomTelegramHandler(logging.Handler):
    """
    Custom logging handler to send log messages to Telegram with special
    handling for error codes.

    Attributes:
        error_codes (dict[Any, ErrorCode]): A dictionary to keep track of error codes
        and their details.

    Methods:
        emit(record: logging.LogRecord):
            Processes a log record and sends a formatted log message to Telegram.
            Handles special cases for error codes, including clearing and tracking
            elapsed time.

        send_telegram_message(message: str):
            Asynchronously sends a message to Telegram.
            This method needs to be implemented to integrate with the Telegram API.
    """

    error_codes: dict[Any, ErrorCode] = {}

    def format(self, record: logging.LogRecord) -> str:
        """
        Formats the log record.

        Args:
            record (logging.LogRecord): The log record to be formatted.

        Returns:
            str: The formatted log message.
        """
        return record.message

    def emit(self, record: logging.LogRecord):
        log_message = self.format(record)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # No event loop in the current thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            # Start the event loop in a new thread
            threading.Thread(target=loop.run_forever, daemon=True).start()

        # Do something special here with error codes or details
        if hasattr(record, "error_code") and hasattr(record, "error_code_clear"):
            if record.error_code in self.error_codes:
                elapsed_time = self.error_codes[record.error_code].elapsed_time
                log_message = (
                    f"Error code {record.error_code} "
                    f"cleared after {elapsed_time} {log_message}"
                )
                self.error_codes.pop(record.error_code)
                asyncio.run_coroutine_threadsafe(
                    self.send_telegram_message(log_message), loop
                )
            else:
                log_message = f"Error code {record.error_code} not found in error_codes {log_message}"
                asyncio.run_coroutine_threadsafe(
                    self.send_telegram_message(log_message), loop
                )
            return
        if hasattr(record, "error_code"):
            if record.error_code not in self.error_codes:
                asyncio.run_coroutine_threadsafe(
                    self.send_telegram_message(log_message), loop
                )
                self.error_codes[record.error_code] = ErrorCode(code=record.error_code)
            else:
                # Do not send the same error code to Telegram
                pass
        else:
            asyncio.run_coroutine_threadsafe(
                self.send_telegram_message(log_message), loop
            )

    async def send_telegram_message(self, message: str):
        # Assign the configuration to a local variable
        config = InternalConfig().config

        url = (
            f"{config.tailscale.notification_server}."
            f"{config.tailscale.tailnet_name}:"
            f"{config.tailscale.notification_server_port}/send_notification/"
        )
        alert_level = 1
        params: Dict = {
            "notify": message,
            "alert_level": alert_level,
            "room_id": config.telegram.chat_id,
        }
        try:
            async with httpx.AsyncClient() as client:
                _ = await client.get(url, params=params, timeout=60)
        except httpx.RequestError as ex:
            logger.error(
                f"An error occurred while sending the message: {ex}",
                extra={
                    "telegram": False,
                },
            )
        except Exception as ex:
            logger.error(
                f"An error occurred while sending the message: {ex}",
                extra={
                    "telegram": False,
                },
            )
        pass
        # raise NotImplementedError


class TelegramFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        """
        Filter method for the logger.

        Args:
            record (logging.LogRecord): The log record to be filtered.

        Returns:
            bool | logging.LogRecord: True if the log record level is
                                      greater than or equal to WARNING,
                                      or if the log record has a
                                      'telegram' attribute and it is True.
                                      Otherwise, returns False.
        """
        # If the record.telegram flag is set to False,
        # do not send the message to Telegram
        if hasattr(record, "telegram") and not record.telegram:
            return False

        # Send everything with level WARNING or higher to Telegram
        # unless the record.telegram flag is set to False
        return record.levelno >= logging.WARNING or (
            hasattr(record, "telegram") and record.telegram
        )


class NonErrorFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.levelno <= logging.INFO
