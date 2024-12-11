import datetime as dt
import json
import logging
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


def timedelta_display(td: timedelta) -> str:
    """
    Convert a timedelta object to a string in the format "HHh MMm SSs".

    Args:
        td (timedelta): The timedelta object to be converted.

    Returns:
        str: The formatted string representing the timedelta.
    """
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    seconds = td.seconds % 60
    return f"{hours:02}h {minutes:02}m {seconds:02}s"


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
        if self.error_codes:
            print("error_codes")
            print(self.error_codes)
        # Do something special here with error codes or details
        if (
            self.error_codes
            and hasattr(record, "error_code")
            and hasattr(record, "error_code_clear")
        ):
            print(self.error_codes)
            elapsed_time = self.error_codes[record.error_code].elapsed_time
            elapsed_time_str = timedelta_display(elapsed_time)
            log_message = (
                f"✅ Error code {record.error_code} "
                f"cleared after {elapsed_time_str} {log_message}"
            )
            if record.error_code in self.error_codes:
                self.error_codes.pop(record.error_code)
                self.send_telegram_message(log_message, record, alert_level=5)
            else:
                logger.warning(
                    f"Error code not found in error_codes {record.error_code}",
                    extra={"telegram": False},
                )
                self.send_telegram_message(log_message, record, alert_level=5)
            return
        if hasattr(record, "error_code"):
            if record.error_code not in self.error_codes:
                self.send_telegram_message(log_message, record, alert_level=5)
                self.error_codes[record.error_code] = ErrorCode(code=record.error_code)
                print(self.error_codes)
            else:
                # Do not send the same error code to Telegram
                pass
        # Default case
        else:
            self.send_telegram_message(log_message, record, alert_level=1)

    def send_telegram_message(
        self, message: str, record: logging.LogRecord, alert_level: int = 1
    ) -> None:
        """
        Sends a message to a Telegram chat via a notification server.

        This method sends a message to a specified Telegram chat by calling an
        external notification server API. It handles the creation and management
        of the asyncio event loop required for making the asynchronous HTTP request.

        Args:
            message (str): The message to be sent to the Telegram chat.

        Raises:
            httpx.RequestError: If an error occurs while making the HTTP request.
            Exception: For any other exceptions that occur during the process.

        Note:
            The configuration for the notification server and Telegram chat is
            retrieved from the InternalConfig class.
        """

        async def call_notification_api(message: str):
            try:
                async with httpx.AsyncClient() as client:
                    ans = await client.get(url, params=params, timeout=60)
                    if ans.status_code != 200:
                        logger.warning(
                            f"An error occurred while sending the message: {ans.text}",
                            extra={
                                "telegram": False,
                                "failed_message": message,
                            },
                        )
                    else:
                        logger.debug(f"Sent message: {message}")

            except Exception as ex:
                logger.warning(
                    f"An error occurred while sending the message: {ex}",
                    extra={
                        "telegram": False,
                        "failed_message": message,
                    },
                )

        # Assign the configuration to a local variable
        internal_config = InternalConfig()
        _config = internal_config.config

        url = (
            f"{_config.tailscale.notification_server}."
            f"{_config.tailscale.tailnet_name}:"
            f"{_config.tailscale.notification_server_port}/send_notification/"
        )
        params: Dict = {
            "notify": message,
            "alert_level": alert_level,
            "room_id": _config.telegram.chat_id,
        }
        try:
            formatter = MyJSONFormatter()
            logger.debug(
                f"NOTIFICATION SENT -> {message}",
                extra={
                    "telegram": False,
                    "details": formatter._prepare_log_dict(record),
                },
            )
            internal_config.notification_loop.run_until_complete(
                call_notification_api(message)
            )

        except Exception as ex:
            logger.error(
                f"An error occurred while sending the message: {ex}",
                extra={
                    "telegram": False,
                    "failed_message": message,
                },
            )
        logger.debug(
            f"Finished emit, loop is running: "
            f"{internal_config.notification_loop.is_running()}"
        )

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
