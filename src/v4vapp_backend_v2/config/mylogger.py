import asyncio
import datetime as dt
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import OrderedDict, override

from colorama import Fore, Style

from v4vapp_backend_v2.config.error_code_class import ErrorCode
from v4vapp_backend_v2.config.notification_protocol import BotNotification, NotificationProtocol
from v4vapp_backend_v2.config.setup import InternalConfig, logger

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


def human_readable_datetime_str(dt_obj: datetime) -> str:
    """
    Convert a datetime object to a human-readable string.

    Args:
        dt_obj (datetime): The datetime object to be converted.

    Returns:
        str: The formatted string representing the datetime.
    """
    ms = dt_obj.microsecond // 1000
    return f"{dt_obj:%H:%M:%S}.{ms:03d} {dt_obj:%a %d %b}"


def parse_log_level(level: str | int | None, fallback: int = logging.INFO) -> int:
    """
    Parse a logging level into its numeric value without using
    logging.getLevelName (which can return a string for unknown names).

    Accepts:
      - int -> returned unchanged
      - numeric string like "20" -> parsed to int
      - name like "INFO" or "debug" -> looked up in logging._nameToLevel
      - common synonym "WARN" is accepted for "WARNING"

    On unknown inputs, returns `fallback` (default: logging.INFO).
    """
    # Already numeric
    if isinstance(level, int):
        return level
    if level is None:
        return fallback

    s = str(level).strip()

    # Numeric string
    try:
        return int(s)
    except (ValueError, TypeError):
        pass

    name = s.upper()
    if name == "WARN":
        name = "WARNING"

    name_to_level = getattr(logging, "_nameToLevel", None)
    if name_to_level is not None and name in name_to_level:
        return name_to_level[name]

    # Unknown -> fallback
    return fallback


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
        """
        Formats the given log record into a JSON string.
        Runs for every line written to the log
        This method attempts to prepare a dictionary representation of the log record
        using the _prepare_log_dict method and then serializes it to a JSON string.
        If an exception occurs during this process, it prints an error message and
        falls back to the parent class's format method.
        Args:
            record (logging.LogRecord): The log record to be formatted.
        Returns:
            str: The formatted log message as a JSON string, or the result of the
                 parent class's format method if an error occurs.
        """

        # Use module-level json_default helper so it's testable and robust
        def json_default(o):
            return _json_default(o)

        try:
            message = self._prepare_log_dict(record)
            ans_str = json.dumps(message, default=json_default)
            if hasattr(record, "error_code"):
                error_code = record.error_code  # type: ignore[attr-defined]
                error_state = InternalConfig().error_codes.get(error_code, None)
                if hasattr(record, "re_alert_time"):
                    re_alert_time = record.re_alert_time  # type: ignore[attr-defined]
                else:
                    re_alert_time = timedelta(hours=1)
                if error_state and error_state.check_time_since_last_log(re_alert_time):
                    error_state.reset_last_log_time()
                    return ans_str
                elif error_state is None:
                    return ans_str
                else:
                    return ""

            return ans_str
        except Exception as e:
            print(f"Error formatting log record: {e}")
            return super().format(record)

    def _prepare_log_dict(self, record: logging.LogRecord):
        """
        Prepares a dictionary representation of the given log record for structured logging.

        This method constructs a dictionary containing key log information, including a human-readable
        timestamp, ISO-formatted timestamp, message, and optional exception or stack information.
        It incorporates custom format keys from self.fmt_keys, adds any extra attributes from the
        record not in the built-in attributes, and ensures 'human_time' is positioned after 'level'
        in the resulting OrderedDict.

        Args:
            record (logging.LogRecord): The log record to process.

        Returns:
            OrderedDict: A dictionary with the prepared log data, ordered with 'human_time' after 'level'.
        """
        human_readable_str = human_readable_datetime_str(
            dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc)
        )
        always_fields = {
            "message": record.getMessage(),
            "human_time": human_readable_str,
            "timestamp": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).isoformat(),
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

        # Move human_time to the desired position
        if "human_time" in message:
            human_time_value = message.pop("human_time")
            # Insert human_time after level
            new_message = OrderedDict()
            for k, v in message.items():
                new_message[k] = v
                if k == "level":
                    new_message["human_time"] = human_time_value
            message = new_message
        return message


class CustomNotificationHandler(logging.Handler):
    """
    Custom logging handler to send log messages to Notification with special
    handling for error codes.

    To add extra bots to the notification, use the `extra_bot_name` or
    `extra_bot_names` attributes in the log record.
    This handler processes log records and sends formatted log messages to Notification.

    Note: Error code tracking is now handled by ErrorTrackingFilter which runs
    before this handler. This handler only needs to send the notifications.

    Methods:
        emit(record: logging.LogRecord):
            Processes a log record and sends a formatted log message to Notification.

        send_notification_message(message: str):
            Asynchronously sends a message to Notification.
            This method needs to be implemented to integrate with the Notification API.
    """

    sender: NotificationProtocol = BotNotification()

    @override
    def emit(self, record: logging.LogRecord):
        if hasattr(record, "bot_name"):
            bot_name = record.bot_name  # type: ignore[attr-defined]
        else:
            bot_name = InternalConfig().config.logging.default_notification_bot_name

        if InternalConfig.notification_loop:
            pending_tasks = asyncio.all_tasks(loop=InternalConfig.notification_loop)
            if pending_tasks:
                logger.info(f"Pending tasks: {len(pending_tasks)}")

        log_message = record.getMessage()

        # Send the notification - error code tracking is handled by ErrorTrackingFilter
        self.sender.send_notification(log_message, record, bot_name=bot_name)
        self._extra_bots(log_message, record)

    def _extra_bots(self, log_message: str, record: logging.LogRecord) -> None:
        """
        Check if the log record has extra bot names and send notifications.

        Args:
            log_message (str): The log message to send.
            record (logging.LogRecord): The log record to be checked.
        """

        def process_bot_names(bot_names: str | list[str]) -> None:
            """Helper function to process bot names and send notifications."""
            if isinstance(bot_names, str):
                # Single bot name as a string
                self.sender.send_notification(log_message, record, bot_name=bot_names)
            elif isinstance(bot_names, list):
                # Multiple bot names in a list
                for bot_name in bot_names:
                    if isinstance(bot_name, str):
                        self.sender.send_notification(log_message, record, bot_name=bot_name)

        # Check for extra_bot_name
        if hasattr(record, "extra_bot_name") and record.extra_bot_name:  # type: ignore[attr-defined]
            process_bot_names(record.extra_bot_name)  # type: ignore[attr-defined]

        # Check for extra_bot_names
        elif hasattr(record, "extra_bot_names") and record.extra_bot_names:  # type: ignore[attr-defined]
            process_bot_names(record.extra_bot_names)  # type: ignore[attr-defined]


# MARK: Logging filters


class ErrorTrackingFilter(logging.Filter):
    """
    A logging filter that tracks error codes in InternalConfig.error_codes.

    This filter runs on all log records and:
    1. Adds new error codes to InternalConfig.error_codes when `error_code` is in extra
    2. Removes error codes when `error_code_clear` is in extra
    3. Suppresses duplicate log entries based on `re_alert_time` (default 1 hour)

    The filter returns False to suppress duplicate error code logs from being written,
    regardless of the notification setting.
    """

    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        # Check if this record has already been processed by another ErrorTrackingFilter instance
        # This prevents race conditions when multiple handlers have this filter
        if hasattr(record, "_error_tracking_processed"):
            return record._error_tracking_result  # type: ignore[attr-defined]

        # Is this a notifiable error?
        notification = record.notification if hasattr(record, "notification") else True  # type: ignore[attr-defined]

        # Handle error_code_clear first - always allow these through and clear the code
        if hasattr(record, "error_code_clear") and record.error_code_clear:  # type: ignore[attr-defined]
            error_code_clear = record.error_code_clear  # type: ignore[attr-defined]
            if error_code_clear in InternalConfig().error_codes:
                error_code_obj = InternalConfig().error_codes.get(error_code_clear)
                elapsed_time = (
                    error_code_obj.elapsed_time if error_code_obj else timedelta(seconds=0)
                )
                elapsed_time_str = timedelta_display(elapsed_time)
                message = (
                    f"✅ {Fore.WHITE}Error code {error_code_clear} cleared after "
                    f"{elapsed_time_str} original: {error_code_obj.message if error_code_obj else ''}{Style.RESET_ALL}"
                )
                logger.info(
                    message,
                    extra={"notification": notification, "error_code_obj": error_code_obj},
                )
                InternalConfig().error_codes.pop(error_code_clear, clear_message=message)
            record._error_tracking_processed = True  # type: ignore[attr-defined]
            record._error_tracking_result = True  # type: ignore[attr-defined]
            return True  # Allow the clear message through

        # Handle error_code tracking
        if hasattr(record, "error_code") and record.error_code:  # type: ignore[attr-defined]
            error_code = record.error_code  # type: ignore[attr-defined]

            # Get re_alert_time from record or use default of 1 hour
            if hasattr(record, "re_alert_time"):
                re_alert_time = record.re_alert_time  # type: ignore[attr-defined]
            else:
                re_alert_time = timedelta(hours=1)

            if error_code not in InternalConfig().error_codes:
                # New error code - add it (triggers MongoDB persistence) and allow the log through
                error_code_obj = ErrorCode(code=error_code, message=record.getMessage())
                InternalConfig().error_codes.add(error_code_obj)
                logger.error(
                    f"❌ New error: {error_code}",
                    extra={
                        "notification": notification,
                        "error_code_obj": error_code_obj,
                    },
                )
                record._error_tracking_processed = True  # type: ignore[attr-defined]
                record._error_tracking_result = True  # type: ignore[attr-defined]
                return True
            else:
                # Existing error code - check if we should re-alert
                error_state = InternalConfig().error_codes[error_code]
                if error_state.check_time_since_last_log(re_alert_time):
                    # Time to re-alert - reset the timer and allow through
                    error_state.reset_last_log_time()
                    record._error_tracking_processed = True  # type: ignore[attr-defined]
                    record._error_tracking_result = True  # type: ignore[attr-defined]
                    return True
                else:
                    # Suppress this log - too soon since last log
                    record._error_tracking_processed = True  # type: ignore[attr-defined]
                    record._error_tracking_result = False  # type: ignore[attr-defined]
                    return False

        # No error_code handling needed - allow through
        record._error_tracking_processed = True  # type: ignore[attr-defined]
        record._error_tracking_result = True  # type: ignore[attr-defined]
        return True


class NotificationFilter(logging.Filter):
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
                                      'notification' attribute and it is True.
                                      Otherwise, returns False.
        """
        ic = InternalConfig()
        if hasattr(record, "name"):
            package_name = record.name.split(".")[0]
            if package_name in ic.config.logging.log_notification_silent:
                # If the module is in the suppression list, do not send to Notification
                return False

        # If the record.notification flag is set to False,
        # do not send the message to Notification
        if hasattr(record, "notification") and not record.notification:  # type: ignore[attr-defined]
            return False

        # Send everything with level WARNING or higher to Notification
        # unless the record.notification flag is set to False
        if record.levelno >= logging.WARNING or (
            hasattr(record, "notification") and record.notification  # type: ignore[attr-defined]
        ):
            return True
        return False


class NonErrorFilter(logging.Filter):
    """
    A logging filter that allows only non-error log records (i.e., log records
    with a level less than or equal to INFO).

    This is referenced in the logging configuration json file.

    Methods:
        filter(record: logging.LogRecord) -> bool | logging.LogRecord:
            Determines if the given log record should be logged. Returns True
            if the log level is less than or equal to INFO, otherwise False.
    """

    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.levelno <= logging.INFO


class ConsoleLogFilter(logging.Filter):
    """
    A logging filter that allows only log records with a level greater than debug.

    This is referenced in the logging configuration json file.

    Methods:
        filter(record: logging.LogRecord) -> bool | logging.LogRecord:
            Determines if the given log record should be logged. Returns True
            if the log level is more than debug, otherwise False.
    """

    # cached value for the configured console level (parsed to int)
    _cached_levelno: int = logging.INFO

    @classmethod
    def refresh_cached_level(cls) -> None:
        """Refresh the cached level from the current config. Call this after
        reloading/changing config if you need the filter to pick up the new value."""
        cls._cached_levelno = parse_log_level(
            InternalConfig().config.logging.console_log_level, fallback=logging.INFO
        )

    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        # Initialize cached level on first call (or if None)
        if ConsoleLogFilter._cached_levelno is None:
            ConsoleLogFilter.refresh_cached_level()
        return record.levelno >= ConsoleLogFilter._cached_levelno


class AddNotificationBellFilter(logging.Filter):
    """
    A logging filter that adds a notification bell emoji to log messages
    that are warnings or higher, or have the 'notification' attribute set to True.

    Methods:
        filter(record: logging.LogRecord) -> logging.LogRecord:
            Modifies the log record to add a notification bell emoji if
            the log level is WARNING or higher, or if the 'notification'
            attribute is set to True.
    """

    @override
    def filter(self, record: logging.LogRecord) -> logging.LogRecord:
        if record.levelno >= logging.WARNING or (
            hasattr(record, "notification") and record.notification  # type: ignore[attr-defined]
        ):
            if hasattr(record, "msg") and isinstance(record.msg, str):
                record.msg += " 🔔"
            if hasattr(record, "message") and isinstance(record.message, str):
                record.message += " 🔔"

        return record


IGNORE_REPORT_FIELDS = LOG_RECORD_BUILTIN_ATTRS | {
    "notification",
    "notification_str",
    "_error_tracking_processed",
    "_error_tracking_result",
}


def _json_default(o):
    """JSON default handler for structured logs.

    Handles Decimal, bson.Decimal128, and fallbacks safely without raising
    decimal.InvalidOperation or OverflowError for pathological inputs.

    Strategy:
      - If value is bson.Decimal128, convert to Decimal using .to_decimal()
      - If value is Decimal:
          - If NaN or infinite -> return str(o)
          - Try safe float conversion and round to 11 places
          - On conversion error -> fallback to string
      - Otherwise -> fallback to str(o)
    """

    # Lazy import so module doesn't require pymongo/bson unless used
    try:
        from bson.decimal128 import Decimal128  # type: ignore
    except Exception:  # pragma: no cover - environment may not have bson
        Decimal128 = None

    # Support bson.Decimal128 (convert to Decimal then handle)
    if Decimal128 is not None and isinstance(o, Decimal128):
        try:
            o = o.to_decimal()
        except Exception:
            return str(o)

    if isinstance(o, Decimal):
        try:
            # Preserve special values as strings so json doesn't try to use NaN
            if o.is_nan() or o.is_infinite():
                return str(o)
            # Try converting to float; catch errors and detect overflow to inf
            f = float(o)
            import math

            # If conversion produced an infinite float (too large), return string
            if not math.isfinite(f):
                return str(o)
        except (InvalidOperation, OverflowError):
            return str(o)
        # Safe to return rounded float
        return round(f, 11)

    # Final fallback
    return str(o)


class AddJsonDataIndicatorFilter(logging.Filter):
    """
    A logging filter that adds a JSON data indicator to log messages
    that have the 'json_data' attribute set to True.

    Methods:
        filter(record: logging.LogRecord) -> logging.LogRecord:
            Modifies the log record to add a JSON data indicator if
            the 'json_data' attribute is set to True.
    """

    @override
    def filter(self, record: logging.LogRecord) -> logging.LogRecord:
        extra_fields = set(record.__dict__.keys()) - IGNORE_REPORT_FIELDS
        if extra_fields:
            extra_text = " ["
            extra_text += ", ".join(f"{field}" for field in extra_fields)
            extra_text += "]"
            if hasattr(record, "msg") and isinstance(record.msg, str):
                record.msg += extra_text
            if hasattr(record, "message") and isinstance(record.message, str):
                record.message += extra_text
        return record


class NotDebugFilter(logging.Filter):
    """
    A logging filter that allows only log records with a level greater than DEBUG.

    This is referenced in the logging configuration json file.

    Methods:
        filter(record: logging.LogRecord) -> bool | logging.LogRecord:
            Determines if the given log record should be logged. Returns True
            if the log level is more than DEBUG, otherwise False.
    """

    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.levelno > logging.DEBUG
