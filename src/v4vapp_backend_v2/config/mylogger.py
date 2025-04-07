import datetime as dt
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, OrderedDict, override

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
        if not hasattr(record, "levelno"):
            logger.warning(
                f"No levelno: {record}", extra={"notification": False, "record": record}
            )
            record.levelno = logging.INFO
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
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


@dataclass
class ErrorCode:
    code: Any
    start_time: datetime = datetime.now(tz=timezone.utc)

    def __init__(self, code: Any):
        self.code = code
        self.start_time = datetime.now(tz=timezone.utc)
        logger.info(f"Error code set: {self.code}")

    @property
    def elapsed_time(self) -> timedelta:
        return datetime.now(tz=timezone.utc) - self.start_time


class CustomNotificationHandler(logging.Handler):
    """
    Custom logging handler to send log messages to Notification with special
    handling for error codes.

    Attributes:
        error_codes (dict[Any, ErrorCode]): A dictionary to keep track of error codes
        and their details.

    Methods:
        emit(record: logging.LogRecord):
            Processes a log record and sends a formatted log message to Notification.
            Handles special cases for error codes, including clearing and tracking
            elapsed time.

        send_notification_message(message: str):
            Asynchronously sends a message to Notification.
            This method needs to be implemented to integrate with the Notification API.
    """

    error_codes: ClassVar[dict[Any, ErrorCode]] = {}
    sender: NotificationProtocol = BotNotification()

    @override
    def emit(self, record: logging.LogRecord):
        if not hasattr(record, "levelno"):
            logger.warning(
                f"No levelno: {record}", extra={"notification": False, "record": record}
            )
            record.levelno = logging.INFO
        log_message = record.getMessage()
        if self.error_codes:
            logger.debug(f"Error codes: {self.error_codes}")
        # Do something special here with error codes or details
        if self.error_codes and hasattr(record, "error_code_clear"):
            error_code_obj = self.error_codes.get(record.error_code_clear)
            elapsed_time = error_code_obj.elapsed_time if error_code_obj else timedelta(seconds=33)
            elapsed_time_str = timedelta_display(elapsed_time)
            log_message_clear = (
                f"✅ Error code {record.error_code_clear} cleared after {elapsed_time_str}"
            )
            if record.error_code_clear in self.error_codes:
                self.error_codes.pop(record.error_code_clear)
            else:
                logger.warning(
                    f"Error code not found in error_codes {record.error_code_clear}",
                    extra={"notification": False},
                )
            logger.info(log_message_clear, extra={"notification": True, "record": record})
            self.sender.send_notification(log_message, record)
            return
        if hasattr(record, "error_code"):
            if record.error_code not in self.error_codes:
                self.sender.send_notification(log_message, record, alert_level=5)
                self.error_codes[record.error_code] = ErrorCode(code=record.error_code)
            else:
                # Do not send the same error code to Notification
                pass
        # Default case
        else:
            self.sender.send_notification(log_message, record, alert_level=10)


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
        if not hasattr(record, "levelno"):
            logger.warning(
                f"No levelno: {record}", extra={"notification": False, "record": record}
            )
            record.levelno = logging.INFO

        if hasattr(record, "name"):
            package_name = record.name.split(".")[0]
            if package_name in InternalConfig().config.logging.log_notification_silent:
                # If the module is in the suppression list, do not send to Notification
                return False

        # If the record.notification flag is set to False,
        # do not send the message to Notification
        if hasattr(record, "notification") and not record.notification:
            return False

        # Send everything with level WARNING or higher to Notification
        # unless the record.notification flag is set to False
        return record.levelno >= logging.WARNING or (
            hasattr(record, "notification") and record.notification
        )


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
        if not hasattr(record, "levelno"):
            logger.warning(
                f"No levelno: {record}", extra={"notification": False, "record": record}
            )
            record.levelno = logging.INFO

        return record.levelno <= logging.INFO


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
        if not hasattr(record, "levelno"):
            logger.warning(
                f"No levelno: {record}", extra={"notification": False, "record": record}
            )
            record.levelno = logging.INFO
        return record.levelno > logging.DEBUG
