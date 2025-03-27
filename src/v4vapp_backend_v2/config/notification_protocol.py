"""

BotNotification: A class for sending notifications via a bot.
TelegramNotification: A deprecated class for sending notifications via Telegram.

    Sends a notification with the given message, log record, and alert level.

NotificationProtocol._send_notification(self, _config: Config, message: str,
    Asynchronously sends a notification (to be implemented by subclasses).

BotNotification._send_notification(self, _config: Config, message: str,
    Asynchronously sends a notification using a bot.

TelegramNotification._send_notification(self, _config: Config, message: str,

EmailNotification._send_notification(self, _config: Config, message: str,

"""

import asyncio
import logging
import threading
from logging import LogRecord
from typing import Protocol

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.notification_bot import NotificationBot


class NotificationProtocol(Protocol):
    def send_notification(
        self, message: str, record: LogRecord, alert_level: int = 1
    ) -> None:
        internal_config = InternalConfig()
        internal_config.notification_lock = True
        loop = internal_config.notification_loop
        if loop.is_closed() or not loop.is_running():
            # Recreate the event loop if it is closed
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            internal_config.notification_loop = loop  # Update the stored loop

        if "levelno" not in record.__dict__:
            record.__dict__["levelno"] = logging.INFO

        try:
            # If the loop is running, schedule the task using the correct loop
            if loop.is_running():
                try:
                    logger.info(
                        f"✉️ Notification Thread: {threading.get_ident()} loop already running"
                    )
                    asyncio.run_coroutine_threadsafe(
                        self._send_notification(message, record, alert_level), loop
                    )
                except Exception as ex:
                    logger.exception(ex, extra={"notification": False})
            else:
                # Run the task in the loop and handle shutdown gracefully
                loop.run_until_complete(
                    self._run_with_resilience(message, record, alert_level)
                )
        except Exception as ex:
            logger.exception(ex, extra={"notification": False})
            logger.warning(
                f"An error occurred while sending the message: {ex} {message}",
                extra={
                    "notification": False,
                    "failed_message": message,
                },
            )
        finally:
            internal_config.notification_lock = False

    async def _run_with_resilience(
        self, message: str, record: LogRecord, alert_level: int
    ):
        try:
            logger.info(
                f"📩 Notification Thread: {threading.get_ident()} sending: {message[:30]}"
            )
            await self._send_notification(message, record, alert_level)

        except asyncio.CancelledError:
            logger.warning("Notification task was cancelled.")
        except Exception as ex:
            logger.exception(
                f"Error in notification task: {ex}", extra={"notification": False}
            )
        finally:
            InternalConfig().notification_lock = False

    async def _send_notification(
        self,
        message: str,
        record: LogRecord,
        alert_level: int = 1,
    ) -> None:
        raise NotImplementedError("Subclasses must implement this method")


class BotNotification(NotificationProtocol):
    async def _send_notification(
        self,
        message: str,
        record: LogRecord,
        alert_level: int = 1,
    ) -> None:
        """
        Asynchronously sends a notification message using the NotificationBot.
        Set the extra attribute 'silent' to True in the log record to disable notifications.

        Args:
            message (str): The message to be sent.
            record (LogRecord): The log record associated with the notification.
            alert_level (int, optional): The alert level of the notification. Defaults to 1.

        Returns:
            None
        """
        bot = NotificationBot()
        # Using Silent as the attribute name to avoid conflicts with the logging module
        if hasattr(record, "notification_str"):
            message = record.notification_str
        if hasattr(record, "silent") and record.silent:
            await bot.send_message(message, disable_notification=True)
        else:
            await bot.send_message(message)


class EmailNotification(NotificationProtocol):
    async def _send_notification(
        self,
        # _config: Config,
        message: str,
        record: LogRecord,
        alert_level: int = 1,
    ) -> None:
        raise NotImplementedError("Email notification is not implemented yet.")
