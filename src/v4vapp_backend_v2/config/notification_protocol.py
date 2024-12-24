from logging import LogRecord
from typing import Dict, Protocol

import httpx

from v4vapp_backend_v2.config.setup import Config, InternalConfig, logger


class NotificationProtocol(Protocol):
    def send_notification(
        self, message: str, record: LogRecord, alert_level: int = 1
    ) -> None: ...


class TelegramNotification:
    def send_notification(
        self, message: str, record: LogRecord, alert_level: int = 1
    ) -> None:
        # Send notification to Telegram
        # Assign the configuration to a local variable
        internal_config = InternalConfig()
        _config: Config = internal_config.config

        internal_config.notification_loop.run_until_complete(
            self._send_to_telegram(_config, message, record, alert_level)
        )

    async def _send_to_telegram(
        self,
        _config: Config,
        message: str,
        record: LogRecord,
        alert_level: int = 1,
    ) -> None:
        # Send notification to Telegram
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
            async with httpx.AsyncClient() as client:
                ans = await client.get(url, params=params, timeout=60)
                if ans.status_code != 200:
                    logger.warning(
                        f"An error occurred while sending the message: {ans.text}",
                        extra={
                            "notification": False,
                            "failed_message": message,
                        },
                    )
                else:
                    logger.debug(f"Sent message: {message}")

        except Exception as ex:
            logger.warning(
                f"An error occurred while sending the message: {ex}",
                extra={
                    "notification": False,
                    "failed_message": message,
                },
            )


class EmailNotification:
    def send_notification(
        self, message: str, record: LogRecord, alert_level: int = 1
    ) -> None:
        raise NotImplementedError("Email notification is not implemented yet.")
