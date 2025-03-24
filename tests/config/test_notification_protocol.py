import asyncio
import logging
from random import randint
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from v4vapp_backend_v2.config.notification_protocol import (
    BotNotification,
    EmailNotification,
    NotificationProtocol,
)
from v4vapp_backend_v2.config.setup import Config, InternalConfig

# Test data from JSON
TEST_JSON = {
    "level": "INFO",
    "human_time": "05:32:23.466 Wed 19 Mar",
    "message": "🆅 💰 Attempted 10,015 bfx-lnd1 → WalletOfSatoshi.com ❌ (1706)",
    "timestamp": "2025-03-19T05:32:23.466081+00:00",
    "logger": "lnd_monitor_v2",
    "module": "lnd_monitor_v2",
    "function": "track_events",
    "line": 93,
    "thread_name": "MainThread",
    "notification": True,
    "HtlcEvent": {
        "htlc_id": 1706,
        "from_channel": "bfx-lnd1",
        "to_channel": "WalletOfSatoshi.com",
        "amount": 10015,
        "fee": 0,
        "fee_percent": 0,
        "fee_ppm": 0,
    },
}


# # Fixture for a mock Config object (minimal, since not used in _send_notification)
# @pytest.fixture
# def mock_config():
#     return MagicMock(spec=Config)


# Fixture for a mock InternalConfig object
# @pytest.fixture
# def mock_internal_config():
#     internal_config = MagicMock(spec=InternalConfig)
#     internal_config.notification_loop = asyncio.new_event_loop()
#     return internal_config


@pytest.fixture
def mock_internal_config():
    with patch("v4vapp_backend_v2.config.notification_protocol.InternalConfig") as mock:
        internal_config = MagicMock(spec=InternalConfig)
        internal_config.notification_loop = asyncio.new_event_loop()
        mock.return_value = internal_config
        yield internal_config


# Fixture for a LogRecord based on JSON test data
@pytest.fixture
def mock_log_record():
    record = logging.LogRecord(
        name=TEST_JSON["logger"],
        level=logging.INFO,
        pathname=f"{TEST_JSON['module']}.py",
        lineno=TEST_JSON["line"],
        msg=TEST_JSON["message"],
        args=(),
        exc_info=None,
    )
    record.__dict__.update(
        {
            "levelno": logging.INFO,
            "notification": TEST_JSON["notification"],
            "HtlcEvent": TEST_JSON["HtlcEvent"],
        }
    )
    record.silent = False  # Default to non-silent
    return record


@pytest.mark.asyncio
async def test_notification_protocol_not_implemented():
    """
    Test that a subclass of NotificationProtocol raises
    NotImplementedError if _send_notification is not implemented.
    """

    class IncompleteNotification(NotificationProtocol):
        pass

    notifier = IncompleteNotification()
    with pytest.raises(
        NotImplementedError, match="Subclasses must implement this method"
    ):
        await notifier._send_notification(None, "test", None)


@pytest.mark.asyncio
async def test_bot_notification_send_message(mock_log_record):
    """Test BotNotification sends a message from JSON data."""
    with patch(
        "v4vapp_backend_v2.config.notification_protocol.NotificationBot"
    ) as mock_bot:
        bot_instance = mock_bot.return_value
        bot_instance.send_message = AsyncMock()

        notifier = BotNotification()
        await notifier._send_notification(TEST_JSON["message"], mock_log_record)

        bot_instance.send_message.assert_awaited_once_with(TEST_JSON["message"])


@pytest.mark.asyncio
async def test_bot_notification_silent_mode(mock_log_record):
    """Test BotNotification respects silent mode with JSON data."""
    mock_log_record.silent = True
    with patch(
        "v4vapp_backend_v2.config.notification_protocol.NotificationBot"
    ) as mock_bot:
        bot_instance = mock_bot.return_value
        bot_instance.send_message = AsyncMock()

        notifier = BotNotification()
        await notifier._send_notification(TEST_JSON["message"], mock_log_record)

        bot_instance.send_message.assert_awaited_once_with(
            TEST_JSON["message"], disable_notification=True
        )


@pytest.mark.asyncio
async def test_email_notification_not_implemented(mock_log_record):
    """Test EmailNotification raises NotImplementedError."""
    notifier = EmailNotification()
    with pytest.raises(
        NotImplementedError, match="Email notification is not implemented yet"
    ):
        await notifier._send_notification(TEST_JSON["message"], mock_log_record)


def test_send_notification_loop_running(mock_internal_config, mock_log_record):
    """Test send_notification schedules a task when the event loop is running."""
    # Ensure is_running() returns True when called
    mock_internal_config.notification_loop.is_running = MagicMock(return_value=True)
    notifier = BotNotification()

    with patch.object(notifier, "_send_notification", new=AsyncMock()):
        with patch("asyncio.create_task") as _:
            notifier.send_notification(TEST_JSON["message"], mock_log_record)
            notifier._send_notification.assert_called_once()


def test_send_notification_loop_not_running(mock_internal_config, mock_log_record):
    """Test send_notification runs the coroutine when the event loop is not running."""
    mock_internal_config.notification_loop.is_running = MagicMock(return_value=False)
    notifier = BotNotification()

    with patch.object(notifier, "_send_notification", new=AsyncMock()):
        notifier.send_notification(TEST_JSON["message"], mock_log_record)
        notifier._send_notification.assert_called_once()


def test_send_notification_error_handling(
    mock_internal_config, mock_log_record, caplog
):
    """Test send_notification logs an error if _send_notification fails."""
    mock_internal_config.notification_loop.is_running = MagicMock(return_value=False)
    notifier = BotNotification()
    rand_int = randint(1, 999999)
    error_text = f"Test error {rand_int}"
    with patch.object(
        notifier, "_send_notification", side_effect=Exception(error_text)
    ):
        with patch(
            "v4vapp_backend_v2.config.notification_protocol.logger.warning",
            new=MagicMock(),
        ) as mock_log:
            caplog.set_level(logging.WARNING)
            notifier.send_notification(TEST_JSON["message"], mock_log_record)
            assert error_text in mock_log.call_args[0][0]
