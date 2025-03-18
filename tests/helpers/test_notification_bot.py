import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from telegram.error import InvalidToken

from v4vapp_backend_v2.config.setup import InternalConfig, NotificationBotConfig
from v4vapp_backend_v2.helpers.general_purpose_funcs import is_markdown
from v4vapp_backend_v2.helpers.notification_bot import (
    BOT_CONFIG_EXTENSION,
    NotificationBadTokenError,
    NotificationBot,
    NotificationNotSetupError,
)

# Mark the test module as async
pytestmark = pytest.mark.asyncio


# Mock InternalConfig for consistent base_config_path
@pytest.fixture
def mock_internal_config(tmp_path):
    class MockInternalConfig:
        base_config_path = tmp_path

    with patch(
        "v4vapp_backend_v2.helpers.notification_bot.InternalConfig", MockInternalConfig
    ):
        yield MockInternalConfig


@pytest.fixture
def mock_bot():
    bot = MagicMock()
    bot.send_message = AsyncMock()
    bot.get_updates = AsyncMock(return_value=[])
    bot.name = "@TestBot"
    return bot


@pytest.fixture
def mock_config(tmp_path):
    config = NotificationBotConfig(token="valid_token", chat_id=12345, name="testbot")
    config_file = tmp_path / f"testbot{BOT_CONFIG_EXTENSION}"
    with open(config_file, "w") as f:
        json.dump(config.model_dump(), f)
    return config


@pytest.fixture
def notification_bot(mock_internal_config, mock_bot, mock_config):
    with patch("v4vapp_backend_v2.helpers.notification_bot.Bot", return_value=mock_bot):
        bot = NotificationBot(name="testbot")
        bot.config = mock_config
        return bot


# Test initialization with token
def test_init_with_token(mock_internal_config, mock_bot):
    with patch("v4vapp_backend_v2.helpers.notification_bot.Bot", return_value=mock_bot):
        bot = NotificationBot(token="valid_token")
        assert bot.config.token == "valid_token"
        assert bot.bot == mock_bot


# Test initialization with name and existing config
def test_init_with_name(mock_internal_config, mock_bot, mock_config):
    with patch("v4vapp_backend_v2.helpers.notification_bot.Bot", return_value=mock_bot):
        bot = NotificationBot(name="testbot")
        assert bot.config.token == "valid_token"
        assert bot.name == "testbot"
        assert bot.bot == mock_bot


# Test initialization failure when no token or name provided and no configs exist
def test_init_no_token_no_name_no_configs(mock_internal_config, mock_bot):
    with patch(
        "v4vapp_backend_v2.helpers.notification_bot.Bot", return_value=mock_bot
    ), patch(
        "v4vapp_backend_v2.helpers.notification_bot.NotificationBot.names_list",
        return_value=[],
    ):
        with pytest.raises(
            NotificationNotSetupError, match="No token or name set for bot."
        ):
            NotificationBot()


# Test get_bot_name
async def test_get_bot_name(notification_bot):
    assert await notification_bot.get_bot_name() == "@TestBot"


# Test get_bot_name with invalid token
async def test_get_bot_name_invalid_token(notification_bot):
    notification_bot.bot.__aenter__.side_effect = InvalidToken("Invalid token")
    with pytest.raises(NotificationBadTokenError):
        await notification_bot.get_bot_name()


# Test send_message with markdown
async def test_send_message_markdown(notification_bot):
    with patch(
        "v4vapp_backend_v2.helpers.general_purpose_funcs.is_markdown", return_value=True
    ):
        await notification_bot.send_message("**bold text**")
        notification_bot.bot.send_message.assert_called_once_with(
            chat_id=12345, text="**bold text**", parse_mode="Markdown"
        )


# Test send_message without markdown
async def test_send_message_no_markdown(notification_bot):
    with patch(
        "v4vapp_backend_v2.helpers.general_purpose_funcs.is_markdown",
        return_value=False,
    ):
        await notification_bot.send_message("plain text")
        notification_bot.bot.send_message.assert_called_once_with(
            chat_id=12345, text="plain text"
        )


# Test send_message with no chat_id
async def test_send_message_no_chat_id(notification_bot):
    notification_bot.config.chat_id = 0
    with pytest.raises(NotificationNotSetupError, match="No chat ID set"):
        await notification_bot.send_message("test message")


# Test handle_update sets chat_id
async def test_handle_update_sets_chat_id(notification_bot, tmp_path):
    notification_bot.config.chat_id = 0
    update = MagicMock()
    update.message.chat_id = 67890
    update.message.text = "random text"

    with patch(
        "v4vapp_backend_v2.helpers.notification_bot.NotificationBot.save_config"
    ) as mock_save:
        await notification_bot.handle_update(update)
        assert notification_bot.config.chat_id == 67890
        mock_save.assert_called_once()


# Test handle_update with /start
async def test_handle_update_start(notification_bot):
    update = MagicMock()
    update.message.text = "/start"
    with patch(
        "v4vapp_backend_v2.helpers.notification_bot.NotificationBot.send_menu",
        AsyncMock(),
    ) as mock_send_menu:
        await notification_bot.handle_update(update)
        mock_send_menu.assert_called_once()


# Test send_menu
async def test_send_menu(notification_bot):
    await notification_bot.send_menu()
    notification_bot.bot.send_message.assert_called_once_with(
        chat_id=12345,
        text="\n        Welcome to the Bot Menu!\n        Available commands:\n        /start - Start the bot\n        /menu - Show this menu\n        /status - Get bot status\n        ",
    )


# Test run_bot with invalid token
async def test_run_bot_invalid_token(notification_bot):
    notification_bot.bot.get_updates.side_effect = InvalidToken("Invalid token")
    with pytest.raises(NotificationBadTokenError):
        await notification_bot.run_bot()


# Test load_config
def test_load_config(notification_bot, mock_config):
    notification_bot.load_config()
    assert notification_bot.config.token == "valid_token"
    assert notification_bot.config.chat_id == 12345


# Test load_config_no_file
def test_load_config_no_file(mock_internal_config, mock_bot):
    with patch("v4vapp_backend_v2.helpers.notification_bot.Bot", return_value=mock_bot):
        with pytest.raises(
            NotificationNotSetupError, match="No configuration file found"
        ):
            bot = NotificationBot(name="nonexistent")
            with pytest.raises(
                NotificationNotSetupError, match="No configuration file found"
            ):
                bot.load_config()


# Test save_config
def test_save_config(notification_bot, tmp_path):
    notification_bot.save_config()
    config_file = tmp_path / f"testbot{BOT_CONFIG_EXTENSION}"
    with open(config_file, "r") as f:
        saved_config = json.load(f)
    assert saved_config["token"] == "valid_token"
    assert saved_config["chat_id"] == 12345


# Test names_list
def test_names_list(mock_internal_config, tmp_path):
    config_file = tmp_path / f"testbot{BOT_CONFIG_EXTENSION}"
    with open(config_file, "w") as f:
        json.dump({"token": "valid_token"}, f)
    assert NotificationBot.names_list() == ["testbot"]


# Test names
def test_names(mock_internal_config, tmp_path):
    config_file = tmp_path / f"testbot{BOT_CONFIG_EXTENSION}"
    with open(config_file, "w") as f:
        json.dump({"token": "valid_token"}, f)
    assert NotificationBot.names() == "testbot"
