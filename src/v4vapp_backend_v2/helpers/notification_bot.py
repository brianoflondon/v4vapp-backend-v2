import json
from pathlib import Path

from telegram import Bot
from telegram.error import InvalidToken

from v4vapp_backend_v2.config.setup import InternalConfig, NotificationBotConfig, logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    is_markdown,
    sanitize_markdown_v1,
)

BOT_CONFIG_EXTENSION = "_n_bot_config.json"


class NotificationNotSetupError(Exception):
    pass


class NotificationBadTokenError(NotificationNotSetupError):
    pass


class NotificationBot:
    bot: Bot
    config: NotificationBotConfig

    model_config = {"arbitrary_types_allowed": True}

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
            self.name = self.names_list()[0]
            self.load_config()
            self.bot = Bot(token=self.config.token)
            return
        raise NotificationNotSetupError("No token or name set for bot.")

    @property
    def n_bot_config_file(self) -> Path:
        return Path(
            InternalConfig.base_config_path, f"{self.name}{BOT_CONFIG_EXTENSION}"
        )

    @classmethod
    def names(cls) -> str:
        return ", ".join(cls.names_list())

    @classmethod
    def names_list(cls) -> list:
        config_paths = [
            f
            for f in Path(InternalConfig.base_config_path).glob(
                f"*{BOT_CONFIG_EXTENSION}"
            )
            if f.is_file()
        ]
        return [
            config.name.replace(BOT_CONFIG_EXTENSION, "") for config in config_paths
        ]

    async def get_bot_name(self):
        try:
            async with self.bot:
                return self.bot.name
        except InvalidToken as e:
            raise NotificationBadTokenError(e)
        except Exception as e:
            raise NotificationNotSetupError(e)

    async def send_message(self, text: str, **kwargs):
        """Use this method to send text messages, chat_id will be provided.

        Args:
            chat_id (:obj:`int` | :obj:`str`): |chat_id_channel|
            text (:obj:`str`): Text of the message to be sent. Max
                :tg-const:`telegram.constants.MessageLimit.MAX_TEXT_LENGTH` characters after
                entities parsing.
            parse_mode (:obj:`str`): |parse_mode|
            entities (Sequence[:class:`telegram.MessageEntity`], optional): Sequence of special
                entities that appear in message text, which can be specified instead of
                :paramref:`parse_mode`.

                .. versionchanged:: 20.0
                    |sequenceargs|
            link_preview_options (:obj:`LinkPreviewOptions`, optional): Link preview generation
                options for the message. Mutually exclusive with
                :paramref:`disable_web_page_preview`.

                .. versionadded:: 20.8

            disable_notification (:obj:`bool`, optional): |disable_notification|
            protect_content (:obj:`bool`, optional): |protect_content|

                .. versionadded:: 13.10

            reply_markup (:class:`InlineKeyboardMarkup` | :class:`ReplyKeyboardMarkup` | \
                :class:`ReplyKeyboardRemove` | :class:`ForceReply`, optional):
                Additional interface options. An object for an inline keyboard, custom reply
                keyboard, instructions to remove reply keyboard or to force a reply from the user.
            message_thread_id (:obj:`int`, optional): |message_thread_id_arg|

                .. versionadded:: 20.0
            reply_parameters (:class:`telegram.ReplyParameters`, optional): |reply_parameters|

                .. versionadded:: 20.8
            business_connection_id (:obj:`str`, optional): |business_id_str|

                .. versionadded:: 21.1
            message_effect_id (:obj:`str`, optional): |message_effect_id|

                .. versionadded:: 21.3
            allow_paid_broadcast (:obj:`bool`, optional): |allow_paid_broadcast|

                .. versionadded:: 21.7

        Keyword Args:
            allow_sending_without_reply (:obj:`bool`, optional): |allow_sending_without_reply|
                Mutually exclusive with :paramref:`reply_parameters`, which this is a convenience
                parameter for

                .. versionchanged:: 20.8
                    Bot API 7.0 introduced :paramref:`reply_parameters` |rtm_aswr_deprecated|

                .. versionchanged:: 21.0
                    |keyword_only_arg|
            reply_to_message_id (:obj:`int`, optional): |reply_to_msg_id|
                Mutually exclusive with :paramref:`reply_parameters`, which this is a convenience
                parameter for

                .. versionchanged:: 20.8
                    Bot API 7.0 introduced :paramref:`reply_parameters` |rtm_aswr_deprecated|

                .. versionchanged:: 21.0
                    |keyword_only_arg|
            disable_web_page_preview (:obj:`bool`, optional): Disables link previews for links in
                this message. Convenience parameter for setting :paramref:`link_preview_options`.
                Mutually exclusive with :paramref:`link_preview_options`.

                .. versionchanged:: 20.8
                    Bot API 7.0 introduced :paramref:`link_preview_options` replacing this
                    argument. PTB will automatically convert this argument to that one, but
                    for advanced options, please use :paramref:`link_preview_options` directly.

                .. versionchanged:: 21.0
                    |keyword_only_arg|

        Returns:
            :class:`telegram.Message`: On success, the sent message is returned.

        Raises:
            :exc:`ValueError`: If both :paramref:`disable_web_page_preview` and
                :paramref:`link_preview_options` are passed.
            :class:`telegram.error.TelegramError`: For other errors.

        """
        if self.bot and self.config.chat_id:
            text = self.truncate_to_3000(text)
            try:
                if is_markdown(text):
                    kwargs["parse_mode"] = "Markdown"  # Use V1
                    if text.endswith("no_preview"):
                        kwargs["disable_web_page_preview"] = (
                            True  # Optional: disable link previews
                        )
                        text = text.rstrip("no_preview").strip()
                    sanitized_text = sanitize_markdown_v1(text)
                    try:
                        await self.bot.send_message(
                            chat_id=self.config.chat_id, text=sanitized_text, **kwargs
                        )
                    except Exception as e:
                        logger.warning(
                            f"Markdown V1 error: {e}. Sending without parse_mode. Text: {text}",
                            extra={"notification": False},
                        )
                        try:
                            await self.bot.send_message(
                                chat_id=self.config.chat_id, text=sanitized_text
                            )
                        except Exception as e:
                            logger.exception(
                                f"Error sending [ {text} ] for second time: {e}",
                                extra={"notification": False},
                            )
                else:
                    await self.bot.send_message(
                        chat_id=self.config.chat_id, text=text, **kwargs
                    )
            except Exception as e:
                logger.exception(
                    f"Error sending [ {text} ]: {e}", extra={"notification": False}
                )
        else:
            raise NotificationNotSetupError(
                "No chat ID set. Please start the bot first by sending /start"
            )

    async def handle_update(self, update):
        if update.message:
            if self.config.chat_id == 0:
                self.config.chat_id = update.message.chat_id
                self.save_config()
            if update.message.text == "/start":
                await self.send_menu()
            elif update.message.text == "/menu":
                await self.send_menu()
            elif update.message.text == "/status":
                await self.send_message("Bot is running")

    def truncate_to_3000(self, text):
        # Check if string exceeds 3000 characters
        if len(text) > 3000:
            # Truncate to 3000 characters and add ellipsis to indicate truncation
            truncated_text = text[:3000] + "..."
            return truncated_text
        else:
            return text

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
        except Exception as e:
            raise NotificationNotSetupError(e)

    def load_config(self) -> None:
        config_file = self.n_bot_config_file
        if config_file.exists():
            with open(config_file, "r") as f:
                self.config = NotificationBotConfig.model_validate(json.load(f))
        else:
            raise NotificationNotSetupError("No configuration file found.")

    def save_config(self) -> None:
        """
        Saves the given bot configuration to a file.
        Args:
            bot_config (NotificationBotConfig): The bot configuration to save.
        Returns:
            NotificationBotConfig: The saved bot configuration.
        """
        if not self.config.name:
            raise NotificationNotSetupError("No name set for bot.")
        with open(self.n_bot_config_file, "w") as f:
            json.dump(self.config.model_dump(), f)
