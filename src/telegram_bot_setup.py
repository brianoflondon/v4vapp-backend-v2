import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Annotated, Optional

import telegram
import typer

from lnd_monitor_v2 import InternalConfig, logger

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
TELEGRAM_CONFIG_PATH = Path(InternalConfig.base_config_path, "telegram_bot.json")
ICON = "🏆"
app = typer.Typer()


class TelegramBot:
    def __init__(self, token: str):
        self.bot = telegram.Bot(token)
        self.chat_id = None

    async def send_message(self, text: str):
        if self.chat_id:
            await self.bot.send_message(chat_id=self.chat_id, text=text)
        else:
            typer.echo("No chat ID set. Please start the bot first by sending /start")

    async def handle_update(self, update):
        if update.message:
            if self.chat_id is None:
                config = load_config()
                save_config(config["token"], update.message.chat_id)
            self.chat_id = update.message.chat_id
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
        await self.bot.send_message(chat_id=self.chat_id, text=menu_text)


# Load or save config
def load_config() -> Optional[dict]:
    if TELEGRAM_CONFIG_PATH.exists():
        with open(TELEGRAM_CONFIG_PATH, "r") as f:
            return json.load(f)
    return None


def save_config(token: str, chat_id: int | None = None) -> dict:
    config = {"token": token}
    if chat_id:
        config["chat_id"] = chat_id
    TELEGRAM_CONFIG_PATH.parent.mkdir(exist_ok=True)
    with open(TELEGRAM_CONFIG_PATH, "w") as f:
        json.dump(config, f)
    return config


# CLI Commands
@app.command()
def setup(token: str = typer.Argument(..., help="Telegram Bot API token")):
    """Setup the bot with a Telegram API token"""
    save_config(token)
    typer.echo(f"Bot configured with token. Config saved to {TELEGRAM_CONFIG_PATH}")
    typer.echo("Start your bot by sending /start to it in Telegram")


@app.command()
def notify(
    message: str = typer.Argument(..., help="Message to send to the bot"),
):
    """Send a notification to the Telegram bot"""
    config = load_config()
    if not config or "token" not in config:
        typer.echo("Bot not configured. Please run 'setup' command first.")
        raise typer.Exit()

    bot = TelegramBot(config["token"])
    bot.chat_id = config.get("chat_id")

    async def send_notification():
        await bot.send_message(message)

    asyncio.run(send_notification())
    typer.echo(f"Notification sent: {message}")


@app.command()
def run():
    """Run the Telegram bot listener"""
    config = load_config()
    if not config or "token" not in config:
        typer.echo("Bot not configured. Please run 'setup' command first.")
        raise typer.Exit()

    bot = TelegramBot(config["token"])
    typer.echo("Bot is running... Send /start to your bot in Telegram")

    async def main():
        async with bot.bot:
            # Get updates with long polling
            offset = None
            while True:
                updates = await bot.bot.get_updates(offset=offset, timeout=30)
                for update in updates:
                    logger.info(f"Received update: {update}")
                    await bot.handle_update(update)
                    offset = update.update_id + 1

    asyncio.run(main())


if __name__ == "__main__":

    try:
        logger.name = "telegram_bot_setup"
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
