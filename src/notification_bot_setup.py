import asyncio
import sys
from pathlib import Path
from typing import Annotated

import typer

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.notification_bot import (
    NotificationBadTokenError,
    NotificationBot,
    NotificationNotSetupError,
)

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
TELEGRAM_CONFIG_PATH = Path(InternalConfig.base_config_path, "telegram_bot_config.json")
ICON = "🏆"
app = typer.Typer(
    name="Notification Bot Setup",
    help=(
        f"Setup a Notification Bot for use with v4vapp_backend_v2\n\n"
        f"Stored bot configurations: {NotificationBot.names()}"
    ),
)


# CLI Commands
@app.command()
def setup(
    token: str = typer.Argument(..., help="Notification Bot API token"),
    # name: str = typer.Option("telegram_bot", help="Name of the bot matching the token"),
):
    """
    Setup the bot with a Notification API token, put the bot token in the command line.

    After running this command, re-run with the Run command to start the bot.

    Either just send a message to the bot (to get the private chat_id which will be a +ve int)
    or add the bot to a group and send a message to the group, the bot will get the group chat_id
    and will be able to send messages to the group if you create a new config with that
    group chat_id.

    This will create a new config file in the config directory with the name of the group
    chat or the bot name.

    """
    bot = NotificationBot(token=token)
    try:
        bot.config.name = asyncio.run(bot.get_bot_name())
    except NotificationBadTokenError as e:
        typer.echo(f"Bad Token: failed to setup bot: {e}")
        raise typer.Exit()
    bot.save_config()
    typer.echo(f"Bot configured with token. Config saved to {bot.n_bot_config_file}")
    typer.echo("Start your bot by sending /start to it in Notification")


@app.command()
def notify(
    message: str = typer.Argument(..., help="Message to send to the bot"),
    name: Annotated[
        str,
        typer.Argument(
            help=f"Name of the bot to send the message to choose from: {NotificationBot.names()}"
        ),
    ] = NotificationBot.names_list()[0],
):
    """Send a notification to the Notification bot"""
    try:
        bot = NotificationBot(name=name)
    except NotificationNotSetupError as e:
        typer.echo(e)
        raise typer.Exit()

    async def send_notification():
        await bot.send_message(text=message)

    asyncio.run(send_notification())
    typer.echo(f"Notification sent: {message}")


@app.command()
def run(
    name: Annotated[
        str,
        typer.Argument(help=f"Name of the bot to send run: {NotificationBot.names()}"),
    ] = NotificationBot.names_list()[0],
):
    """Run the Notification bot listener"""
    try:
        bot = NotificationBot(name=name)
    except NotificationNotSetupError as e:
        typer.echo(e)
        raise typer.Exit()

    typer.echo(f"Bot ({bot.config.name}) is running... Send /start to your bot in Notification")
    typer.echo("Press Ctrl+C to stop the bot")
    typer.echo(f"Chat ID: {bot.config.chat_id}")

    async def main():
        await bot.run_bot()

    asyncio.run(main())


if __name__ == "__main__":
    try:
        logger.name = "notification_bot_setup"
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
