import asyncio

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.models.lnd_balance_models import (
    ChannelBalance,
    WalletBalance,
    fetch_balances_from_default,
)


async def get_balances() -> tuple[WalletBalance | None, ChannelBalance | None]:
    """Fetch balances using the shared helper. Returns typed models or (None, None)."""
    return await fetch_balances_from_default()


async def main():
    logger.info("Starting LND opening balance task...")

    wallet_balance, channel_balance = await get_balances()

    if not wallet_balance and not channel_balance:
        logger.warning("No balances available (no default LND node configured or error).")
        return

    if wallet_balance:
        logger.info(
            f"Wallet total: {wallet_balance.total_sats} sats, confirmed: {wallet_balance.confirmed_sats} sats, unconfirmed: {wallet_balance.unconfirmed_sats} sats"
        )
    if channel_balance:
        logger.info(
            f"Channels local: {channel_balance.local_sats} sats, remote: {channel_balance.remote_sats} sats, balance: {channel_balance.balance} sats"
        )


if __name__ == "__main__":
    ic = InternalConfig(config_filename="devhive.config.yaml")
    asyncio.run(main())
