import asyncio
from decimal import Decimal

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.conversion.exchange_protocol import (
    BaseExchangeAdapter,
    ExchangeConnectionError,
    ExchangeMinimums,
    ExchangeOrderResult,
)
from v4vapp_backend_v2.magi.magi_balances import get_magi_btc_balance_by_account


class MagiAdapter(BaseExchangeAdapter):
    """Exchange adapter for MAGI BTC balances backed by Hive account state."""

    def __init__(self, server_name: str | None = None, testnet: bool = False):
        super().__init__(testnet=testnet)
        self.server_name = server_name

    @property
    def exchange_name(self) -> str:
        return "MagiSwap"

    def _resolve_server_account(self) -> str:
        if self.server_name:
            return self.server_name

        config = InternalConfig()
        server_account = config.config.server_account
        if server_account is None:
            raise ExchangeConnectionError(
                "No server Hive account configured for MagiSwap balance lookup"
            )
        return server_account.name

    def get_min_order_requirements(self, base_asset: str, quote_asset: str) -> ExchangeMinimums:
        raise ExchangeConnectionError("MagiSwap order requirements are not implemented")

    def get_balance(self, asset: str) -> Decimal:
        server_name = self._resolve_server_account()

        try:
            magi_balance = asyncio.run(get_magi_btc_balance_by_account(server_name))
        except Exception as exc:
            raise ExchangeConnectionError(
                f"Failed to fetch MagiSwap balance for {server_name}: {exc}"
            ) from exc

        asset_key = asset.upper()
        if asset_key == "BTC":
            return magi_balance.balance_sats / Decimal("100000000")
        if asset_key == "SATS":
            return magi_balance.balance_sats
        if asset_key == "MSATS":
            return magi_balance.balance_msats

        raise ExchangeConnectionError(
            f"MagiSwap only supports BTC/SATS/MSATS balance lookup, not {asset}"
        )

    def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
        raise ExchangeConnectionError("MagiSwap price lookup is not implemented")

    def market_sell(
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        raise NotImplementedError("MagiSwap market_sell is not implemented")

    def market_buy(
        self,
        base_asset: str,
        quote_asset: str,
        quantity: Decimal,
        client_order_id: str | None = None,
    ) -> ExchangeOrderResult:
        raise NotImplementedError("MagiSwap market_buy is not implemented")
