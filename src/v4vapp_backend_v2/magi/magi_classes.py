from decimal import Decimal

from pydantic import BaseModel

from v4vapp_backend_v2.hive_models.account_name_type import AccNameType

ICON = "🧙‍♂️"


class MagiBTCBalanceError(Exception):
    """Custom exception for errors related to fetching Magi BTC balance."""


class MagiBTCBalance(BaseModel):
    account: AccNameType
    balance_sats: Decimal
    error: str | None = None

    @property
    def balance_msats(self) -> Decimal:
        return self.balance_sats * Decimal(1000)


class MagiBTCTransferEvent(BaseModel):
    from_addr: AccNameType
    to_addr: AccNameType
    amount: Decimal
    indexer_block_height: int
    indexer_tx_hash: str
    indexer_ts: str
    indexer_id: int

    @property
    def log_str(self) -> str:
        return (
            f"{ICON} Transfer {self.from_addr:>18} -> {self.to_addr:>18} "
            f"{self.amount:,.0f} sats (indexer_id={self.indexer_id})"
        )
