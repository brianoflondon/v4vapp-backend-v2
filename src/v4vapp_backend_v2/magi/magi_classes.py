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
            f"{self.amount:,.0f} sats (indexer_id={self.indexer_id}) {self.link or ''}"
        )

    @property
    def link(self) -> str:
        """
        Generates a link to the Hive block explorer for the transaction ID.

        Returns:
            str: A formatted string containing the link to the Hive block explorer.
        """
        url = f"https://hivehub.dev/tx/{self.indexer_tx_hash}"
        return url
