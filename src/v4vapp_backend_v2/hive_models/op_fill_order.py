from datetime import datetime

from nectar.amount import Amount
from pydantic import Field

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate

from .amount_pyd import AmountPyd


class FillOrder(OpBase):
    current_orderid: int
    current_owner: str
    current_pays: AmountPyd
    open_orderid: int
    open_owner: str
    open_pays: AmountPyd
    timestamp: datetime
    debit_conv: CryptoConv = CryptoConv()
    credit_conv: CryptoConv = CryptoConv()

    completed_order: bool = Field(
        default=False,
        description="True if the order was completed, False if it was partially filled",
    )

    log_internal: str = Field(
        default="",
        exclude=True,
        description="Holds the internal log string for the log and notification log operations",
    )

    def __init__(self, **data: dict):
        super().__init__(**data)
        # Debit conv should match the debit side in the ledger (open_pays, HIVE received)
        if TrackedBaseModel.last_quote.sats_usd == 0:
            logger.warning(
                f"FillOrder: {self.current_orderid} {self.open_orderid} last_quote.sats_usd is 0",
                extra={
                    "notification": False,
                    "last_quote": TrackedBaseModel.last_quote,
                    "fill_order": self,
                },
            )
        self.debit_conv = CryptoConv(
            conv_from=self.open_pays.unit,  # HIVE
            value=self.open_pays.amount_decimal,  # 25.052 HIVE
            converted_value=self.current_pays.amount_decimal,  # 6.738 HBD
            quote=TrackedBaseModel.last_quote,
            timestamp=self.timestamp,
        )
        # Credit conv should match the credit side in the ledger (current_pays, HBD given)
        self.credit_conv = CryptoConv(
            conv_from=self.current_pays.unit,  # HBD
            value=self.current_pays.amount_decimal,  # 6.738 HBD
            converted_value=self.open_pays.amount_decimal,  # 25.052 HIVE
            quote=TrackedBaseModel.last_quote,
            timestamp=self.timestamp,
        )
        # Set the log_internal string to None to force it to be generated
        self.log_internal = self._log_internal()

    def _log_internal(self) -> str:
        if self.log_internal:
            return self.log_internal
        check_str = self.check_open_orders()  # forces self.completed_orders to be set
        current_pays_str = self.current_pays.fixed_width_str(15)
        open_pays_str = self.open_pays.fixed_width_str(15)
        if self.current_pays.symbol == "HIVE":
            rate = self.open_pays.amount_decimal / self.current_pays.amount_decimal
        else:
            rate = self.current_pays.amount_decimal / self.open_pays.amount_decimal
        rate_str = f"{rate:.3f}"  # HIVE/HBD
        icon = "📈"
        self.log_internal = (
            f"{icon} "
            f"{current_pays_str} --> {open_pays_str} "
            f"{self.open_owner} filled order for "
            f"{self.current_owner} "
            f"{check_str} {rate_str:>8}"
        )
        return self.log_internal

    @property
    def is_watched(self) -> bool:
        """
        Check if the order is watched.

        Returns:
            bool: True if the order is watched, False otherwise.
        """
        if FillOrder.watch_users:
            if (
                self.current_owner in FillOrder.watch_users
                or self.open_owner in FillOrder.watch_users
            ):
                return True
        return False

    @property
    def log_str(self) -> str:
        return f"{self._log_internal()} {self.link}"

    @property
    def notification_str(self) -> str:
        return f"{self._log_internal()} {self.markdown_link}"

    @property
    def ledger_str(self) -> str:
        """
        Returns a string representation of the ledger entry for the transaction.
        This string is formatted to include the transaction details, including
        the current and open pays amounts, the order IDs, and the rate.
        """
        # return _log_internal but strip the icon from the start
        return_str = self._log_internal()
        return_str = return_str.replace("📈", "")
        return return_str.strip()

    def check_open_orders(self) -> str:
        """
        Checks and updates the status of open orders based on the current order's details.
        This method determines if there is an open order associated with the current order ID
        or the open order ID. If an open order is found, it calculates the remaining amount
        after processing the current transaction and updates the order's status accordingly.
        Returns:
            str: A message indicating the status of the order. Possible messages include:
                - The remaining amount and order ID if the order is partially filled.
                - A confirmation that the order has been fully filled.
                - The open order ID if no matching open order is found.
        Attributes:
            self.current_orderid (str): The ID of the current order being processed.
            self.open_orderid (str): The ID of the open order to check.
            self.open_owner (str): The owner of the open order.
            self.current_pays (Amount): The amount being paid in the current transaction.
            self.open_pays (Amount): The amount being paid in the open order.
            self.completed_order (bool): A flag indicating whether the order has been fully completed.
        Notes:
            - If the remaining amount of the open order is greater than zero, the order is
              marked as incomplete and the remaining amount is returned.
            - If the remaining amount is zero or less, the order is removed from the list
              of open orders and marked as completed.
        """
        open_order = LimitOrderCreate.open_order_ids.get(self.current_orderid, None)
        if not open_order:
            open_order = LimitOrderCreate.open_order_ids.get(self.open_orderid, None)
        if open_order is not None:
            if self.open_owner == open_order.owner:  # This is when we fill someone else's order
                amount_remaining = Amount(str(open_order.amount_remaining))
                amount_remaining -= Amount(self.current_pays.model_dump())
            else:
                amount_remaining = Amount(str(open_order.amount_remaining))
                amount_remaining -= Amount(self.open_pays.model_dump())
            open_order.amount_remaining = AmountPyd.model_validate(amount_remaining)
            if amount_remaining.amount > 0:
                self.completed_order = False
                return f"Remaining {open_order.amount_remaining} {open_order.orderid}"
            else:
                LimitOrderCreate.open_order_ids.pop(open_order.orderid)
                self.completed_order = True
                return f"✅ Order {open_order.orderid} has been filled (xs {amount_remaining})"
        return f"id {self.open_orderid}"

    def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion for the transaction.

        If the subclass has a `conv` object, update it with the latest quote.
        If a quote is provided, it sets the conversion to the provided quote.
        If no quote is provided, it uses the last quote to set the conversion.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, uses the last quote.
        """
        quote = quote or TrackedBaseModel.last_quote
        self.debit_conv = CryptoConv(
            conv_from=self.open_pays.unit,  # HIVE
            value=self.open_pays.amount_decimal,  # 25.052 HIVE
            converted_value=self.current_pays.amount_decimal,  # 6.738 HBD
            quote=quote,
        )
        # Credit conv should match the credit side in the ledger (current_pays, HBD given)
        self.credit_conv = CryptoConv(
            conv_from=self.current_pays.unit,  # HBD
            value=self.current_pays.amount_decimal,  # 6.738 HBD
            converted_value=self.open_pays.amount_decimal,  # 25.052 HIVE
            quote=quote,
        )
