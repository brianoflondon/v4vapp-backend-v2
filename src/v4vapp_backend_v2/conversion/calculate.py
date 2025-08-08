from datetime import datetime, timedelta, timezone

from nectar.amount import Amount
from pydantic import BaseModel
from tabulate import tabulate

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.process.process_errors import HiveToKeepsatsConversionError


class ConversionResult(BaseModel):
    """
    ConversionResult is a Pydantic model that encapsulates the result of a conversion operation.
    It includes the converted amount, fee, and any additional information related to the conversion.
    """

    quote: QuoteResponse
    from_currency: Currency
    to_currency: Currency
    to_convert: float
    to_convert_conv: CryptoConv
    net_to_receive: float
    net_to_receive_conv: CryptoConv
    fee: float
    fee_conv: CryptoConv
    change: float
    change_conv: CryptoConv
    balance: float = 0.0

    def __str__(self) -> str:
        conversion_data = [
            ["to_convert", f"{self.to_convert:>50,.6f}", str(self.from_currency)],
            [
                "to_convert",
                f"{self.to_convert_conv.value_in(self.to_currency):>50,.3f}",
                str(self.to_currency),
            ],
            ["net_to_receive", f"{self.net_to_receive:>50,.6f}", str(self.from_currency)],
            [
                "net_to_receive",
                f"{self.net_to_receive_conv.value_in(self.to_currency):>50,.3f}",
                str(self.to_currency),
            ],
            ["fee", f"{self.fee:>50,.6f}", str(self.from_currency)],
            ["fee", f"{self.fee_conv.value_in(self.to_currency):>50,.3f}", str(self.to_currency)],
            ["change", f"{self.change:>50,.6f}", str(self.from_currency)],
            ["balance", f"{self.balance:>50,.6f}", str(self.from_currency)],
            ["from_currency", str(self.from_currency), ""],
            ["to_currency", str(self.to_currency), ""],
        ]

        return f"Conversion Details:\n{tabulate(conversion_data, headers=['Parameter', 'Value', 'Unit'], tablefmt='fancy_grid')}"


async def hive_to_keepsats(
    tracked_op: TransferBase,
    quote: QuoteResponse | None = None,
    msats: int = 0,
) -> ConversionResult:
    """
    Converts a Hive or HBD transfer operation to its equivalent value in millisatoshis (msats) using a provided or inferred quote.
    Args:
        tracked_op (TransferBase): The transfer operation containing the original Hive/HBD amount and timestamp.
        quote (QuoteResponse | None, optional): The exchange rate quote to use for conversion. If not provided, the function will determine the appropriate quote based on the transfer timestamp.
        msats (int, optional): If provided and non-zero, this value (in msats) will be used for conversion instead of the Hive/HBD amount from the transfer.
    Returns:
        ConversionResult: The result of the conversion, including the converted amount and relevant metadata.
    Notes:
        - If no quote is provided and the transfer is older than 5 minutes, the nearest historical quote is fetched.
        - If no quote is provided and the transfer is recent, the latest quote is used.
        - If a quote is provided, it is overridden by the latest quote.
        - The conversion can be based either on the original Hive/HBD amount or a specified msats value.
    """
    if quote is None:
        if datetime.now(tz=timezone.utc) - tracked_op.timestamp > timedelta(minutes=5):
            quote = await TrackedBaseModel.nearest_quote(tracked_op.timestamp)
        else:
            await TrackedBaseModel.update_quote()
            quote = TrackedBaseModel.last_quote

    from_currency = Currency(tracked_op.amount.symbol_lower)
    to_currency = Currency.MSATS

    original = tracked_op.amount.amount_decimal

    if msats == 0:
        # Base transfer amount on the inbound Hive/HBD transfer amount
        hive_to_convert_amount = tracked_op.amount.minus_minimum
        to_convert_conv = CryptoConversion(
            amount=hive_to_convert_amount, quote=quote, conv_from=from_currency
        ).conversion
        to_convert = to_convert_conv.value_in(from_currency)

        msats_fee = to_convert_conv.msats_fee
        fee_conv = CryptoConversion(
            value=msats_fee, conv_from=Currency.MSATS, quote=quote
        ).conversion
        fee = fee_conv.value_in(from_currency)

        net_to_receive = to_convert - fee
        if net_to_receive < 0:
            raise HiveToKeepsatsConversionError(
                f"Net sats to receive {net_to_receive} is negative, cannot convert."
            )
        net_to_receive_conv = CryptoConversion(
            value=net_to_receive, quote=quote, conv_from=from_currency
        ).conversion

    else:
        # Use the provided msats but this needs to define the amount to receive, not the
        # amount to convert.
        net_to_receive_conv = CryptoConversion(
            value=msats, quote=quote, conv_from=Currency.MSATS
        ).conversion
        net_to_receive = net_to_receive_conv.value_in(from_currency)

        msats_fee = net_to_receive_conv.msats_fee
        fee_conv = CryptoConversion(
            value=msats_fee, conv_from=Currency.MSATS, quote=quote
        ).conversion
        fee = fee_conv.value_in(from_currency)

        to_convert = net_to_receive + fee
        to_convert_conv = CryptoConversion(
            value=to_convert, conv_from=from_currency, quote=quote
        ).conversion

        hive_to_convert_amount = to_convert_conv.amount(from_currency)

    if to_convert < 0:
        raise HiveToKeepsatsConversionError(
            f"Net Hive to receive {to_convert} is negative, cannot convert."
        )

    change = original - (net_to_receive + fee)
    change_conv = CryptoConversion(value=change, quote=quote, conv_from=from_currency).conversion

    balance = change + fee + net_to_receive

    answer = ConversionResult(
        quote=quote,
        from_currency=from_currency,
        to_currency=to_currency,
        to_convert=to_convert,
        to_convert_conv=to_convert_conv,
        net_to_receive=net_to_receive,
        net_to_receive_conv=net_to_receive_conv,
        fee=fee,
        fee_conv=fee_conv,
        change=change,
        change_conv=change_conv,
        balance=balance,
    )
    return answer


async def keepsats_to_hive(
    msats: int,
    quote: QuoteResponse | None = None,
    to_currency: Currency = Currency.HIVE,
) -> ConversionResult:
    # First deduct the notification minimum from the msats IF the value is > notification minimum:

    if quote is None:
        await TrackedBaseModel.update_quote()
        quote = TrackedBaseModel.last_quote

    from_currency = Currency.MSATS
    original_msats = msats

    # Deduct notification fee if above minimum threshold
    notification_fee = 0
    if msats > V4VConfig().data.minimum_invoice_payment_sats * 1_000:
        notification_amount = Amount(f"0.001 {to_currency.value.upper()}")
        notification_amount_conv = CryptoConversion(
            amount=notification_amount, conv_from=to_currency, quote=quote
        ).conversion
        notification_fee = notification_amount_conv.msats
        msats -= notification_fee

    # Calculate the total amount to convert (including fees)
    to_convert_conv = CryptoConversion(
        value=msats, conv_from=from_currency, quote=quote
    ).conversion
    to_convert = to_convert_conv.value_in(from_currency)

    # Calculate conversion fee
    msats_fee = to_convert_conv.msats_fee
    fee_conv = CryptoConversion(value=msats_fee, conv_from=from_currency, quote=quote).conversion
    fee = fee_conv.value_in(from_currency)

    # Calculate net amount to receive in target currency
    net_to_receive = to_convert - fee
    if net_to_receive < 0:
        raise HiveToKeepsatsConversionError(
            f"Net msats to receive {net_to_receive} is negative, cannot convert."
        )

    net_to_receive_conv = CryptoConversion(
        value=net_to_receive, conv_from=from_currency, quote=quote
    ).conversion

    change = original_msats - (to_convert + notification_fee)
    change_conv = CryptoConversion(value=change, conv_from=from_currency, quote=quote).conversion

    # Balance check
    balance = change + fee + net_to_receive + notification_fee

    answer = ConversionResult(
        quote=quote,
        from_currency=from_currency,
        to_currency=to_currency,
        to_convert=to_convert,
        to_convert_conv=to_convert_conv,
        net_to_receive=net_to_receive,
        net_to_receive_conv=net_to_receive_conv,
        fee=fee,
        fee_conv=fee_conv,
        change=change,
        change_conv=change_conv,
        balance=balance,
    )
    return answer
