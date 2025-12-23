from datetime import datetime, timedelta, timezone
from decimal import Decimal

from nectar.amount import Amount
from pydantic import BaseModel
from tabulate import tabulate

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive.hive_extras import HiveToKeepsatsConversionError
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase


class ConversionResult(BaseModel):
    """
    ConversionResult is a Pydantic model that encapsulates the result of a conversion operation.
    It includes the converted amount, fee, and any additional information related to the conversion.
    """

    quote: QuoteResponse
    from_currency: Currency
    to_currency: Currency
    to_convert: Decimal
    to_convert_conv: CryptoConv
    net_to_receive: Decimal
    net_to_receive_conv: CryptoConv
    fee: Decimal
    fee_conv: CryptoConv
    change: Decimal
    change_conv: CryptoConv
    balance: Decimal = Decimal(0)
    original_msats: Decimal = Decimal(0)
    original_msats_conv: CryptoConv | None = None

    def _fmt_value(self, value: Decimal | int, currency: Currency, padded: bool = False) -> str:
        """
        Format numbers:
        - MSATS displayed as sats (value/1000) with 3 decimals
        - HIVE/HBD as 3 decimals
        """
        if currency == Currency.MSATS:
            return f"{float(value) / 1000:>50,.3f}" if padded else f"{float(value) / 1000:,.0f}"
        return f"{float(value):>50,.3f}" if padded else f"{float(value):,.3f}"

    def __str__(self) -> str:
        def fmt(value: Decimal | int, currency: Currency) -> str:
            # Reuse class formatter with padding for the table
            return self._fmt_value(value, currency, padded=True)

        def unit_label(currency: Currency) -> str:
            if currency == Currency.MSATS:
                return "sats"
            return str(currency)

        conversion_data = [
            [
                "to_convert (from_currency)",
                fmt(self.to_convert, self.from_currency),
                unit_label(self.from_currency),
            ],
            [
                "to_convert (to_currency)",
                fmt(self.to_convert_conv.value_in(self.to_currency), self.to_currency),
                unit_label(self.to_currency),
            ],
            [
                "net_to_receive (from_currency)",
                fmt(self.net_to_receive, self.from_currency),
                unit_label(self.from_currency),
            ],
            [
                "net_to_receive (to_currency)",
                fmt(self.net_to_receive_conv.value_in(self.to_currency), self.to_currency),
                unit_label(self.to_currency),
            ],
            [
                "fee (from_currency)",
                fmt(self.fee, self.from_currency),
                unit_label(self.from_currency),
            ],
            [
                "fee (to_currency)",
                fmt(self.fee_conv.value_in(self.to_currency), self.to_currency),
                unit_label(self.to_currency),
            ],
            [
                "change (from_currency)",
                fmt(self.change, self.from_currency),
                unit_label(self.from_currency),
            ],
            [
                "balance (from_currency)",
                fmt(self.balance, self.from_currency),
                unit_label(self.from_currency),
            ],
            ["from_currency", unit_label(self.from_currency), ""],
            ["to_currency", unit_label(self.to_currency), ""],
            ["to_convert", str(self.to_convert_amount), ""],
            ["net_to_receive", str(self.net_to_receive_amount), ""],
            ["original_msats", str(self.original_msats), "msats"],
        ]
        return f"Conversion Details:\n{tabulate(conversion_data, headers=['Parameter', 'Value', 'Unit'], tablefmt='fancy_grid')}"

    @property
    def log_str(self) -> str:
        # Compact log: "<from> -> <to> (fee <sats> sats)"
        # - sats: 0 decimals
        # - hive/hbd: 3 decimals
        def unit_for(c: Currency) -> str:
            return "sats" if c == Currency.MSATS else c.value

        # Left side (from)
        left_val = self._fmt_value(
            self.to_convert,
            Currency.MSATS if self.from_currency == Currency.MSATS else self.from_currency,
            padded=False,
        )
        left_unit = unit_for(self.from_currency)

        # Right side (to) — use net_to_receive in target currency
        right_amount = self.net_to_receive_conv.value_in(self.to_currency)
        right_val = self._fmt_value(
            right_amount,
            Currency.MSATS if self.to_currency == Currency.MSATS else self.to_currency,
            padded=False,
        )
        right_unit = unit_for(self.to_currency)

        # Fee always in sats, 0 decimals (truncate)
        fee_msats = self.fee_conv.value_in(Currency.MSATS)
        fee_sats_int = int(fee_msats / 1000)

        return f"{left_val} {left_unit} -> {right_val} {right_unit} (fee {fee_sats_int} sats)"

    @property
    def hive_or_hbd(self) -> Currency:
        """
        Determines whether the conversion involves HIVE or HBD currency.

        Returns:
            Currency: Returns `to_currency` if it is HIVE or HBD, otherwise returns `from_currency` if it is HIVE or HBD.
                      If neither is HIVE or HBD, defaults to returning `Currency.HIVE`.
        """
        if self.to_currency in [Currency.HIVE, Currency.HBD]:
            return self.to_currency
        if self.from_currency in [Currency.HIVE, Currency.HBD]:
            return self.from_currency
        return Currency.HIVE

    @property
    def net_to_receive_amount(self) -> Amount:
        """
        Calculates and returns the net amount to be received in the target currency if the target is Hive/HBD
        Returns:
            Amount: An Amount object representing the net amount to receive, denominated in the target currency.
        """
        hive_rounded = round(self.net_to_receive_conv.value_in(self.hive_or_hbd), 3)
        return Amount(f"{hive_rounded} {self.hive_or_hbd.value.upper()}")

    @property
    def to_convert_amount(self) -> Amount:
        """
        Calculates and returns the amount to be converted in the target currency if the target is Hive/HBD
        Returns:
            Amount: An Amount object representing the amount to convert, denominated in the target currency.
        """
        hive_rounded = round(self.to_convert_conv.value_in(self.hive_or_hbd), 3)
        return Amount(f"{hive_rounded} {self.hive_or_hbd.value.upper()}")


async def calc_hive_to_keepsats(
    tracked_op: TransferBase,
    quote: QuoteResponse | None = None,
    msats: Decimal = Decimal(0),
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

    original = Decimal(str(tracked_op.amount.amount_decimal))

    return hive_to_keepsats_calc(
        msats=msats,
        amount_minus_minimum=tracked_op.amount.minus_minimum,
        quote=quote,
        from_currency=from_currency,
        to_currency=to_currency,
        original=original,
    )


def hive_to_keepsats_calc(
    msats: Decimal,
    amount_minus_minimum: Amount,
    quote: QuoteResponse,
    from_currency: Currency,
    to_currency: Currency,
    original: Decimal,
) -> ConversionResult:
    if msats == 0:
        # Base transfer amount on the inbound Hive/HBD transfer amount
        hive_to_convert_amount = amount_minus_minimum
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
        to_convert=Decimal(to_convert),
        to_convert_conv=to_convert_conv,
        net_to_receive=Decimal(net_to_receive),
        net_to_receive_conv=net_to_receive_conv,
        fee=Decimal(fee),
        fee_conv=fee_conv,
        change=Decimal(change),
        change_conv=change_conv,
        balance=Decimal(balance),
    )
    return answer


async def calc_keepsats_to_hive(
    timestamp: datetime = datetime.now(tz=timezone.utc),
    msats: Decimal | None = None,
    to_currency: Currency = Currency.HIVE,
    amount: Amount | None = None,
    quote: QuoteResponse | None = None,
) -> ConversionResult:
    """
    Convert Keepsats (msats) to Hive/HBD OR (if a target Amount is supplied) compute the
    msats required so the user receives exactly that Hive/HBD amount net of fees
    (and notification fee if applicable).

    Modes:
        1. msats -> Hive/HBD (current behavior, when 'amount' is None)
           - Apply notification fee (fixed 0.001 target unit) if initial msats > threshold.
           - Apply conversion fee.
           - net_to_receive (msats) = msats_after_notification - conversion_fee_msats.

        2. target Amount -> required msats (when 'amount' provided)
           - 'amount' (Hive or HBD) is the desired NET amount after all fees.
           - Invert fee logic iteratively to find required msats (before notification fee).
           - Apply notification fee rule (adds fixed msats before conversion if threshold hit).
           - Ensures net_to_receive_conv.value_in(to_currency) == amount.amount_decimal (±1 unit of precision).

    Returned fields:
        net_to_receive: always expressed in FROM currency (msats).
        net_to_receive_conv: conversion object representing the target currency net amount.
        to_convert: msats actually passed into conversion AFTER notification fee is removed.
        change / balance: remain consistent with existing semantics.
    """
    if quote is None:
        if datetime.now(tz=timezone.utc) - timestamp > timedelta(minutes=5):
            quote = await TrackedBaseModel.nearest_quote(timestamp)
        else:
            await TrackedBaseModel.update_quote()
            quote = TrackedBaseModel.last_quote

    from_currency = Currency.MSATS

    # If a target amount is supplied, override to_currency from it
    if amount is not None:
        to_currency = Currency(amount.symbol.lower())

    # Threshold (msats) for applying notification fee is either minimum sats value configured or 250 sats
    notification_threshold_msats = max(
        V4VConfig().data.minimum_invoice_payment_sats * Decimal(1_000), Decimal(250_000)
    )

    # ------------------------------------------------------------------
    # Mode 2: Target Amount (Hive/HBD) provided -> solve for msats needed
    # ------------------------------------------------------------------
    if amount is not None:
        target_amount = amount  # Amount in Hive/HBD the user must receive NET
        # Convert target (net) amount to msats (net target msats after all conversion fees)
        target_net_conv = CryptoConversion(
            amount=target_amount, conv_from=to_currency, quote=quote
        ).conversion
        target_net_msats = (
            target_net_conv.msats
        )  # Net msats needed after conversion fee & notif deduction

        # Iteratively add back conversion fee to find msats AFTER notification fee deduction
        # Start with an initial guess (net)
        msats_after_notification = target_net_msats
        for _ in range(8):  # usually converges in 1-3 iterations
            conv_guess = CryptoConversion(
                value=msats_after_notification, conv_from=from_currency, quote=quote
            ).conversion
            fee_guess = conv_guess.msats_fee
            required = target_net_msats + fee_guess
            if abs(required - msats_after_notification) <= 1:
                msats_after_notification = required
                break
            msats_after_notification = required

        # Notification fee (fixed 0.001 target unit) in msats
        notification_fee_msats = Decimal(0)
        notif_amount = Amount(f"0.001 {to_currency.value.upper()}")
        notif_conv = CryptoConversion(
            amount=notif_amount, conv_from=to_currency, quote=quote
        ).conversion
        potential_notification_fee = notif_conv.msats

        # Compute initial msats BEFORE notification deduction
        # If initial msats exceeds threshold, we must add the notification fee back
        msats_initial = msats_after_notification
        if msats_initial > notification_threshold_msats:
            msats_initial += potential_notification_fee
            notification_fee_msats = potential_notification_fee

        # Now recompute forward using the derived msats_after_notification to build standard objects
        # (This ensures consistency with regular forward mode)
        to_convert_conv = CryptoConversion(
            value=msats_after_notification, conv_from=from_currency, quote=quote
        ).conversion
        to_convert = to_convert_conv.value_in(from_currency)  # msats_after_notification

        msats_fee = to_convert_conv.msats_fee
        fee_conv = CryptoConversion(
            value=msats_fee, conv_from=from_currency, quote=quote
        ).conversion
        fee = fee_conv.value_in(from_currency)  # msats

        net_to_receive = to_convert - fee  # should equal target_net_msats
        net_to_receive_conv = CryptoConversion(
            value=net_to_receive, conv_from=from_currency, quote=quote
        ).conversion  # convert net msats to Hive/HBD for reporting

        # Sanity clamp if tiny rounding differences
        # (If conversion drift causes > small difference, you could re-iterate with refined guess)
        # change / balance semantics:
        original_msats = Decimal(msats_initial)
        change = original_msats - (to_convert + notification_fee_msats)
        change_conv = CryptoConversion(
            value=change, conv_from=from_currency, quote=quote
        ).conversion
        balance = change + fee + net_to_receive + notification_fee_msats

        return ConversionResult(
            quote=quote,
            from_currency=from_currency,
            to_currency=to_currency,
            to_convert=Decimal(to_convert),
            to_convert_conv=to_convert_conv,
            net_to_receive=Decimal(net_to_receive),
            net_to_receive_conv=net_to_receive_conv,  # contains the requested Hive/HBD amount
            fee=Decimal(fee),
            fee_conv=fee_conv,
            change=Decimal(change),
            change_conv=change_conv,
            balance=Decimal(balance),
            original_msats=Decimal(target_net_conv.msats),
            original_msats_conv=target_net_conv,
        )

    # ------------------------------------------------------------------
    # Mode 1: Original forward conversion (msats provided)
    # ------------------------------------------------------------------
    if msats is None:
        raise HiveToKeepsatsConversionError("msats must be provided if amount is not supplied.")

    original_msats = Decimal(msats)
    original_msats_conv = CryptoConversion(
        value=original_msats, conv_from=from_currency, quote=quote
    ).conversion

    # Deduct notification fee if above minimum threshold
    notification_fee = Decimal(0)
    if msats > notification_threshold_msats:
        notification_amount = Amount(f"0.001 {to_currency.value.upper()}")
        notification_amount_conv = CryptoConversion(
            amount=notification_amount, conv_from=to_currency, quote=quote
        ).conversion
        notification_fee = notification_amount_conv.msats
        msats -= notification_fee

    # Calculate the total amount to convert (including fees)
    to_convert_conv = CryptoConversion(
        value=original_msats, conv_from=from_currency, quote=quote
    ).conversion
    to_convert = to_convert_conv.value_in(from_currency)

    # Calculate conversion fee
    msats_fee = to_convert_conv.msats_fee
    fee_conv = CryptoConversion(value=msats_fee, conv_from=from_currency, quote=quote).conversion
    fee = fee_conv.value_in(from_currency)

    # Net (msats) after conversion fee
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

    balance = change + fee + net_to_receive + notification_fee

    return ConversionResult(
        quote=quote,
        from_currency=from_currency,
        to_currency=to_currency,
        to_convert=Decimal(to_convert),
        to_convert_conv=to_convert_conv,
        net_to_receive=Decimal(net_to_receive),
        net_to_receive_conv=net_to_receive_conv,
        fee=Decimal(fee),
        fee_conv=fee_conv,
        change=Decimal(change),
        change_conv=change_conv,
        balance=Decimal(balance),
        original_msats=original_msats,
        original_msats_conv=original_msats_conv,
    )
