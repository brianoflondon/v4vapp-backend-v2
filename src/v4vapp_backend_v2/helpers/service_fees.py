from decimal import Decimal

from v4vapp_backend_v2.hive.v4v_config import V4VConfig

MARGIN_SPREAD = Decimal(0.002)


class V4VMinimumInvoice(ValueError):
    """
    Exception raised when the invoice amount is less than the minimum allowed.
    """

    pass


class V4VMaximumInvoice(ValueError):
    """
    Exception raised when the invoice amount is greater than the maximum allowed.
    """

    pass


def msats_fee(msats: Decimal) -> Decimal:
    """
    Calculate the service fee based on the base amount in milisats.
    Returns zero below the minimum invoice amount but calcs a fee for values above maximum.

    Args:
        msats (float): The base amount in milisats.

    Returns:
        int: The calculated service fee in milisats.
    """
    config_data = V4VConfig().data
    fee = ((config_data.conv_fee_percent + MARGIN_SPREAD) * msats) + (
        config_data.conv_fee_sats * 1_000
    )
    return fee.quantize(Decimal("1"))  # Round to nearest integer


def limit_test(msats: Decimal = Decimal(0.0)) -> bool:
    """
    Checks if the given amount in millisatoshis (msats) is within the allowed invoice payment limits.

        msats (float, optional): The amount in millisatoshis to check. Defaults to 0.0.

        bool: True if the amount is within the configured minimum and maximum invoice payment limits.
    Raises:
        V4VMinimumInvoice: If the amount is less than the configured minimum invoice payment in satoshis.
        V4VMaximumInvoice: If the amount is greater than the configured maximum invoice payment in satoshis.
    """
    config_data = V4VConfig().data
    sats = msats // 1000
    if sats < config_data.minimum_invoice_payment_sats:
        raise V4VMinimumInvoice(
            f"{sats:,.0f} sats is below minimum invoice of {config_data.minimum_invoice_payment_sats} sats"
        )
    if sats > config_data.maximum_invoice_payment_sats:
        raise V4VMaximumInvoice(
            f"{sats:,.0f} sats exceeds maximum invoice of {config_data.maximum_invoice_payment_sats} sats"
        )
    return True
