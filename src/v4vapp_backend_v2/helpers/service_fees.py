from v4vapp_backend_v2.hive.v4v_config import V4VConfig

MARGIN_SPREAD = 0.002


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


def msats_fee(msats: float) -> int:
    """
    Calculate the service fee based on the base amount in milisats.
    Returns zero below the minimum invoice amount but calcs a fee for values above maximum.

    Args:
        msats (float): The base amount in milisats.

    Returns:
        int: The calculated service fee in milisats.
    """
    try:
        limit_test(msats)
    except V4VMinimumInvoice:
        return 0
    except V4VMaximumInvoice:
        pass
    config_data = V4VConfig().data
    fee: float = (config_data.conv_fee_sats * 1_000) + (
        config_data.conv_fee_percent + MARGIN_SPREAD
    ) * msats
    return int(fee)


def limit_test(msats: float = 0.0) -> bool:
    """
    Calculate the service fee based on the base amount in milisats.

    Args:
        msats (float): The base amount in milisats.

    Returns:
        bool: True if the invoice amount is within limits.

    Raises:
        ValueError: If the invoice amount is less than the minimum or greater than the maximum.
    """
    config_data = V4VConfig().data
    sats = msats / 1000
    if sats < config_data.minimum_invoice_payment_sats:
        raise V4VMinimumInvoice(
            f"Minimum invoice is {config_data.minimum_invoice_payment_sats} sats"
        )
    if sats > config_data.maximum_invoice_payment_sats:
        raise V4VMaximumInvoice(
            f"Maximum invoice is {config_data.maximum_invoice_payment_sats} sats"
        )
    return True
