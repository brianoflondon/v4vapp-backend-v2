from decimal import ROUND_CEILING, ROUND_HALF_UP, Decimal

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.hive.v4v_config import V4VConfig

MARGIN_SPREAD = Decimal(0.0)


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


def calculate_fee_msats(msats: Decimal) -> Decimal:
    """
    Calculate the service fee based on the base amount in milisats.
    Returns zero below the minimum invoice amount but calcs a fee for values above maximum.

    Args:
        msats (Decimal): The base amount in milisats.

    Returns:
        Decimal: The calculated service fee in milisats.
    """
    config_data = V4VConfig().data
    fee = ((config_data.conv_fee_percent + MARGIN_SPREAD) * msats) + (
        config_data.conv_fee_sats * 1_000
    )
    return fee.quantize(Decimal(1), rounding=ROUND_HALF_UP)  # Round to nearest integer


def calculate_fee_estimate_msats(msats: Decimal) -> Decimal:
    """
      Calculate a maximum estimated forwarding fee that a lightning payment might incur..
      The maximum ppm fee allowed is set in the `lnd_config` config file alongside a limit and base fee.

      ```
        lightning_fee_limit_ppm: 5000
        lightning_fee_estimate_ppm: 1000
        lightning_fee_base_msats: 50000
    ```


      Args:
          msats (Decimal): The base amount in milisats.

      Returns:
          Decimal: The estimated total amount in milisats including the service fee.
    """
    lnd_config = InternalConfig().config.lnd_config
    fee_estimate = Decimal(lnd_config.lightning_fee_base_msats) + (
        msats * Decimal(lnd_config.lightning_fee_estimate_ppm) / 1_000_000
    )
    return fee_estimate.quantize(Decimal(1), rounding=ROUND_HALF_UP)  # Round to nearest integer


def limit_test(msats: Decimal = Decimal(0)) -> bool:
    """
    Checks if an invoice principal in millisatoshis is within configured payment limits.

    Callers must pass the Lightning invoice principal (BOLT11/LNURL `value_msat`, or the
    amount attached to a zero-value invoice). Do not pass the full Hive/HBD deposit
    conversion: deposits intentionally exceed principal to cover service and routing fees.

    Args:
        msats: Invoice principal in millisatoshis.

    Returns:
        True if the principal is within min and max invoice payment limits.

    Raises:
        V4VMinimumInvoice: If the amount is less than the configured minimum invoice payment in satoshis.
        V4VMaximumInvoice: If the amount is greater than the configured maximum invoice payment in satoshis.
    """
    config_data = V4VConfig().data
    sats = Decimal(Decimal(msats) / Decimal(1000)).quantize(
        Decimal(1), rounding=ROUND_CEILING
    )  # Convert msats to sats; ceiling so fractional msats cannot slip under a limit
    if sats < config_data.minimum_invoice_payment_sats:
        raise V4VMinimumInvoice(
            f"{sats:,.0f} sats is below minimum invoice of {config_data.minimum_invoice_payment_sats:,.0f} sats"
        )
    if sats > config_data.maximum_invoice_payment_sats:
        raise V4VMaximumInvoice(
            f"{sats:,.0f} sats exceeds maximum invoice of {config_data.maximum_invoice_payment_sats:,.0f} sats"
        )
    return True
