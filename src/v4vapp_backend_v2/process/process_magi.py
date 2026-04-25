from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.models.invoice_models import Invoice


async def forward_magisats(invoice: Invoice) -> None:
    """
    This function is responsible for forwarding #magisats to the appropriate destination.
    The specific logic for forwarding will depend on the requirements of the application.
    For example, it could involve transferring the sats to a specific wallet or account.
    """
    # Placeholder for the actual forwarding logic
    logger.info("Forwarding #magisats to the designated destination.")

    fixed_quote = invoice.fixed_quote
    if fixed_quote:
        quote = fixed_quote.quote_response
    else:
        quote = await Invoice.nearest_quote(timestamp=invoice.timestamp)

    if not invoice.conv or invoice.conv.is_unset():
        await invoice.update_conv(quote=quote)

    if not invoice.conv or invoice.conv.is_unset():
        logger.error("Conversion details are missing for the invoice.")
        return

    amount_to_send_msats = invoice.value_msat - invoice.conv.msats_fee

    logger.info(
        f"Amount to forward (after fees): {amount_to_send_msats / 1000:.0f} sats "
        f"fee: {invoice.conv.msats_fee / 1000:.3f} sats {invoice.short_id}"
    )

    # Implement the forwarding logic here
    # check we have necessary balance on the server to forward the sats

    # calculate the fee to take from the invoice amount to cover the forwarding transaction fee

    # perform the forwarding transaction to the designated destination
