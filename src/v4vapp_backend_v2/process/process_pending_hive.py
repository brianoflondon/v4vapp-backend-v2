from typing import List

from colorama import Fore, Style
from nectar.amount import Amount

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import (  # Assuming this function exists for sending custom JSONs
    account_hive_balances,
    get_verified_hive_client,
    send_custom_json,
    send_pending,
)
from v4vapp_backend_v2.hive_models.pending_transaction_class import (
    PendingCustomJson,
    PendingTransaction,
)


async def resend_transactions() -> None:
    """
    Resend all pending transactions.
    """
    await resend_pending_transactions()
    await resend_pending_custom_jsons()


async def resend_pending_transactions() -> None:
    """
    Re-sends all pending Hive transactions if the server has sufficient balance for each transaction.

    This function retrieves all pending transactions and attempts to resend them one by one.
    For each transaction, it checks if the server's Hive or HBD balance is sufficient before resending.
    If the balance is insufficient, the transaction is skipped and a warning is logged.
    Successfully resent transactions are deleted from the pending list.
    All actions and errors are logged.

    Returns:
        None
    """
    all_pending = await PendingTransaction.list_all()
    if len(all_pending) == 0:
        logger.info("No pending Hive transactions to resend.")
        return
    logger.info(f"Resending pending Hive transaction {len(all_pending)}")

    server_id = InternalConfig().server_id
    server_balance = account_hive_balances(hive_accname=server_id)
    # Removed unused total_pending

    sending: List[PendingTransaction] = []

    for pending in all_pending:
        if pending.amount.symbol == "HIVE":
            if server_balance.get("HIVE", Amount("0.000 HIVE")) < pending.amount:
                logger.warning(
                    f"Insufficient HIVE balance to resend pending transactions. "
                    f"Required: {pending.amount}, Available: {server_balance.get('HIVE', Amount('0.000 HIVE'))}"
                )
                continue
            else:
                server_balance["HIVE"] = (
                    server_balance.get("HIVE", Amount("0.000 HIVE")) - pending.amount
                )
                sending.append(pending)
        elif pending.amount.symbol == "HBD":
            if server_balance.get("HBD", Amount("0.000 HBD")) < pending.amount:
                logger.warning(
                    f"Insufficient HBD balance to resend pending transactions. "
                    f"Required: {pending.amount}, Available: {server_balance.get('HBD', Amount('0.000 HBD'))}"
                )
                continue
            else:
                server_balance["HBD"] = (
                    server_balance.get("HBD", Amount("0.000 HBD")) - pending.amount
                )
                sending.append(pending)

    nobroadcast = any(pending.nobroadcast for pending in sending)
    hive_client, _ = await get_verified_hive_client(nobroadcast=nobroadcast)
    for pending in sending:
        try:
            pending.resend_attempt += 1
            trx = await send_pending(pending=pending, hive_client=hive_client)
            logger.info(
                f"{Fore.GREEN}Resent pending transaction {pending}, trx: {trx.get('trx_id')}{Style.RESET_ALL}"
            )
            await pending.delete()
        except Exception as e:
            await pending.save()
            logger.warning(f"Failed to resend pending transaction {pending}: {e}")


async def resend_pending_custom_jsons():
    """
    Asynchronously re-sends all active pending custom JSONs stored in the system.

    - Retrieves all pending custom JSON objects.
    - Filters for active custom JSONs.
    - Initializes a Hive client, determining the 'nobroadcast' flag based on pending items.
    - Iterates through each active pending custom JSON:
        - Skips if 'json_data' is None.
        - Attempts to send the custom JSON using the Hive client.
        - Logs the transaction ID on success and deletes the pending item.
        - Logs a warning if sending fails.

    Logs the number of custom JSONs to be resent and any issues encountered during processing.
    """
    all_pending_cj = await PendingCustomJson.list_all()
    if len(all_pending_cj) == 0:
        logger.info("No pending custom JSONs to resend.")
        return
    logger.info(f"Resending {len(all_pending_cj)} pending custom JSONs.")

    sending_cj: List[PendingCustomJson] = []

    # Process custom JSONs (no balance checks needed)
    for pending in all_pending_cj:
        if pending.active:  # Assuming only active custom JSONs should be sent
            sending_cj.append(pending)

    # Use a default hive_client (assuming nobroadcast=False for custom JSONs)
    nobroadcast = any(pending.nobroadcast for pending in sending_cj)
    hive_client, _ = await get_verified_hive_client(nobroadcast=nobroadcast)

    # Send custom JSONs
    for pending in sending_cj:
        if pending.json_data is None:
            logger.warning(f"Skipping custom JSON {pending} as json_data is None.")
            continue
        try:
            # Note: Adjust parameters based on actual send_custom_json signature
            pending.resend_attempt += 1
            trx = await send_custom_json(
                json_data=pending.json_data,
                send_account=pending.send_account,
                id=pending.cj_id,
                hive_client=hive_client,
                nobroadcast=pending.nobroadcast,
                resend_attempt=pending.resend_attempt,
            )
            logger.info(f"Resent pending custom JSON {pending}, trx: {trx.get('trx_id')}")
            await pending.delete()
        except Exception as e:
            await pending.save()
            logger.warning(f"Failed to resend pending custom JSON {pending}: {e}")
