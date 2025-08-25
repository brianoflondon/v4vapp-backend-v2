from pprint import pprint
from typing import List

from nectar.amount import Amount

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import (
    account_hive_balances,
    get_verified_hive_client,
    send_transfer_bulk,
)
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction


async def resend_hive_transaction():
    """
    Resend a pending Hive transaction.
    """
    all_pending = await PendingTransaction.list_all()
    if len(all_pending) == 0:
        logger.info("No pending Hive transactions to resend.")
        return
    logger.info(f"Resending pending Hive transaction {len(all_pending)}")

    server_id = InternalConfig().server_id
    server_balance = account_hive_balances(hive_accname=server_id)
    total_pending = await PendingTransaction.total_pending()

    sending: List[PendingTransaction] = []

    for pending in all_pending:
        if pending.amount.symbol == "HIVE":
            if server_balance.get("HIVE", 0) < pending.amount.amount:
                logger.warning(
                    f"Insufficient HIVE balance to resend pending transactions. "
                    f"Required: {pending.amount}, Available: {server_balance.get('HIVE', 0)}"
                )
            else:
                server_balance["HIVE"] = (
                    server_balance.get("HIVE", Amount("0.000 HIVE")) - pending.amount
                )
                sending.append(pending)
        if pending.amount.symbol == "HBD":
            if server_balance.get("HBD", 0) < pending.amount.amount:
                logger.warning(
                    f"Insufficient HBD balance to resend pending transactions. "
                    f"Required: {pending.amount}, Available: {server_balance.get('HBD', 0)}"
                )
            else:
                server_balance["HBD"] = (
                    server_balance.get("HBD", Amount("0.000 HBD")) - pending.amount
                )
                sending.append(pending)

    for pending in sending:
        print(pending)

    nobroadcast = any(pending.nobroadcast for pending in sending)
    hive_client, _ = await get_verified_hive_client(nobroadcast=nobroadcast)
    broadcast = await send_transfer_bulk(transfer_list=sending, hive_client=hive_client)
    pprint(broadcast)
