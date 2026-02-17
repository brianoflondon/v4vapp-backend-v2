import re
from datetime import datetime, timezone
from typing import List

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import (
    LedgerEntry,
    LedgerEntryCreationException,
    LedgerEntryDuplicateException,
    LedgerEntryException,
    LedgerType,
)
from v4vapp_backend_v2.actions.tracked_any import TrackedAny, TrackedTransfer
from v4vapp_backend_v2.actions.tracked_models import ReplyType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.bad_actors_list import check_not_development_accounts
from v4vapp_backend_v2.helpers.general_purpose_funcs import lightning_memo
from v4vapp_backend_v2.helpers.lightning_memo_class import LightningMemo
from v4vapp_backend_v2.hive.hive_extras import HiveAccountNameOnExchangesList, HiveNotHiveAccount
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.process.process_custom_json import process_custom_json_func
from v4vapp_backend_v2.process.process_errors import CustomJsonRetryError, HiveLightningError
from v4vapp_backend_v2.process.process_orders import process_create_fill_order_op
from v4vapp_backend_v2.process.process_pending_hive import resend_transactions
from v4vapp_backend_v2.process.process_transfer import HiveTransferError, follow_on_transfer

# MARK: Hive Transaction Processing


async def process_hive_op(op: TrackedAny, nobroadcast: bool = False) -> List[LedgerEntry]:
    """
    Processes the transfer operation and creates a ledger entry if applicable.
    Returns a list of entries even though this is likely to be only one.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

    Args:
        op (TrackedAny): The operation to process, which can be a transfer, limit order, fill order, or custom JSON.
        nobroadcast (bool): If True, suppresses broadcasting used mostly for testing purposes.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    # Check if a ledger entry with the same group_id already exists
    existing_entry = await LedgerEntry.collection().find_one(filter=op.group_id_query)
    if existing_entry:
        logger.info(f"Ledger entry for group_id {op.group_id} already exists. Skipping.")
        try:
            ledger_entry = LedgerEntry.model_validate(existing_entry)
        except Exception as e:
            message = f"Error validating existing ledger entry: {e}"
            logger.error(message)
            raise LedgerEntryCreationException(message) from e

        raise LedgerEntryDuplicateException(
            f"Ledger entry already exists: {ledger_entry.group_id}"
        )

    # Check if the transfer is between the server account and the treasury account
    # Check if the transfer is between specific accounts

    # MARK: Transfers or Recurrent Transfers
    try:
        if isinstance(op, TransferBase):
            if await check_not_development_accounts([op.from_account, op.to_account]):
                logger.error(
                    f"Development account check failed for: {op.from_account}, {op.to_account}",
                    extra={"notification": True, **op.log_extra},
                )
                return []

            ledger_entry = await process_transfer_op(hive_transfer=op, nobroadcast=nobroadcast)
            return [ledger_entry] if ledger_entry else []
        elif isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder):
            ledger_entries = await process_create_fill_order_op(
                limit_fill_order=op, nobroadcast=nobroadcast
            )
            return ledger_entries
        elif isinstance(op, CustomJson):
            ledger_entries = await process_custom_json_func(
                custom_json=op, nobroadcast=nobroadcast
            )
            return ledger_entries
        return []

    except CustomJsonRetryError as e:
        raise e

    except LedgerEntryException as e:
        if "Transfer between untracked accounts" in str(e):
            logger.info(
                f"Transfer between untracked accounts, no ledger entry created: {e}",
                extra={"notification": False, **op.log_extra},
            )
            return []
        logger.error(
            f"Error processing transfer operation: {e}",
            extra={"notification": False, **op.log_extra},
        )
        return []
        # raise LedgerEntryCreationException(f"Error processing transfer operation: {e}") from e

    except HiveLightningError as e:
        logger.error(f"Hive to Lightning error: {e}", extra={"notification": True, **op.log_extra})
        return []

    except HiveNotHiveAccount as e:
        logger.info(
            f"Not sending to a non-Hive Account: {e}", extra={"notification": True, **op.log_extra}
        )
        return []

    except HiveAccountNameOnExchangesList as e:
        logger.warning(
            f"Not sending to an exchange account: {e}",
            extra={"notification": True, **op.log_extra},
        )
        return []

    except HiveTransferError as e:
        logger.error(f"Hive transfer error: {e}", extra={"notification": True, **op.log_extra})
        return []


async def process_transfer_op(
    hive_transfer: TrackedTransfer, nobroadcast: bool = False
) -> LedgerEntry:
    """
    Processes a Hive transfer operation and creates a ledger entry if applicable.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    # Guard against transfers from and to the same account -- no ledger entry possible
    if hive_transfer.from_account == hive_transfer.to_account:
        message = f"Transfer from and to the same account: {hive_transfer.from_account}, no ledger entry possible."
        logger.debug(
            message,
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise LedgerEntryCreationException(message)

    if not hive_transfer.conv or hive_transfer.conv.is_unset():
        await hive_transfer.update_conv()
        if hive_transfer.conv and hive_transfer.conv.is_unset():
            raise LedgerEntryCreationException(
                "Conversion failed during update_conv, conversion not set in operation."
            )

    ledger_entry = LedgerEntry(
        group_id=hive_transfer.group_id,
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        user_memo=hive_transfer.user_memo,
        timestamp=datetime.now(tz=timezone.utc),
        link=hive_transfer.link,
    )
    processed_d_memo = lightning_memo(hive_transfer.d_memo)
    base_description = f"{hive_transfer.amount_str} from {hive_transfer.from_account} to {hive_transfer.to_account} {processed_d_memo}"
    hive_config = InternalConfig().config.hive
    server_account, treasury_account, funding_account, exchange_account = (
        hive_config.all_account_names
    )
    expense_accounts = InternalConfig().config.expense_config.hive_expense_accounts
    if not server_account or not treasury_account or not funding_account or not exchange_account:
        raise LedgerEntryCreationException(
            "Server account, treasury account, funding account, or exchange account not configured."
        )
    if not hive_transfer.conv:
        raise LedgerEntryCreationException("Conversion not set in operation.")

    follow_on_task = None
    ledger_entry.description = base_description
    ledger_entry.credit_unit = ledger_entry.debit_unit = hive_transfer.unit
    ledger_entry.credit_amount = ledger_entry.debit_amount = hive_transfer.amount_decimal
    ledger_entry.credit_conv = ledger_entry.debit_conv = hive_transfer.conv
    ledger_entry.cust_id = hive_transfer.cust_id

    # MARK: Server to Treasury
    if (
        hive_transfer.from_account == server_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
        ledger_entry.description = f"Server to Treasury transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.SERVER_TO_TREASURY
    # MARK: Treasury to Server
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == server_account
    ):
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.description = f"Treasury to Server transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_SERVER
        # different treatment if a payment to an expense account is involved
        if hive_transfer.user_memo:
            lm = LightningMemo(memo=hive_transfer.user_memo)
            if lm.invoice:
                ledger_entry.user_memo = lm.short_memo
                follow_on_task = follow_on_transfer(
                    tracked_op=hive_transfer, nobroadcast=nobroadcast
                )

    # MARK: Funding to Treasury
    elif (
        hive_transfer.from_account == funding_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = LiabilityAccount(name="Owner Loan Payable", sub=funding_account)
        ledger_entry.description = f"Funding to Treasury transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.FUNDING
    # MARK: Treasury to Funding
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == funding_account
    ):
        ledger_entry.debit = LiabilityAccount(name="Owner Loan Payable", sub=treasury_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=funding_account)
        ledger_entry.description = f"Treasury to Funding transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_FUNDING
    # MARK: Treasury to Exchange
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == exchange_account
    ):
        ledger_entry.debit = AssetAccount(name="Exchange Deposits Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.description = f"Treasury to Exchange transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_EXCHANGE
    # MARK: Server to Exchange
    elif (
        hive_transfer.from_account == server_account
        and hive_transfer.to_account == exchange_account
    ):
        ledger_entry.debit = AssetAccount(name="Exchange Deposits Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
        ledger_entry.description = f"Server to Exchange transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.SERVER_TO_EXCHANGE
    # MARK: Exchange to Treasury
    elif (
        hive_transfer.from_account == exchange_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Exchange Deposits Hive", sub=treasury_account)
        ledger_entry.description = f"Exchange to Treasury transfer: {base_description}"
        ledger_entry.user_memo = lightning_memo(hive_transfer.user_memo)
        ledger_entry.ledger_type = LedgerType.EXCHANGE_TO_TREASURY

    # MARK: Expense Payments
    elif hive_transfer.to_account in expense_accounts:
        expense_account = hive_config.hive_accs.get(hive_transfer.to_account, None)
        if not expense_account:
            raise LedgerEntryCreationException(
                f"Expense account {hive_transfer.to_account} not found in configuration."
            )
        expense_rule = InternalConfig().config.expense_config.hive_expense_rules.get(
            hive_transfer.to_account, None
        )
        if not expense_rule:
            raise LedgerEntryCreationException(
                f"Expense account {hive_transfer.to_account} has no expense rule defined."
            )
        ledger_entry.debit = ExpenseAccount(
            name=expense_rule.expense_account_name, sub=expense_account.name
        )
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=hive_transfer.from_account)
        ledger_entry.description = f"{expense_rule.description} - {base_description}"
        ledger_entry.user_memo = lightning_memo(hive_transfer.user_memo)
        ledger_entry.ledger_type = expense_rule.ledger_type

    # MARK: Server to customer account withdrawal
    elif hive_transfer.from_account == server_account:
        customer = hive_transfer.to_account
        server = hive_transfer.from_account
        ledger_entry.debit = LiabilityAccount("VSC Liability", sub=customer)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.description = f"Withdrawal: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_OUT

        # MARK: Server to suspicious account holding v4vapp.sus
        if hive_transfer.to_account == "v4vapp.sus":
            # This is the balancing transaction for the suspicious account hold
            follow_on_task = suspicious_account_transfer_accounting(hive_transfer=hive_transfer)

    # MARK: Customer account to server account deposit
    elif hive_transfer.to_account == server_account:
        customer = hive_transfer.from_account
        server = hive_transfer.to_account
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.credit = LiabilityAccount("VSC Liability", sub=customer)
        ledger_entry.description = f"Deposit: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_IN
        ledger_entry.user_memo = lightning_memo(hive_transfer.user_memo)
        # Now we need to see if we can take action for this invoice
        # This will be handled in a separate task
        follow_on_task = follow_on_transfer(tracked_op=hive_transfer, nobroadcast=nobroadcast)

    else:
        logger.info(
            f"Transfer between two different accounts: {hive_transfer.from_account} -> {hive_transfer.to_account}"
        )
        raise LedgerEntryCreationException("Transfer between untracked accounts.")
    try:
        await ledger_entry.save()
    except LedgerEntryException as e:
        message = f"Error saving ledger entry: {e}"
        reply_id = f"{hive_transfer.group_id}_ledger_error"
        try:
            hive_transfer.add_reply(
                reply_id=reply_id, reply_type=ReplyType.LEDGER_ERROR, reply_error=message
            )
            await hive_transfer.save()
        except ValueError as e:
            # Catching this value error means we will only try to processes a given object twice, not more
            message = f"Repeat error processing {hive_transfer.group_id}: {e}"
            logger.error(
                message,
                extra={"notification": False, **hive_transfer.log_extra, **ledger_entry.log_extra},
            )
            raise LedgerEntryException(message)
    # After any hive Transactions we can try to resend
    await resend_transactions()
    if follow_on_task:
        # If there is a follow-on task, we need to run it in the background
        try:
            await follow_on_task
            # In addition to processing, check if there are any pending transactions
        except LedgerEntryDuplicateException as e:
            logger.warning(f"Follow-on task duplicate entry: {e}", extra={"notification": False})
        except HiveTransferError as e:
            logger.warning(f"Follow-on task failed: {e}", extra={"notification": False})
        except Exception as e:
            logger.exception(f"Follow-on task failed: {e}", extra={"notification": False})

    return ledger_entry


async def suspicious_account_transfer_accounting(hive_transfer: TrackedTransfer) -> None:
    """
    Process accounting entries for suspicious account transfers.

    This function handles transactions involving accounts flagged on the bad accounts list.
    It extracts customer ID and original transaction ID from the transfer memo using regex pattern matching,
    then creates and saves a ledger entry to track the suspicious transaction in the liability accounts.

    Args:
        hive_transfer (TrackedTransfer): The transfer object containing transaction details including
            memo, group_id, short_id, operation type, user memo, link, unit, amount, and conversion data.

    Returns:
        None

    Raises:
        Exception: Any exceptions raised by ledger_entry.save() operation.

    Note:
        - The function only processes transfers with memos matching the suspicious account pattern.
        - Creates a LedgerType.SUSPICIOUS entry linking the suspicious account to VSC Liability accounts.
        - The memo pattern expects format: "Suspicious account transaction: {cust_id} is on the bad accounts list | § {original_trans_id} |"
    """

    pattern = r"Suspicious\s+account\s+transaction:\s+(?P<cust_id>\S+)\s+is\s+on\s+the\s+bad\s+accounts\s+list\s+\|\s+§\s+(?P<original_trans_id>\S+)\s+\|"
    m = re.search(pattern, hive_transfer.d_memo)
    if m:
        ledger_type = LedgerType.SUSPICIOUS
        ledger_entry_2 = LedgerEntry(
            cust_id=m.group("cust_id"),
            ledger_type=ledger_type,
            group_id=f"{hive_transfer.group_id}_{ledger_type}",
            short_id=hive_transfer.short_id,
            op_type=hive_transfer.op_type,
            user_memo=hive_transfer.user_memo,
            timestamp=datetime.now(tz=timezone.utc),
            link=hive_transfer.link,
            description=f"{hive_transfer.d_memo}",
            credit=LiabilityAccount(name="VSC Liability", sub="v4vapp.sus"),
            debit=LiabilityAccount(name="VSC Liability", sub=m.group("cust_id")),
            credit_unit=hive_transfer.unit,
            debit_unit=hive_transfer.unit,
            credit_amount=hive_transfer.amount_decimal,
            debit_amount=hive_transfer.amount_decimal,
            credit_conv=hive_transfer.conv,
            debit_conv=hive_transfer.conv,
        )
        await ledger_entry_2.save()
        logger.warning(
            f"Suspicious account transfer detected and recorded: {ledger_entry_2.description}",
            extra={"notification": True, **hive_transfer.log_extra},
        )
