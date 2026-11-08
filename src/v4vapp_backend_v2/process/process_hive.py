import re
from datetime import UTC, datetime

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
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.helpers.bad_actors_list import check_not_development_accounts
from v4vapp_backend_v2.helpers.general_purpose_funcs import lightning_memo
from v4vapp_backend_v2.helpers.lightning_memo_class import LightningMemo
from v4vapp_backend_v2.hive.hive_extras import HiveAccountNameOnExchangesList, HiveNotHiveAccount
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_cancelled import LimitOrderCancelled
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.process.balance_request import reply_with_balance_request
from v4vapp_backend_v2.process.process_custom_json import process_custom_json_func
from v4vapp_backend_v2.process.process_errors import CustomJsonRetryError, HiveLightningError
from v4vapp_backend_v2.process.process_orders import process_create_fill_order_op
from v4vapp_backend_v2.process.process_pending_hive import resend_transactions
from v4vapp_backend_v2.process.process_transfer import HiveTransferError, follow_on_transfer

# MARK: Hive Transaction Processing

# TODO: This should be in the config.
BALANCE_ADJUSTMENT_MEMO = "Balance adjustment"
BALANCE_ADJUSTMENT_ACCOUNT = "v4v-app"


class HiveLedgerOrderMismatch(Exception):
    """Incoming op_in_trx does not follow the ops already booked for this Hive trx."""


# Same set process_hive_op accepts before the ledger check. TransferBase covers
# transfer, recurrent_transfer, and fill_recurrent_transfer.
HiveLedgerOp = TransferBase | LimitOrderCreate | FillOrder | LimitOrderCancelled | CustomJson


def hive_ledger_trx_filter(op: HiveLedgerOp) -> dict[str, dict[str, str]]:
    """Mongo filter for ledger rows of this Hive operation's transaction.

    ``group_id`` is ``{block}_{trx_id}_{op_in_trx}_{realm}``, and a fill adds
    ``_{ledger_type}``. The anchored prefix is a range on the unique
    ``group_id`` index, so this reads one transaction, not the ledger. Realm
    stays in the key because a limit order's real op and its virtual fills
    share a trx id and each start at 1. Block stays in the key because the
    all-zero trx id is shared by every pure virtual op.
    """
    prefix = f"{op.block_num}_{op.trx_id}_"
    realm = f"{op.realm}"
    pattern = rf"^{re.escape(prefix)}\d+_{re.escape(realm)}($|_)"
    return {"group_id": {"$regex": pattern}}


def booked_op_in_trx(group_id: str, op: HiveLedgerOp) -> int | None:
    """Return the op index stored in ``group_id``, ignoring a ledger-type suffix.

    One fill writes a buy row and a sell row. Both end with the same
    ``op_in_trx`` and must count as one booked operation.
    """
    if not isinstance(group_id, str):
        return None
    prefix = f"{op.block_num}_{op.trx_id}_"
    if not group_id.startswith(prefix):
        return None
    op_text, sep, tail = group_id[len(prefix) :].partition("_")
    if not sep or not op_text.isdecimal():
        return None
    realm = f"{op.realm}"
    if tail != realm and not tail.startswith(f"{realm}_"):
        return None
    return int(op_text)


def _ledger_op_slot(op_in_trx: int) -> int:
    """Map a stored or incoming index onto the 1-based sequence.

    Older rows used ``0`` for the first operation in a transaction. That is
    the same slot as ``1``.
    """
    return 1 if op_in_trx == 0 else op_in_trx


def _norm_id(value: object) -> object:
    """Compare order ids whether Mongo stored them as int or decimal string."""
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return value


def _amount_id(amount: object) -> tuple[str, str] | None:
    """Identity of a Hive amount: integer string plus NAI, not a float."""
    if isinstance(amount, dict):
        raw = amount.get("amount")
        nai = amount.get("nai")
    else:
        raw = getattr(amount, "amount", None)
        nai = getattr(amount, "nai", None)
    if raw is None or nai is None:
        return None
    return (str(raw), str(nai))


def fill_exchange_identity(op: FillOrder) -> tuple | None:
    """Identity of one market fill, independent of ``op_in_trx``.

    One Hive transaction can contain several ``fill_order`` virtual ops.
    They arrive together and ``db_monitor`` handles each change in its own
    task, so a later index can be booked first. The fill is still a
    different trade when the order ids, accounts, or amounts differ.
    A rescan that stores the same fill under a new index matches this key
    and is not booked again.
    """
    try:
        current = _amount_id(op.current_pays)
        opened = _amount_id(op.open_pays)
        current_orderid = op.current_orderid
        open_orderid = op.open_orderid
        current_owner = op.current_owner
        open_owner = op.open_owner
    except AttributeError:
        return None
    if current is None or opened is None:
        return None
    return (
        _norm_id(current_orderid),
        _norm_id(open_orderid),
        current_owner,
        open_owner,
        current,
        opened,
    )


def _stored_fill_identity(doc: dict) -> tuple | None:
    current = _amount_id(doc.get("current_pays"))
    opened = _amount_id(doc.get("open_pays"))
    if current is None or opened is None:
        return None
    if "current_orderid" not in doc or "open_orderid" not in doc:
        return None
    return (
        _norm_id(doc.get("current_orderid")),
        _norm_id(doc.get("open_orderid")),
        doc.get("current_owner"),
        doc.get("open_owner"),
        current,
        opened,
    )


async def _fill_booked_under_other_index(
    op: FillOrder, other_slots: dict[int, str]
) -> dict | None:
    """Return the ledger row when this fill is already booked at another index.

    ``other_slots`` maps a booked slot to one of its ledger ``group_id``s.
    The hive_ops row for that slot carries the order ids and amounts.
    Missing hive_ops means we cannot prove it is the same fill, so the
    caller books the incoming op instead of dropping it.
    """
    incoming = fill_exchange_identity(op)
    if incoming is None or not other_slots:
        return None
    cursor = FillOrder.collection().find(
        {
            "block_num": op.block_num,
            "trx_id": op.trx_id,
            "realm": f"{op.realm}",
            "type": "fill_order",
        },
        projection={
            "op_in_trx": 1,
            "current_orderid": 1,
            "open_orderid": 1,
            "current_owner": 1,
            "open_owner": 1,
            "current_pays": 1,
            "open_pays": 1,
        },
    )
    hive_docs = await cursor.to_list(length=None)
    incoming_slot = _ledger_op_slot(op.op_in_trx)
    for hive_doc in hive_docs:
        if _stored_fill_identity(hive_doc) != incoming:
            continue
        try:
            slot = _ledger_op_slot(int(hive_doc.get("op_in_trx")))
        except (TypeError, ValueError):
            continue
        if slot == incoming_slot or slot not in other_slots:
            continue
        group_id = other_slots[slot]
        logger.info(
            f"Fill already booked for {op.trx_id} as {group_id}; "
            f"incoming {op.group_id} is the same exchange fill.",
            extra={"notification": False, **op.log_extra},
        )
        return await LedgerEntry.collection().find_one(filter={"group_id": group_id})
    return None


async def find_existing_hive_ledger_entry(op: HiveLedgerOp) -> dict | None:
    """Decide whether this Hive op is new, already booked, or behind the trx.

    One prefix read of ``group_id``. A transaction has a handful of ledger
    rows, and the field is only the id, so this stays a short index range
    even though every Hive op pays for it.

    Returns the stored ledger document when this same slot is already
    booked, including ``{group_id}_{ledger_type}``. A historical index of
    ``0`` occupies slot 1. The caller raises ``LedgerEntryDuplicateException``.

    A ``fill_order`` is bookkeeping for a match that already happened on
    Hive. Virtual fills of one transaction are handled concurrently, so a
    higher ``op_in_trx`` can be stored before a lower one. That is not a
    duplicate. The same fill is a duplicate only when its slot is booked,
    or when the same orders, accounts, and amounts are already booked
    under another index of this block, trx, and realm. A rescan then does
    not ingest the exchange again, and a fill that was skipped is still
    booked.

    Other Hive ops keep the order check. A transfer can cause an outbound
    payment, and a later booked index still means this read is behind a
    batch that already ran. Logs a warning and raises
    ``HiveLedgerOrderMismatch``.
    """
    cursor = LedgerEntry.collection().find(
        filter=hive_ledger_trx_filter(op),
        projection={"group_id": 1},
    )
    docs = await cursor.to_list(length=None)
    incoming = _ledger_op_slot(op.op_in_trx)
    same_slot: list[str] = []
    later: list[int] = []
    other_slots: dict[int, str] = {}
    for doc in docs:
        op_index = booked_op_in_trx(doc.get("group_id", ""), op)
        if op_index is None:
            continue
        slot = _ledger_op_slot(op_index)
        group_id = doc.get("group_id")
        if not isinstance(group_id, str):
            continue
        if slot == incoming:
            same_slot.append(group_id)
        else:
            other_slots.setdefault(slot, group_id)
            if slot > incoming:
                later.append(slot)
    if same_slot:
        group_id = op.group_id if op.group_id in same_slot else same_slot[0]
        return await LedgerEntry.collection().find_one(filter={"group_id": group_id})

    if isinstance(op, FillOrder):
        already = await _fill_booked_under_other_index(op, other_slots)
        if already is not None:
            return already
        return None

    if later:
        realm = f"{op.realm}"
        message = (
            f"Hive trx {op.trx_id} block {op.block_num} {realm} already has "
            f"later ledger op(s) {sorted(set(later))} booked, "
            f"but incoming op_in_trx is {op.op_in_trx} ({op.group_id}). "
            "Dropping: this transaction was already processed past this operation."
        )
        logger.warning(
            message,
            extra={"notification": True, **op.log_extra},
        )
        raise HiveLedgerOrderMismatch(message)
    return None


async def process_hive_op(op: TrackedAny, nobroadcast: bool = False) -> list[LedgerEntry]:
    """
    Processes the transfer operation and creates a ledger entry if applicable.
    Returns a list of entries even though this is likely to be only one.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If this
    operation is already booked, raises LedgerEntryDuplicateException. A fill_order is
    booked whenever that fill is not already stored, including when a higher op_in_trx
    of the same transaction landed first. For every other Hive op, a later index already
    booked in the same block, transaction, and realm logs a warning and raises
    HiveLedgerOrderMismatch.

    Args:
        op (TrackedAny): The operation to process, which can be a transfer, limit order, fill order, or custom JSON.
        nobroadcast (bool): If True, suppresses broadcasting used mostly for testing purposes.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.

    Raises:
        HiveLedgerOrderMismatch: A non-fill op arrived behind a later booked
            index of the same transaction. The caller records the drop and
            does not log it as a successful booking.
        LedgerEntryDuplicateException: This slot, or this same market fill, is
            already booked.
    """
    # These operations are the ones that create ledger rows. Their group_id
    # uses op_in_trx from the reader, and that number counts every virtual op
    # seen in the transaction, not only this tuple. Adding or removing one,
    # especially a virtual op, changes those numbers. Do not replay or re-scan
    # history after changing this set: a higher index of a non-fill is booked
    # again. A fill_order rescan is matched on the fill itself as well as the
    # slot, so a renumbered fill is not booked twice.
    # HiveLedgerOp must stay the same tuple.
    if not isinstance(
        op,
        (TransferBase, LimitOrderCreate, FillOrder, LimitOrderCancelled, CustomJson),
    ):
        logger.warning(
            f"Unsupported operation type: {type(op)} {op}",
            extra={"notification": False, **op.log_extra},
        )
        return []

    existing_entry = await find_existing_hive_ledger_entry(op)
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
        elif isinstance(op, (LimitOrderCreate, FillOrder, LimitOrderCancelled)):
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
        logger.warning(
            f"Custom JSON retry error: {e}",
            extra={"notification": True, **op.log_extra},
        )
        raise

    except LedgerEntryException as e:
        if "Transfer between untracked accounts" in str(e):
            logger.info(
                f"Transfer between untracked accounts, no ledger entry created: {e}",
                extra={"notification": False, **op.log_extra},
            )
            return []
        logger.error(
            f"Error processing transfer operation: {e}",
            extra={"notification": True, **op.log_extra},
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
        timestamp=datetime.now(tz=UTC),
        link=hive_transfer.link,
    )
    processed_d_memo = lightning_memo(hive_transfer.d_memo)
    base_description = f"{hive_transfer.amount_str} from {hive_transfer.from_account} to {hive_transfer.to_account} {processed_d_memo}"
    hive_config = InternalConfig().config.hive_config
    exchange_config = InternalConfig().config.exchange_config

    server_account, treasury_account, funding_account, exchange_account = (
        hive_config.all_account_names
    )
    server_accounts = hive_config.server_account_names
    funding_accounts = hive_config.funding_account_names
    treasury_accounts = hive_config.treasury_account_names
    exchange_accounts = exchange_config.all_hive_exchange_accounts()

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

    if (
        (
            hive_transfer.from_account == BALANCE_ADJUSTMENT_ACCOUNT
            or hive_transfer.to_account == BALANCE_ADJUSTMENT_ACCOUNT
        )
        and hive_transfer.d_memo
        and BALANCE_ADJUSTMENT_MEMO in hive_transfer.d_memo
    ):
        # Do nothing this is a way to adjust balances without creating ledger entries that affect the P&L
        logger.warning(
            f"Balance adjustment transfer detected: {base_description}",
            extra={"notification": True, **hive_transfer.log_extra},
        )
        raise LedgerEntryCreationException(
            f"Balance Adjustment Transfer detected: {base_description}"
        )

    # MARK: Server to Treasury
    elif (
        hive_transfer.from_account in server_accounts
        and hive_transfer.to_account in treasury_accounts
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=hive_transfer.to_account)
        ledger_entry.credit = AssetAccount(
            name="Customer Deposits Hive", sub=hive_transfer.from_account
        )
        ledger_entry.description = f"Server to Treasury transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.SERVER_TO_TREASURY
    # MARK: Treasury to Server
    elif (
        hive_transfer.from_account in treasury_accounts
        and hive_transfer.to_account in server_accounts
    ):
        ledger_entry.debit = AssetAccount(
            name="Customer Deposits Hive", sub=hive_transfer.to_account
        )
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=hive_transfer.from_account)
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
        hive_transfer.from_account in funding_accounts
        and hive_transfer.to_account in treasury_accounts
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=hive_transfer.to_account)
        ledger_entry.credit = LiabilityAccount(
            name="Owner Loan Payable", sub=hive_transfer.from_account
        )
        ledger_entry.description = f"Funding to Treasury transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.FUNDING
    # MARK: Treasury to Funding
    elif (
        hive_transfer.from_account in treasury_accounts
        and hive_transfer.to_account in funding_accounts
    ):
        ledger_entry.debit = LiabilityAccount(
            name="Owner Loan Payable", sub=hive_transfer.to_account
        )
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=hive_transfer.from_account)
        ledger_entry.description = f"Treasury to Funding transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_FUNDING
    # MARK: Treasury to Exchange
    elif (
        hive_transfer.from_account in treasury_accounts
        and hive_transfer.to_account in exchange_accounts
    ):
        # Use the configured exchange adapter name as the Exchange Holdings sub-account
        provider_name = exchange_config.get_provider_name_from_hive_account(
            hive_transfer.to_account
        )
        exchange_sub = get_exchange_adapter(provider_name).exchange_name
        ledger_entry.cust_id = exchange_sub
        ledger_entry.debit = AssetAccount(name="Exchange Holdings", sub=exchange_sub)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=hive_transfer.from_account)
        ledger_entry.description = f"Treasury to Exchange transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_EXCHANGE
    # MARK: Server to Exchange
    elif (
        hive_transfer.from_account in server_accounts
        and hive_transfer.to_account in exchange_accounts
    ):
        provider_name = exchange_config.get_provider_name_from_hive_account(
            hive_transfer.to_account
        )
        exchange_sub = get_exchange_adapter(provider_name).exchange_name
        ledger_entry.cust_id = exchange_sub
        ledger_entry.debit = AssetAccount(name="Exchange Holdings", sub=exchange_sub)
        ledger_entry.credit = AssetAccount(
            name="Customer Deposits Hive", sub=hive_transfer.from_account
        )
        ledger_entry.description = f"Server to Exchange transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.SERVER_TO_EXCHANGE
    # MARK: Exchange to Treasury
    elif (
        hive_transfer.from_account in exchange_accounts
        and hive_transfer.to_account in treasury_accounts
    ):
        provider_name = exchange_config.get_provider_name_from_hive_account(
            hive_transfer.from_account
        )
        exchange_sub = get_exchange_adapter(provider_name).exchange_name
        ledger_entry.cust_id = exchange_sub
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=hive_transfer.to_account)
        ledger_entry.credit = AssetAccount(name="Exchange Holdings", sub=exchange_sub)
        ledger_entry.description = f"Exchange to Treasury transfer: {base_description}"
        ledger_entry.user_memo = lightning_memo(hive_transfer.user_memo)
        ledger_entry.ledger_type = LedgerType.EXCHANGE_TO_TREASURY

    # MARK: Expense Payments
    elif (
        hive_transfer.from_account in treasury_accounts
        and hive_transfer.to_account in expense_accounts
    ):
        expense_account = hive_config.hive_accs.get(hive_transfer.to_account, None)
        # Need to check from account but all expenses must be paid from Treasury account.
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
    elif hive_transfer.from_account in server_accounts:
        customer = hive_transfer.to_account
        server = hive_transfer.from_account
        ledger_entry.debit = LiabilityAccount(name="VSC Liability", sub=customer)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.description = f"Withdrawal: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_OUT

        # MARK: Server to suspicious account holding v4vapp.sus
        if hive_transfer.to_account == "v4vapp.sus":
            # This is the balancing transaction for the suspicious account hold
            follow_on_task = suspicious_account_transfer_accounting(hive_transfer=hive_transfer)

    # MARK: Customer account to server account deposit
    elif hive_transfer.to_account in server_accounts:
        customer = hive_transfer.from_account
        server = hive_transfer.to_account
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.credit = LiabilityAccount(name="VSC Liability", sub=customer)
        ledger_entry.description = f"Deposit: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_IN
        ledger_entry.user_memo = lightning_memo(hive_transfer.user_memo)
        # Now we need to see if we can take action for this invoice
        # This will be handled in a separate task
        if hive_transfer.balance_request:
            follow_on_task = reply_with_balance_request(
                transfer=hive_transfer, nobroadcast=nobroadcast
            )
        else:
            follow_on_task = follow_on_transfer(tracked_op=hive_transfer, nobroadcast=nobroadcast)

    else:
        logger.info(
            f"Transfer between two different accounts: {hive_transfer.from_account} -> {hive_transfer.to_account} {hive_transfer.short_id}, no ledger entry created.",
            extra={"notification": False, **hive_transfer.log_extra},
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
            timestamp=datetime.now(tz=UTC),
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
