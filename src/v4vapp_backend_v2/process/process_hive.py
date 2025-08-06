from datetime import datetime, timezone
from typing import List, Union

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import (
    LedgerEntry,
    LedgerEntryCreationException,
    LedgerEntryDuplicateException,
    LedgerEntryException,
    LedgerType,
)
from v4vapp_backend_v2.actions.depreciated_custom_json_to_lnd import (
    process_custom_json_to_lightning,
)
from v4vapp_backend_v2.actions.tracked_any import TrackedAny, TrackedTransfer, load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import lightning_memo
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.process.process_custom_json import custom_json_internal_transfer
from v4vapp_backend_v2.process.process_errors import CustomJsonToLightningError, HiveLightningError
from v4vapp_backend_v2.process.process_transfer import follow_on_transfer

# MARK: Hive Transaction Processing


async def process_hive_op(op: TrackedAny) -> List[LedgerEntry]:
    """
    Processes the transfer operation and creates a ledger entry if applicable.
    Returns a list of entries even though this is likely to be only one.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

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
        ledger_entry: LedgerEntry | None = None
        if isinstance(op, TransferBase):
            ledger_entry = await process_transfer_op(hive_transfer=op)
        elif isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder):
            ledger_entry = await process_create_fill_order_op(limit_fill_order=op)
        elif isinstance(op, CustomJson):
            ledger_entry = await process_custom_json(custom_json=op)
        return [ledger_entry] if ledger_entry else []

    except LedgerEntryException as e:
        logger.error(f"Error processing transfer operation: {e}")
        return []
        # raise LedgerEntryCreationException(f"Error processing transfer operation: {e}") from e

    except HiveLightningError as e:
        logger.error(f"Hive to Lightning error: {e}")
        return []


async def process_transfer_op(hive_transfer: TrackedTransfer) -> LedgerEntry:
    """
    Processes a Hive transfer operation and creates a ledger entry if applicable.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    ledger_entry = LedgerEntry(
        group_id=hive_transfer.group_id,
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        user_memo=hive_transfer.user_memo,
        timestamp=datetime.now(tz=timezone.utc),
    )
    expense_accounts = ["privex"]
    processed_d_memo = lightning_memo(hive_transfer.d_memo)
    base_description = f"{hive_transfer.amount_str} from {hive_transfer.from_account} to {hive_transfer.to_account} {processed_d_memo}"
    hive_config = InternalConfig().config.hive
    server_account, treasury_account, funding_account, exchange_account = (
        hive_config.all_account_names
    )
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

    # MARK: Server to customer account withdrawal
    if hive_transfer.from_account == server_account:
        customer = hive_transfer.to_account
        server = hive_transfer.from_account
        ledger_entry.debit = LiabilityAccount("Customer Liability", sub=customer)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.description = f"Withdrawal: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_OUT
        # TODO: There is an argument to say that this hive_transfer should be noted as being connected to the prior event.

    # MARK: Customer account to server account deposit
    elif hive_transfer.to_account == server_account:
        customer = hive_transfer.from_account
        server = hive_transfer.to_account
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.credit = LiabilityAccount("Customer Liability", sub=customer)
        ledger_entry.description = f"Deposit: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_IN
        # Now we need to see if we can take action for this invoice
        # This will be handled in a separate task
        follow_on_task = follow_on_transfer(tracked_op=hive_transfer)

    # MARK: Server to Treasury
    elif (
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
    # MARK: Funding to Treasury
    elif (
        hive_transfer.from_account == funding_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=funding_account
        )
        ledger_entry.description = f"Funding to Treasury transfer: {base_description}"
    # MARK: Treasury to Funding
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == funding_account
    ):
        ledger_entry.debit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=treasury_account
        )
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
        # MARK: Exchange to Treasury
    elif (
        hive_transfer.from_account == exchange_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Exchange Deposits Hive", sub=treasury_account)
        ledger_entry.description = f"Exchange to Treasury transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.EXCHANGE_TO_TREASURY
        # MARK: Payments to special expense accounts if
    elif (
        hive_transfer.from_account == server_account
        and hive_transfer.to_account in expense_accounts
    ):
        # TODO: #110 Implement the system for expense accounts
        raise NotImplementedError("External expense accounts not implemented yet")
    else:
        logger.info(
            f"Transfer between two different accounts: {hive_transfer.from_account} -> {hive_transfer.to_account}"
        )
        raise LedgerEntryCreationException("Transfer between untracked accounts.")
    await ledger_entry.save()

    if follow_on_task:
        # If there is a follow-on task, we need to run it in the background
        try:
            await follow_on_task
        except Exception as e:
            logger.exception(f"Follow-on task failed: {e}", extra={"notification": False})

    return ledger_entry


async def process_create_fill_order_op(
    limit_fill_order: Union[LimitOrderCreate, FillOrder],
) -> LedgerEntry:
    """
    Processes the create or fill order operation and creates a ledger entry if applicable.

    This method handles various types of orders, including limit orders and fill orders. It ensures that
    appropriate debit and credit accounts are assigned based on the order type. If a ledger entry with
    the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    ledger_entry = LedgerEntry(
        group_id=limit_fill_order.group_id,
        short_id=limit_fill_order.short_id,
        timestamp=limit_fill_order.timestamp,
        op_type=limit_fill_order.op_type,
    )
    if isinstance(limit_fill_order, LimitOrderCreate):
        logger.info(f"Limit order create: {limit_fill_order.orderid}")
        if not limit_fill_order.conv or limit_fill_order.conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.conv = CryptoConv(
                conv_from=limit_fill_order.amount_to_sell.unit,  # HIVE
                value=limit_fill_order.amount_to_sell.amount_decimal,  # 25.052 HIVE
                converted_value=limit_fill_order.min_to_receive.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        ledger_entry.debit = AssetAccount(name="Escrow Hive", sub=limit_fill_order.owner)
        ledger_entry.credit = AssetAccount(
            name="Customer Deposits Hive", sub=limit_fill_order.owner
        )
        ledger_entry.description = limit_fill_order.ledger_str
        ledger_entry.ledger_type = LedgerType.LIMIT_ORDER_CREATE
        ledger_entry.debit_unit = ledger_entry.credit_unit = limit_fill_order.amount_to_sell.unit
        ledger_entry.debit_amount = ledger_entry.credit_amount = (
            limit_fill_order.amount_to_sell.amount_decimal
        )
        ledger_entry.debit_conv = ledger_entry.credit_conv = limit_fill_order.conv
    elif isinstance(limit_fill_order, FillOrder):
        logger.info(
            f"Fill order operation: {limit_fill_order.open_orderid} {limit_fill_order.current_owner}"
        )
        if not limit_fill_order.debit_conv or limit_fill_order.debit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.debit_conv = CryptoConv(
                conv_from=limit_fill_order.open_pays.unit,  # HIVE
                value=limit_fill_order.open_pays.amount_decimal,  # 25.052 HIVE
                converted_value=limit_fill_order.current_pays.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        if not limit_fill_order.credit_conv or limit_fill_order.credit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.credit_conv = CryptoConv(
                conv_from=limit_fill_order.current_pays.unit,  # HBD
                value=limit_fill_order.current_pays.amount_decimal,  # 6.738 HBD
                converted_value=limit_fill_order.open_pays.amount_decimal,  # 25.052 HIVE
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        ledger_entry.debit = AssetAccount(
            name="Customer Deposits Hive", sub=limit_fill_order.current_owner
        )
        ledger_entry.credit = AssetAccount(name="Escrow Hive", sub=limit_fill_order.current_owner)
        ledger_entry.description = limit_fill_order.ledger_str
        ledger_entry.ledger_type = LedgerType.FILL_ORDER
        ledger_entry.debit_unit = limit_fill_order.open_pays.unit  # HIVE (received)
        ledger_entry.credit_unit = limit_fill_order.current_pays.unit  # HBD (given)
        ledger_entry.debit_amount = limit_fill_order.open_pays.amount_decimal  # 25.052 HIVE
        ledger_entry.credit_amount = limit_fill_order.current_pays.amount_decimal  # 6.738 HBD
        ledger_entry.debit_conv = limit_fill_order.debit_conv  # Conversion for HIVE
        ledger_entry.credit_conv = limit_fill_order.credit_conv  # Conversion for HBD
    else:
        logger.error(f"Unsupported operation type: {type(limit_fill_order)}")
        raise LedgerEntryCreationException("Unsupported operation type.")
    return ledger_entry


# MARK: CustomJson Operations
async def process_custom_json(custom_json: CustomJson) -> LedgerEntry | None:
    """
    Processes a CustomJson operation and creates a ledger entry if applicable.

    This method handles CustomJson operations, ensuring that appropriate debit and credit
    accounts are assigned based on the operation type. If a ledger entry with the same group_id
    already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    if custom_json.cj_id in ["v4vapp_dev_transfer", "v4vapp_transfer"]:
        keepsats_transfer = KeepsatsTransfer.model_validate(custom_json.json_data)
        # MARK: CustomJson Transfer user to user
        if (
            custom_json.from_account
            and custom_json.to_account
            and keepsats_transfer.msats
            and custom_json.from_account != custom_json.to_account
        ):
            ledger_entry = await custom_json_internal_transfer(
                custom_json=custom_json, keepsats_transfer=keepsats_transfer
            )
            # Check for a parent id to see if this is a reply transaction
            if keepsats_transfer.parent_id:
                # This is a reply transaction, we need to process it as such
                parent_op = await load_tracked_object(tracked_obj=keepsats_transfer.parent_id)
                if parent_op:
                    parent_op.add_reply(
                        reply_id=custom_json.group_id_p,
                        reply_type=custom_json.op_type,
                        reply_msat=keepsats_transfer.msats,
                        reply_message="Reply to transfer",
                    )

            return ledger_entry
        # MARK: CustomJson Pay a lightning invoice
        # If this has a memo that should contain the invoice and the instructions like "#clean"
        # invoice_message we will use to send on if we generate an invoice form a lightning address
        elif keepsats_transfer.memo and not keepsats_transfer.to_account:
            # This is a transfer operation, we need to process it as such

            if not custom_json.conv or custom_json.conv.is_unset():
                await custom_json.update_conv()
                if custom_json.conv.is_unset():
                    raise LedgerEntryCreationException(
                        "Conversion not set in CustomJson operation."
                    )
            try:
                ledger_type = LedgerType.CUSTOM_JSON_TRANSFER
                custom_json_ledger_entry = LedgerEntry(
                    cust_id=custom_json.cust_id,
                    short_id=custom_json.short_id,
                    ledger_type=ledger_type,
                    group_id=f"{custom_json.group_id}",  # The inital recording of an inbound Hive transaction does not have ledger_type
                    timestamp=datetime.now(tz=timezone.utc),
                    description=keepsats_transfer.description,
                    user_memo=keepsats_transfer.user_memo,
                    op_type=custom_json.op_type,
                    debit=LiabilityAccount(name="Customer Liability", sub=custom_json.cust_id),
                    debit_conv=custom_json.conv,
                    debit_unit=Currency.MSATS,
                    debit_amount=custom_json.conv.msats,
                    credit=LiabilityAccount(name="Customer Liability", sub=custom_json.cust_id),
                    credit_conv=custom_json.conv,
                    credit_unit=Currency.MSATS,
                    credit_amount=custom_json.conv.msats,
                )
                await custom_json_ledger_entry.save()

                await process_custom_json_to_lightning(
                    custom_json=custom_json,
                    keepsats_transfer=keepsats_transfer,
                )
                return custom_json_ledger_entry

            except CustomJsonToLightningError as e:
                logger.error(f"Error processing CustomJson to Lightning: {e}")
                raise LedgerEntryCreationException(
                    f"Error processing CustomJson to Lightning: {e}"
                ) from e

            except Exception as e:
                logger.error(f"Failed to process CustomJson to Lightning: {e}")
                raise LedgerEntryCreationException(
                    f"Failed to process CustomJson to Lightning: {e}"
                ) from e

    if custom_json.cj_id in ["v4vapp_dev_notification"]:
        logger.info(f"Notification CustomJson: {custom_json.json_data.memo}")
        return None

    logger.error(
        f"CustomJson operation not implemented for v4vapp_group_id: {custom_json.group_id}.",
        extra={"notification": False},
    )
    raise NotImplementedError(
        f"Some other custom_json functionality which hasn't been implemented yet {custom_json.group_id}."
    )
