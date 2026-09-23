from enum import StrEnum, auto
from typing import Any, Dict

from pydantic import BaseModel

from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccountAny
from v4vapp_backend_v2.hive_models.real_virtual_ops import HIVE_REAL_OPS, HIVE_VIRTUAL_OPS

# This list needs to be synced with op_all.py
OP_TRACKED = [
    "custom_json",
    "transfer",
    "account_witness_vote",
    "producer_reward",
    "fill_order",
    "limit_order_create",
    "limit_order_cancelled",
    "update_proposal_votes",
    "account_update2",
    "fill_recurrent_transfer",
    "recurrent_transfer",
    "producer_missed",
]

# I don't think I need the recurrent_transfer_cancel op because it doesn't affect me.


class OpRealm(StrEnum):
    REAL = auto()
    VIRTUAL = auto()
    MARKER = auto()


class HiveExp(StrEnum):
    HiveHub = "https://hivehub.dev/{prefix_path}"
    HiveScanInfo = "https://hivescan.info/{prefix_path}"
    # HiveBlockExplorer = "https://hiveblockexplorer.com/{prefix_path}"
    # HiveExplorer = "https://hivexplorer.com/{prefix_path}"


# Virtual ops with no real transaction use this id. HiveScan has no page for it.
_ZERO_TRX_ID = "0000000000000000000000000000000000000000"


def get_hive_block_explorer_link(
    trx_id: str,
    block_explorer: HiveExp = HiveExp.HiveHub,
    markdown: bool = False,
    block_num: int = 0,
    op_in_trx: int = 0,
    realm: OpRealm = OpRealm.REAL,
    # any_op: OpBase | None = None,
) -> str:
    """
    Generate a Hive blockchain explorer URL for a given transaction ID.

    HiveHub links a real operation at ``tx/{trx_id}`` and a virtual operation at
    ``tx/{block}/{trx_id}/{op}``. HiveScan serves real transactions at
    ``tx/{trx_id}`` and blocks at ``block/{block_num}``. It has no virtual-op
    page, so a virtual op with a real transaction id links to that transaction
    and an all-zero transaction id links to the block.

    Args:
        trx_id (str): The transaction ID to include in the URL
        block_explorer (HiveExp): The blockchain explorer to use (defaults to HiveHub)

    Returns:
        str: The complete URL with the transaction ID inserted
    """
    zero_trx = not trx_id or trx_id == _ZERO_TRX_ID
    if (realm == OpRealm.VIRTUAL or zero_trx) and block_num:
        prefix = "tx/"
        path = f"{block_num}/{trx_id}/{op_in_trx}"
    elif trx_id:
        prefix = "tx/"
        path = f"{trx_id}"
    elif block_num:
        prefix = "b/"
        path = f"{block_num}"
    else:
        prefix = "tx/"
        path = f"{trx_id}"

    if block_explorer == HiveExp.HiveScanInfo:
        if zero_trx and block_num:
            prefix = "block/"
            path = f"{block_num}"
        elif realm == OpRealm.VIRTUAL and block_num and trx_id:
            prefix = "tx/"
            path = trx_id
        elif prefix == "b/":
            prefix = "block/"

    prefix_path = f"{prefix}{path}"

    link_html = block_explorer.value.format(prefix_path=prefix_path)
    if not markdown:
        return link_html
    markdown_link = f"[{block_explorer.name}]({link_html})"
    return markdown_link


def op_realm(op_type: str) -> OpRealm:
    """
    Determines the operational realm based on the provided operation type.

    Args:
        op_type (str): The type of operation to evaluate. Can be one of the
                       predefined operation types or None.

    Returns:
        OpRealm: The corresponding operational realm, which can be one of the
                 following:
                 - OpRealm.VIRTUAL: If the operation type is in HIVE_VIRTUAL_OPS.
                 - OpRealm.REAL: If the operation type is in HIVE_REAL_OPS.
                 - OpRealm.MARKER: If the operation type is "block_marker".
                 - None: If the operation type is None or does not match any
                         predefined types.
    """
    if op_type in HIVE_VIRTUAL_OPS:
        return OpRealm.VIRTUAL
    elif op_type in HIVE_REAL_OPS:
        return OpRealm.REAL
    elif op_type == "block_marker":
        return OpRealm.MARKER
    raise ValueError(f"Unknown operation type in op_realm check: {op_type}")


class OpLogData(BaseModel):
    """
    OpLogData is a Pydantic model that represents the structure of log data.

    Attributes:
        log (str): The main log message.
        notification (str): A notification message associated with the log.
        log_extra (Dict[str, Any]): Additional data or metadata related to the log.
    """

    log: str
    notification: str
    log_extra: Dict[str, Any]


class WatchPair(BaseModel):
    """
    WatchPair is a Pydantic model that represents a pair of accounts to watch.

    Attributes:
        from_account (str): The account from which the operation originates.
        to_account (str): The account to which the operation is directed.
    """

    from_account: str | None = None
    to_account: str | None = None
    ledger_debit: LedgerAccountAny | None = None
    ledger_credit: LedgerAccountAny | None = None
    ledger_fee: LedgerAccountAny | None = None

    def __init__(self, **data):
        super().__init__(**data)
