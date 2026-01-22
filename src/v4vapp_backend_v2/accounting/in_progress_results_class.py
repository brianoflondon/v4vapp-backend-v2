from dataclasses import dataclass
from decimal import Decimal
from typing import Any, List, Mapping

from v4vapp_backend_v2.accounting.account_balance_pipelines import all_held_msats_balance_pipeline
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.decorators import async_time_decorator
from v4vapp_backend_v2.process.lock_str_class import CustIDType


@dataclass
class InProgressResult:
    cust_id: CustIDType
    hold_total: Decimal
    release_total: Decimal
    net_held: Decimal


@dataclass
class InProgressResults:
    def __init__(self, results: List[Mapping[str, Any]]):
        self.results = {
            doc["cust_id"]: InProgressResult(
                cust_id=doc["cust_id"],
                hold_total=Decimal(doc["hold_total"]),
                release_total=Decimal(doc["release_total"]),
                net_held=Decimal(doc["net_held"]),
            )
            for doc in results
        }

    def get(self, cust_id: CustIDType) -> InProgressResult:
        return self.results.get(
            cust_id,
            InProgressResult(
                cust_id=cust_id,
                hold_total=Decimal(0),
                release_total=Decimal(0),
                net_held=Decimal(0),
            ),
        )

    def get_net_held(self, cust_id: CustIDType) -> Decimal:
        result = self.get(cust_id)
        return result.net_held


@async_time_decorator
async def all_held_msats() -> List[Mapping[str, Any]]:
    """
    Execute the aggregation pipeline that computes "held" balances in millisatoshis (msats)
    from ledger entries and return the resulting documents.

    This coroutine:
    - Constructs the aggregation pipeline by calling all_held_msats_balance_pipeline().
    - Runs the pipeline against the LedgerEntry collection.
    - Collects and returns all resulting documents as a list of mappings.

    Returns:
        List[Mapping[str, Any]]: A list of aggregation result documents. Each mapping
        corresponds to a document emitted by the pipeline (typically containing
        identifier fields and aggregated held balance fields in msats). The exact
        keys depend on the pipeline definition.

    Notes:
        - This function is asynchronous and must be awaited.
        - Database errors raised by the underlying driver will propagate to the caller.
    """
    in_progress_pipeline = all_held_msats_balance_pipeline()
    cursor = await LedgerEntry.collection().aggregate(in_progress_pipeline)
    results = await cursor.to_list(length=None)
    return results
