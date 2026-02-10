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
    """
    Container for in-progress hold/release aggregates keyed by customer ID.
    This class builds a mapping from cust_id to InProgressResult objects from an
    iterable of document-like mappings. Each input document is expected to include
    the keys 'cust_id', 'hold_total', 'release_total', and 'net_held'. Numeric
    values for the totals are converted to Decimal when constructing the
    InProgressResult instances.
    Args:
        results (List[Mapping[str, Any]]): Sequence of documents used to populate
            the internal mapping. Each mapping must contain the required keys
            described above.
    Attributes:
        results (Dict[CustIDType, InProgressResult]): Mapping from customer IDs to
            their corresponding InProgressResult objects.
    Methods:
        get(cust_id: CustIDType) -> InProgressResult:
            Return the InProgressResult for the given cust_id. If the customer ID
            is not present in the internal mapping, a new InProgressResult with
            zeroed Decimal totals is returned.
        get_net_held(cust_id: CustIDType) -> Decimal:
            Convenience accessor that returns the net_held Decimal for the given
            cust_id; returns Decimal(0) when the customer is not present.
    Example:
        docs = [
            {'cust_id': 'cust-1', 'hold_total': '100.00', 'release_total': '40.00', 'net_held': '60.00'},
        ]
        ipr = InProgressResults(docs)
        ipr.get('cust-1').net_held  # Decimal('60.00')
    """

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
        """Return the InProgressResult for the given customer ID.
        Looks up `cust_id` in `self.results` and returns the associated InProgressResult. If no entry exists,
        returns a new InProgressResult initialized with the provided `cust_id` and zero monetary totals
        (`hold_total`, `release_total`, `net_held` all set to Decimal(0)).
        Args:
            cust_id (CustIDType): The customer identifier to look up.
        Returns:
            InProgressResult: The existing InProgressResult for `cust_id`, or a newly created one with
            zeroed totals when absent.
        Notes:
            - The default InProgressResult returned when the key is missing is not inserted into `self.results`.
            - Monetary fields use Decimal(0) for precise zero initialization.
            - May raise exceptions propagated from the underlying mapping lookup (e.g., AttributeError if
              `self.results` is not present or not a mapping).
        """

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


# @async_time_decorator
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
