from decimal import Decimal

try:
    from bson.decimal128 import Decimal128
    from bson.objectid import ObjectId
except Exception:  # pragma: no cover
    Decimal128 = None
    ObjectId = None

from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent


class DummyCursor:
    def __init__(self, results):
        self._results = results

    async def to_list(self, length=None):
        return self._results


class DummyCollection:
    def __init__(self, results):
        self._results = results

    def aggregate(self, pipeline):
        return DummyCursor(self._results)


async def test_pending_for_ledger_inclusion_removes_objectid(monkeypatch):
    if Decimal128 is None or ObjectId is None:
        return

    obj_id = ObjectId()
    results = [
        {
            "total_fee": Decimal128("14.375"),
            "pending_fees": [
                {
                    "_id": obj_id,
                    "htlc_id": 5244,
                    "amount": Decimal128("29886.435"),
                    "fee": Decimal128("14.375"),
                    "fee_percent": Decimal128("0.048"),
                    "htlc_event_dict": {"timestamp_ns": Decimal128("1766678812771294166")},
                }
            ],
        }
    ]

    monkeypatch.setattr(
        TrackedForwardEvent, "collection", classmethod(lambda cls: DummyCollection(results))
    )

    total_fee, pending = await TrackedForwardEvent.pending_for_ledger_inclusion()

    assert isinstance(total_fee, Decimal)
    assert len(pending) == 1
    # model_validate should have converted ObjectId to string for _id
    assert isinstance(pending[0].id, str)
    assert pending[0].amount == Decimal("29886.435")
