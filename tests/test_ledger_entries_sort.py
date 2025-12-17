import asyncio

from v4vapp_backend_v2.accounting.ledger_entries import get_ledger_entries
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry


class FakeCursor:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class FakeCollection:
    def __init__(self):
        self.find_kwargs = None

    def find(self, **kwargs):
        self.find_kwargs = kwargs
        return FakeCursor()


def test_get_ledger_entries_uses_descending_sort(monkeypatch):
    fc = FakeCollection()
    monkeypatch.setattr(LedgerEntry, "collection", lambda: fc)

    # Call the async function
    asyncio.run(get_ledger_entries())

    assert fc.find_kwargs is not None
    assert "sort" in fc.find_kwargs
    assert fc.find_kwargs["sort"] == [("timestamp", -1)]
