from datetime import datetime, timezone

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry

pytestmark = pytest.mark.integration
from v4vapp_backend_v2.admin.routers.ledger_entries import ledger_entries_data


class DummyCursor:
    def __init__(self, items):
        self._items = items

    def __aiter__(self):
        self._it = iter(self._items)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration

    # Support chainable cursor methods used by the real MongoDB cursor
    def sort(self, *args, **kwargs):
        return self

    def skip(self, n):
        return self

    def limit(self, n):
        return self


class DummyCollection:
    def __init__(self, items):
        self._items = items

    async def count_documents(self, *_args, **_kwargs):
        return len(self._items)

    def find(self, *args, **kwargs):
        return DummyCursor(self._items)


@pytest.mark.asyncio
async def test_ledger_entries_data_includes_op_type(monkeypatch):
    # Create a dummy validated LedgerEntry-like object
    class DummyEntry:
        def __init__(self):
            self.group_id = "g1"
            self.short_id = "s1"
            self.timestamp = datetime.now(tz=timezone.utc)
            self.ledger_type = None
            self.ledger_type_str = ""
            self.description = "d"
            self.link = ""
            self.cust_id = "c"
            # Provide minimal debit/credit account-like objects to satisfy serialization
            AccountLike = type(
                "AccountLike", (), {"name": "", "sub": "", "account_type": "", "contra": False}
            )
            self.debit = AccountLike()
            self.credit = AccountLike()
            self.user_memo = "m"
            self.journal = "j"
            self.op_type = "MY_OP"

        def print_journal_entry(self):
            return self.journal

    dummy = DummyEntry()

    dummy_doc = {"_id": "x"}

    fake_collection = DummyCollection([dummy_doc])

    # Patch collection and model_validate to return our DummyEntry
    monkeypatch.setattr(LedgerEntry, "collection", lambda: fake_collection)
    monkeypatch.setattr(LedgerEntry, "model_validate", lambda _x: dummy)

    resp = await ledger_entries_data()
    assert resp.status_code == 200
    body = resp.body.decode()
    # FastAPI JSON has no extra whitespace; check without spaces
    assert '"op_type":"MY_OP"' in body


@pytest.mark.asyncio
async def test_from_date_parsing_accepts_local_and_offset(monkeypatch):
    """Ensure ledger_entries_data accepts naive local ISO datetimes and offset datetimes"""
    fake_collection = DummyCollection([])
    monkeypatch.setattr(LedgerEntry, "collection", lambda: fake_collection)

    # naive local datetime (from datetime-local input)
    resp = await ledger_entries_data(from_date_str="2025-12-18T10:00")
    assert resp.status_code == 200

    # explicit offset included
    resp = await ledger_entries_data(from_date_str="2025-12-18T10:00-05:00")
    assert resp.status_code == 200
