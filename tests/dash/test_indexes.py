from v4vapp_backend_v2.dash.collections import COL_INVOICES, COL_PAYOUTS, COL_WALLET_STATE
from v4vapp_backend_v2.dash.db.indexes import COLLECTION_INDEXES, INVOICE_INDEXES, PAYOUT_INDEXES


def _names(models: list) -> list[str]:
    return [m.document["name"] for m in models]


def test_invoice_index_names_and_uniques() -> None:
    assert _names(INVOICE_INDEXES) == [
        "external_id",
        "address",
        "state_expires",
        "cust_id",
        "cust_state_settled",
        "created_at",
        "lightning_invoice",
    ]
    by_name = {m.document["name"]: m.document for m in INVOICE_INDEXES}
    assert by_name["external_id"]["unique"] is True
    assert by_name["address"]["unique"] is True
    assert by_name["lightning_invoice"]["unique"] is True
    assert by_name["lightning_invoice"]["sparse"] is True
    assert "unique" not in by_name["state_expires"]


def test_payout_txid_is_sparse_unique() -> None:
    by_name = {m.document["name"]: m.document for m in PAYOUT_INDEXES}
    assert by_name["txid"]["unique"] is True
    assert by_name["txid"]["sparse"] is True


def test_collection_map() -> None:
    assert set(COLLECTION_INDEXES) == {COL_INVOICES, COL_WALLET_STATE, COL_PAYOUTS}
    assert COLLECTION_INDEXES[COL_WALLET_STATE] == []
