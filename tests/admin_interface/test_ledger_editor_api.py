"""
Ledger Editor API Tests

Tests for /api/load, /api/update, /api/create, /api/compute-conversion,
and /api/presets endpoints on the ledger editor router.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, ExpenseAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.admin.routers.ledger_editor import (
    _account_class_for_type,
    _build_account,
    _build_editor_presets,
    compute_conversion,
    create_entry,
    get_presets,
    load_entry,
    update_entry,
)
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dummy_entry(**overrides):
    """Build a minimal LedgerEntry-like object for mocking."""
    defaults = dict(
        group_id="test_g1",
        short_id="test_g1",
        timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        ledger_type=LedgerType.EXCHANGE_TO_NODE,
        description="Test entry",
        user_memo="memo",
        cust_id="voltage",
        cust_id_from="",
        cust_id_to="",
        debit=AssetAccount(name="External Lightning Payments", sub="voltage"),
        credit=AssetAccount(name="Exchange Holdings", sub="binance_convert"),
        debit_amount=Decimal("1000"),
        debit_unit=MagicMock(value="sats"),
        debit_conv=CryptoConv(),
        credit_amount=Decimal("1000"),
        credit_unit=MagicMock(value="sats"),
        credit_conv=CryptoConv(),
        reversed=None,
        extra_data=[{"source": "test"}],
        link="",
        op_type="ledger_entry",
    )
    defaults.update(overrides)

    entry = MagicMock()
    for k, v in defaults.items():
        setattr(entry, k, v)
    entry.log_extra = {"group_id": defaults["group_id"]}
    return entry


# ===================================================================
# /api/load
# ===================================================================


class TestLoadEntry:
    @pytest.mark.asyncio
    async def test_load_entry_not_found(self, monkeypatch):
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=None))
        resp = await load_entry(group_id="nonexistent")
        assert resp.status_code == 404
        body = json.loads(resp.body)
        assert "error" in body

    @pytest.mark.asyncio
    async def test_load_entry_success(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        resp = await load_entry(group_id="test_g1")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["group_id"] == "test_g1"
        assert body["description"] == "Test entry"
        assert body["cust_id"] == "voltage"
        assert body["debit"]["name"] == "External Lightning Payments"
        assert body["credit"]["name"] == "Exchange Holdings"
        assert body["reversed"] is None

    @pytest.mark.asyncio
    async def test_load_entry_reversed(self, monkeypatch):
        rev = datetime(2025, 7, 1, tzinfo=timezone.utc)
        entry = _make_dummy_entry(reversed=rev)
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        resp = await load_entry(group_id="test_g1")
        body = json.loads(resp.body)
        assert body["reversed"] is not None


# ===================================================================
# /api/compute-conversion
# ===================================================================


class TestComputeConversion:
    @pytest.mark.asyncio
    async def test_unknown_currency(self):
        resp = await compute_conversion(amount=100.0, currency="FAKE")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "Unknown currency" in body["error"]

    @pytest.mark.asyncio
    async def test_conversion_success(self, monkeypatch):
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv

        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )
        resp = await compute_conversion(amount=1000.0, currency="sats")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        # CryptoConv model_dump should produce a dict
        assert isinstance(body, dict)

    @pytest.mark.asyncio
    async def test_conversion_failure(self, monkeypatch):
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock(side_effect=RuntimeError("quote failed"))

        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )
        resp = await compute_conversion(amount=1000.0, currency="sats")
        assert resp.status_code == 500
        body = json.loads(resp.body)
        assert "error" in body


# ===================================================================
# /api/update
# ===================================================================


class TestUpdateEntry:
    @pytest.mark.asyncio
    async def test_update_missing_group_id(self):
        resp = await update_entry(payload={})
        assert resp.status_code == 400
        assert "group_id" in json.loads(resp.body)["error"]

    @pytest.mark.asyncio
    async def test_update_entry_not_found(self, monkeypatch):
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=None))
        resp = await update_entry(payload={"group_id": "missing"})
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_update_no_changes(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        resp = await update_entry(payload={"group_id": "test_g1"})
        assert resp.status_code == 400
        assert "No changes" in json.loads(resp.body)["error"]

    @pytest.mark.asyncio
    async def test_update_invalid_ledger_type(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        resp = await update_entry(
            payload={"group_id": "test_g1", "ledger_type": "TOTALLY_INVALID"}
        )
        assert resp.status_code == 400
        assert "Invalid ledger_type" in json.loads(resp.body)["error"]

    @pytest.mark.asyncio
    async def test_update_invalid_debit_account(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        resp = await update_entry(
            payload={
                "group_id": "test_g1",
                "debit": {"account_type": "BadType", "name": "x"},
            }
        )
        assert resp.status_code == 400
        assert "Invalid debit account" in json.loads(resp.body)["error"]

    @pytest.mark.asyncio
    async def test_update_invalid_credit_account(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        resp = await update_entry(
            payload={
                "group_id": "test_g1",
                "credit": {"account_type": "BadType", "name": "x"},
            }
        )
        assert resp.status_code == 400
        assert "Invalid credit account" in json.loads(resp.body)["error"]

    @pytest.mark.asyncio
    async def test_update_description_success(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))

        mock_result = MagicMock(modified_count=1, matched_count=1)
        mock_collection = MagicMock()
        mock_collection.update_one = AsyncMock(return_value=mock_result)
        monkeypatch.setattr(LedgerEntry, "collection", lambda: mock_collection)

        # Patch cache invalidation at its source module (lazy-imported inside update_entry)
        monkeypatch.setattr(
            "v4vapp_backend_v2.accounting.ledger_cache.invalidate_ledger_cache",
            AsyncMock(),
        )

        resp = await update_entry(payload={"group_id": "test_g1", "description": "Updated desc"})
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["status"] == "ok"
        assert body["modified"] == 1

    @pytest.mark.asyncio
    async def test_update_reversed_now(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))

        mock_result = MagicMock(modified_count=1, matched_count=1)
        mock_collection = MagicMock()
        mock_collection.update_one = AsyncMock(return_value=mock_result)
        monkeypatch.setattr(LedgerEntry, "collection", lambda: mock_collection)
        monkeypatch.setattr(
            "v4vapp_backend_v2.accounting.ledger_cache.invalidate_ledger_cache",
            AsyncMock(),
        )

        resp = await update_entry(payload={"group_id": "test_g1", "reversed": "now"})
        assert resp.status_code == 200
        # Verify the update_one was called with a datetime in $set
        call_args = mock_collection.update_one.call_args
        set_dict = call_args[0][1]["$set"]
        assert isinstance(set_dict["reversed"], datetime)

    @pytest.mark.asyncio
    async def test_update_reversed_clear(self, monkeypatch):
        entry = _make_dummy_entry(reversed=datetime.now(tz=timezone.utc))
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))

        mock_result = MagicMock(modified_count=1, matched_count=1)
        mock_collection = MagicMock()
        mock_collection.update_one = AsyncMock(return_value=mock_result)
        monkeypatch.setattr(LedgerEntry, "collection", lambda: mock_collection)
        monkeypatch.setattr(
            "v4vapp_backend_v2.accounting.ledger_cache.invalidate_ledger_cache",
            AsyncMock(),
        )

        resp = await update_entry(payload={"group_id": "test_g1", "reversed": None})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_update_reversed_invalid(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        resp = await update_entry(payload={"group_id": "test_g1", "reversed": "not-a-date"})
        assert resp.status_code == 400


# ===================================================================
# /api/create
# ===================================================================


class TestCreateEntry:
    def _valid_payload(self, **overrides):
        payload = {
            "debit": {
                "account_type": "Asset",
                "name": "External Lightning Payments",
                "sub": "voltage",
            },
            "credit": {
                "account_type": "Asset",
                "name": "Exchange Holdings",
                "sub": "binance_convert",
            },
            "amount": 5000,
            "currency": "sats",
            "ledger_type": LedgerType.EXCHANGE_TO_NODE.value,
            "description": "Test create",
            "cust_id": "voltage",
        }
        payload.update(overrides)
        return payload

    @pytest.mark.asyncio
    async def test_create_success(self, monkeypatch):
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        # Mock CryptoConversion
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        resp = await create_entry(payload=self._valid_payload())
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["status"] == "ok"
        assert "group_id" in body

    @pytest.mark.asyncio
    async def test_create_auto_group_id(self, monkeypatch):
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        resp = await create_entry(payload=self._valid_payload())
        body = json.loads(resp.body)
        # Auto-generated group_id starts with "manual_"
        assert body["group_id"].startswith("manual_")
        assert LedgerType.EXCHANGE_TO_NODE.value in body["group_id"]

    @pytest.mark.asyncio
    async def test_create_custom_group_id_appends_manual(self, monkeypatch):
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        resp = await create_entry(payload=self._valid_payload(group_id="my_withdrawal"))
        body = json.loads(resp.body)
        assert body["group_id"] == f"my_withdrawal_manual_{LedgerType.EXCHANGE_TO_NODE.value}"

    @pytest.mark.asyncio
    async def test_create_different_ledger_types_get_unique_group_ids(self, monkeypatch):
        """When same base group_id is used with different ledger types, each gets a unique group_id."""
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        resp1 = await create_entry(
            payload=self._valid_payload(
                group_id="withdrawal_001",
                ledger_type=LedgerType.EXCHANGE_TO_NODE.value,
            )
        )
        resp2 = await create_entry(
            payload=self._valid_payload(
                group_id="withdrawal_001",
                ledger_type=LedgerType.EXCHANGE_FEES.value,
                debit={
                    "account_type": "Expense",
                    "name": "Withdrawal Fees Paid",
                    "sub": "binance_convert",
                },
            )
        )
        body1 = json.loads(resp1.body)
        body2 = json.loads(resp2.body)
        assert body1["group_id"] != body2["group_id"]
        assert "exc_to_n" in body1["group_id"]
        assert "exc_fee" in body2["group_id"]

    @pytest.mark.asyncio
    async def test_create_invalid_currency(self, monkeypatch):
        resp = await create_entry(payload=self._valid_payload(currency="INVALID_COIN"))
        assert resp.status_code == 400
        assert "Unknown currency" in json.loads(resp.body)["error"]

    @pytest.mark.asyncio
    async def test_create_invalid_debit_account_type(self):
        payload = self._valid_payload()
        payload["debit"]["account_type"] = "NotReal"
        resp = await create_entry(payload=payload)
        assert resp.status_code == 500  # caught by outer try/except

    @pytest.mark.asyncio
    async def test_create_with_timestamp(self, monkeypatch):
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        resp = await create_entry(payload=self._valid_payload(timestamp="2025-06-15T12:00:00"))
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_create_with_invalid_timestamp_falls_back(self, monkeypatch):
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        resp = await create_entry(payload=self._valid_payload(timestamp="not-a-date"))
        # Should still succeed with fallback to now()
        assert resp.status_code == 200


# ===================================================================
# /api/presets
# ===================================================================


class TestPresets:
    @pytest.mark.asyncio
    async def test_get_presets_returns_list(self, monkeypatch):
        # Patch config calls to avoid real config
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_exchange_sub",
            lambda: "binance_test",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_node_name",
            lambda: "test_node",
        )

        resp = await get_presets()
        body = json.loads(resp.body)
        assert isinstance(body, list)
        assert len(body) == 2
        assert body[0]["id"] == "exchange_to_lightning"
        assert body[1]["id"] == "exchange_fee"

    @pytest.mark.asyncio
    async def test_presets_contain_cust_id(self, monkeypatch):
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_exchange_sub",
            lambda: "binance_test",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_node_name",
            lambda: "test_node",
        )

        resp = await get_presets()
        body = json.loads(resp.body)

        # exchange_to_lightning entry 1: cust_id = node_name
        assert body[0]["entries"][0]["cust_id"] == "test_node"
        # exchange_to_lightning entry 2: cust_id = exchange_sub
        assert body[0]["entries"][1]["cust_id"] == "binance_test"
        # exchange_fee entry: cust_id = exchange_sub
        assert body[1]["entries"][0]["cust_id"] == "binance_test"

    @pytest.mark.asyncio
    async def test_presets_use_config_names(self, monkeypatch):
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_exchange_sub",
            lambda: "my_exchange",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_node_name",
            lambda: "my_node",
        )

        resp = await get_presets()
        body = json.loads(resp.body)
        assert "my_exchange" in body[0]["label"]
        assert "my_node" in body[0]["label"]
        assert body[0]["entries"][0]["debit_sub"] == "my_node"
        assert body[0]["entries"][0]["credit_sub"] == "my_exchange"


# ===================================================================
# Helper function unit tests
# ===================================================================


class TestHelperFunctions:
    def test_account_class_for_type_valid(self):
        cls = _account_class_for_type("Asset")
        assert cls is AssetAccount

    def test_account_class_for_type_expense(self):
        cls = _account_class_for_type("Expense")
        assert cls is ExpenseAccount

    def test_account_class_for_type_invalid(self):
        with pytest.raises(ValueError, match="Unknown account type"):
            _account_class_for_type("FakeType")

    def test_build_account(self):
        acc = _build_account("Asset", "Exchange Holdings", "binance")
        assert isinstance(acc, AssetAccount)
        assert acc.name == "Exchange Holdings"
        assert acc.sub == "binance"

    def test_build_editor_presets_structure(self, monkeypatch):
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_exchange_sub",
            lambda: "ex",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor._get_node_name",
            lambda: "nd",
        )
        presets = _build_editor_presets()
        assert len(presets) == 2
        # First preset has 2 entries
        assert len(presets[0]["entries"]) == 2
        # Second preset has 1 entry
        assert len(presets[1]["entries"]) == 1
        # All entries have required keys
        for preset in presets:
            for entry in preset["entries"]:
                assert "ledger_type" in entry
                assert "debit_account_type" in entry
                assert "credit_account_type" in entry
                assert "cust_id" in entry
