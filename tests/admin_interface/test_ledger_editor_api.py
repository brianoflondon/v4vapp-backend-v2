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
    _validate_and_build_entry,
    compute_conversion,
    create_batch,
    create_entry,
    get_presets,
    load_entry,
    update_entry,
)
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.currency_class import Currency

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dummy_entry(**overrides):
    """Build a minimal LedgerEntry for mocking."""
    defaults = dict(
        group_id="test_g1",
        short_id="test_g1",
        timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
        ledger_type=LedgerType.EXCHANGE_TO_NODE,
        description="Test entry",
        user_memo="memo",
        cust_id="voltage",
        debit=AssetAccount(name="External Lightning Payments", sub="voltage"),
        credit=AssetAccount(name="Exchange Holdings", sub="binance_convert"),
        debit_amount=Decimal("1000"),
        debit_unit=Currency.MSATS,
        debit_conv=CryptoConv(),
        credit_amount=Decimal("1000"),
        credit_unit=Currency.MSATS,
        credit_conv=CryptoConv(),
        reversed=None,
        extra_data=[{"source": "test"}],
        link="",
    )
    defaults.update(overrides)
    return LedgerEntry(**defaults)


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
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        resp = await update_entry(payload={"group_id": "test_g1", "description": "Updated desc"})
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["status"] == "ok"
        assert body["modified"] == 1
        mock_save.assert_awaited_once_with(upsert=True)

    @pytest.mark.asyncio
    async def test_update_reversed_now(self, monkeypatch):
        entry = _make_dummy_entry()
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        resp = await update_entry(payload={"group_id": "test_g1", "reversed": "now"})
        assert resp.status_code == 200
        mock_save.assert_awaited_once_with(upsert=True)

    @pytest.mark.asyncio
    async def test_update_reversed_clear(self, monkeypatch):
        entry = _make_dummy_entry(reversed=datetime.now(tz=timezone.utc))
        monkeypatch.setattr(LedgerEntry, "load", AsyncMock(return_value=entry))
        mock_save = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", mock_save)

        resp = await update_entry(payload={"group_id": "test_g1", "reversed": None})
        assert resp.status_code == 200
        mock_save.assert_awaited_once_with(upsert=True)

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
        # Auto-generated group_id contains _manual_ and ledger_type suffix
        assert "_manual_" in body["group_id"]
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
        assert resp.status_code == 400

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

    @pytest.mark.asyncio
    async def test_create_sats_normalised_to_msats(self, monkeypatch):
        """When currency=sats, amount should be ×1000 and unit stored as msats."""
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        entry, error = await _validate_and_build_entry(
            self._valid_payload(amount=5000, currency="sats")
        )
        assert error is None
        assert entry is not None
        assert entry.debit_unit.value == "msats"
        assert entry.credit_unit.value == "msats"
        assert entry.debit_amount == 5_000_000  # 5000 sats × 1000
        assert entry.credit_amount == 5_000_000

    @pytest.mark.asyncio
    async def test_create_hive_not_normalised(self, monkeypatch):
        """hive should be stored as-is, not converted."""
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        entry, error = await _validate_and_build_entry(
            self._valid_payload(amount=100, currency="hive")
        )
        assert error is None
        assert entry is not None
        assert entry.debit_unit.value == "hive"
        assert entry.debit_amount == 100

    @pytest.mark.asyncio
    async def test_create_disallowed_currency_usd(self):
        """USD should be rejected — only sats, hive, hbd allowed."""
        entry, error = await _validate_and_build_entry(self._valid_payload(currency="usd"))
        assert entry is None
        assert "cannot be stored directly" in error

    @pytest.mark.asyncio
    async def test_create_disallowed_currency_btc(self):
        """BTC should be rejected."""
        entry, error = await _validate_and_build_entry(self._valid_payload(currency="btc"))
        assert entry is None
        assert "cannot be stored directly" in error

    @pytest.mark.asyncio
    async def test_create_msats_stored_directly(self, monkeypatch):
        """msats should be stored as-is without conversion."""
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        entry, error = await _validate_and_build_entry(
            self._valid_payload(amount=5000000, currency="msats")
        )
        assert error is None
        assert entry is not None
        assert entry.debit_unit.value == "msats"
        assert entry.debit_amount == 5_000_000  # unchanged

    @pytest.mark.asyncio
    async def test_short_id_max_10_chars(self, monkeypatch):
        """short_id should be at most 10 characters."""
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        entry, error = await _validate_and_build_entry(
            self._valid_payload(group_id="very_long_group_id_12345")
        )
        assert error is None
        assert entry is not None
        assert len(entry.short_id) <= 10

    @pytest.mark.asyncio
    async def test_auto_group_id_uses_uuid(self, monkeypatch):
        """Auto-generated group_id should contain uuid hex prefix."""
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

        entry, error = await _validate_and_build_entry(self._valid_payload())
        assert error is None
        assert entry is not None
        # Format: {uuid_hex[:10]}_manual_{ledger_type}
        parts = entry.group_id.split("_manual_")
        assert len(parts) == 2
        assert len(parts[0]) == 10  # uuid hex prefix


# ===================================================================
# /api/presets
# ===================================================================


class TestPresets:
    @pytest.mark.asyncio
    async def test_get_presets_returns_list(self, monkeypatch):
        # Patch config calls to avoid real config
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_exchange_sub",
            lambda: "binance_test",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_node_name",
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
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_exchange_sub",
            lambda: "binance_test",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_node_name",
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
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_exchange_sub",
            lambda: "my_exchange",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_node_name",
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
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_exchange_sub",
            lambda: "ex",
        )
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_edit_presets._get_node_name",
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


# ===================================================================
# /api/create-batch
# ===================================================================


class TestCreateBatch:
    def _valid_entry(self, **overrides):
        entry = {
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
            "description": "Test batch entry",
            "cust_id": "voltage",
        }
        entry.update(overrides)
        return entry

    def _mock_crypto(self, monkeypatch):
        dummy_conv = CryptoConv()
        mock_conversion = MagicMock()
        mock_conversion.get_quote = AsyncMock()
        mock_conversion.conversion = dummy_conv
        monkeypatch.setattr(
            "v4vapp_backend_v2.admin.routers.ledger_editor.CryptoConversion",
            lambda **kwargs: mock_conversion,
        )

    @pytest.mark.asyncio
    async def test_batch_empty_list(self):
        resp = await create_batch(entries=[])
        assert resp.status_code == 400
        assert "No entries" in json.loads(resp.body)["error"]

    @pytest.mark.asyncio
    async def test_batch_all_valid(self, monkeypatch):
        self._mock_crypto(monkeypatch)
        monkeypatch.setattr(LedgerEntry, "save", AsyncMock(return_value=MagicMock()))

        entries = [
            self._valid_entry(ledger_type=LedgerType.EXCHANGE_TO_NODE.value),
            self._valid_entry(
                ledger_type=LedgerType.EXCHANGE_FEES.value,
                debit={
                    "account_type": "Expense",
                    "name": "Withdrawal Fees Paid",
                    "sub": "binance_convert",
                },
                amount=100,
            ),
        ]
        resp = await create_batch(entries=entries)
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["status"] == "ok"
        assert len(body["group_ids"]) == 2

    @pytest.mark.asyncio
    async def test_batch_one_invalid_blocks_all(self, monkeypatch):
        """If one entry has zero amount, NO entries should be saved."""
        self._mock_crypto(monkeypatch)
        save_mock = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", save_mock)

        entries = [
            self._valid_entry(amount=5000),  # valid
            self._valid_entry(amount=0),  # invalid — zero amount
        ]
        resp = await create_batch(entries=entries)
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "Validation failed" in body["error"]
        assert len(body["details"]) == 1
        assert "Entry #2" in body["details"][0]
        # Crucially: save was never called
        save_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_multiple_invalid(self, monkeypatch):
        """Multiple invalid entries should all be reported."""
        self._mock_crypto(monkeypatch)
        save_mock = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", save_mock)

        entries = [
            self._valid_entry(amount=0),  # invalid
            self._valid_entry(currency="FAKE_COIN"),  # invalid
        ]
        resp = await create_batch(entries=entries)
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert len(body["details"]) == 2
        save_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_missing_account_name_blocks_all(self, monkeypatch):
        self._mock_crypto(monkeypatch)
        save_mock = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", save_mock)

        entries = [
            self._valid_entry(),
            self._valid_entry(debit={"account_type": "Asset", "name": "", "sub": ""}),
        ]
        resp = await create_batch(entries=entries)
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "Validation failed" in body["error"]
        save_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_invalid_ledger_type_blocks_all(self, monkeypatch):
        self._mock_crypto(monkeypatch)
        save_mock = AsyncMock(return_value=MagicMock())
        monkeypatch.setattr(LedgerEntry, "save", save_mock)

        entries = [
            self._valid_entry(),
            self._valid_entry(ledger_type="TOTALLY_INVALID"),
        ]
        resp = await create_batch(entries=entries)
        assert resp.status_code == 400
        save_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_shared_group_id_gets_unique_suffixes(self, monkeypatch):
        self._mock_crypto(monkeypatch)
        monkeypatch.setattr(LedgerEntry, "save", AsyncMock(return_value=MagicMock()))

        entries = [
            self._valid_entry(group_id="wd_001", ledger_type=LedgerType.EXCHANGE_TO_NODE.value),
            self._valid_entry(
                group_id="wd_001",
                ledger_type=LedgerType.EXCHANGE_FEES.value,
                debit={
                    "account_type": "Expense",
                    "name": "Withdrawal Fees Paid",
                    "sub": "binance_convert",
                },
                amount=100,
            ),
        ]
        resp = await create_batch(entries=entries)
        assert resp.status_code == 200
        body = json.loads(resp.body)
        ids = body["group_ids"]
        assert ids[0] != ids[1]
        assert "exc_to_n" in ids[0]
        assert "exc_fee" in ids[1]

    @pytest.mark.asyncio
    async def test_batch_auto_group_id_shared_prefix(self, monkeypatch):
        """When no group_id is supplied, all entries in a batch share the same uuid prefix."""
        self._mock_crypto(monkeypatch)
        monkeypatch.setattr(LedgerEntry, "save", AsyncMock(return_value=MagicMock()))

        entries = [
            self._valid_entry(ledger_type=LedgerType.EXCHANGE_TO_NODE.value),
            self._valid_entry(
                ledger_type=LedgerType.EXCHANGE_FEES.value,
                debit={
                    "account_type": "Expense",
                    "name": "Withdrawal Fees Paid",
                    "sub": "binance_convert",
                },
                amount=100,
            ),
        ]
        # Remove any group_id from payloads so auto-generation kicks in
        for e in entries:
            e.pop("group_id", None)

        resp = await create_batch(entries=entries)
        assert resp.status_code == 200
        body = json.loads(resp.body)
        ids = body["group_ids"]
        assert len(ids) == 2
        # Both should share the same uuid prefix (before _manual_)
        prefix0 = ids[0].split("_manual_")[0]
        prefix1 = ids[1].split("_manual_")[0]
        assert prefix0 == prefix1
        # But suffixes differ (different ledger types)
        assert ids[0] != ids[1]
