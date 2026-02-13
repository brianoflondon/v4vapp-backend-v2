"""Tests for opening balance functions (Lightning and Exchange/Binance)."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from tests.get_last_quote import last_quote
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.opening_balances import (
    reset_exchange_opening_balance,
    reset_lightning_opening_balance,
)
from v4vapp_backend_v2.models.lnd_balance_models import ChannelBalance, LNDAmount, NodeBalances


@pytest.fixture(scope="module")
def module_monkeypatch():
    """MonkeyPatch fixture with module scope."""
    from _pytest.monkeypatch import MonkeyPatch

    monkey_patch = MonkeyPatch()
    yield monkey_patch
    monkey_patch.undo()


@pytest.fixture(autouse=True, scope="module")
async def setup_db(module_monkeypatch):
    """Set up test config and database, clean up ledger collection after tests."""
    test_config_path = Path("tests/data/config")
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    module_monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    i_c = InternalConfig()
    db_conn = DBConn()
    await db_conn.setup_database()
    # Start with a clean ledger
    await i_c.db["ledger"].drop()
    yield
    await i_c.db["ledger"].drop()
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


# ---------------------------------------------------------------------------
# Helper to set up TrackedBaseModel.last_quote without hitting external APIs
# ---------------------------------------------------------------------------
def _setup_quote():
    TrackedBaseModel.last_quote = last_quote()


def _make_mock_adapter(
    btc: Decimal = Decimal(0),
    hive: Decimal = Decimal(0),
    name: str = "binance_testnet",
) -> MagicMock:
    """Create a mock exchange adapter with the given balances and name."""
    adapter = MagicMock()
    type(adapter).exchange_name = PropertyMock(return_value=name)
    adapter.get_balance = lambda asset: {"BTC": btc, "HIVE": hive}.get(asset, Decimal(0))
    return adapter


# ---------------------------------------------------------------------------
# Tests for reset_lightning_opening_balance
# ---------------------------------------------------------------------------


class TestResetLightningOpeningBalance:
    """Tests for the Lightning opening-balance function."""

    @pytest.mark.asyncio
    async def test_no_channel_balance_logs_warning(self):
        """When fetch_balances returns no channel, function should return early."""
        _setup_quote()
        no_channel = NodeBalances(node="example", channel=None)

        with patch(
            "v4vapp_backend_v2.helpers.opening_balances.fetch_balances",
            new_callable=AsyncMock,
            return_value=no_channel,
        ):
            await reset_lightning_opening_balance()

        # No ledger entries should have been created
        count = await LedgerEntry.collection().count_documents({})
        assert count == 0

    @pytest.mark.asyncio
    async def test_initial_opening_balance_created(self):
        """First run should create an opening-balance ledger entry."""
        _setup_quote()
        # Clean slate
        await InternalConfig.db["ledger"].drop()

        local_msat = Decimal(5_000_000_000)  # 5,000,000 sats = 5M sats
        channel = ChannelBalance(
            local_balance=LNDAmount(sat=local_msat / 1000, msat=local_msat),
            remote_balance=LNDAmount(sat=Decimal(1_000_000), msat=Decimal(1_000_000_000)),
        )
        mock_balances = NodeBalances(node="example", channel=channel)

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.fetch_balances",
                new_callable=AsyncMock,
                return_value=mock_balances,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
        ):
            await reset_lightning_opening_balance()

        # Verify ledger entry was created
        entries = await LedgerEntry.collection().find({}).to_list()
        assert len(entries) == 1

        entry = LedgerEntry.model_validate(entries[0])
        assert entry.debit.name == "External Lightning Payments"
        assert entry.credit.name == "Owner Loan Payable"
        assert entry.debit_amount == local_msat
        assert entry.short_id == "open"
        assert entry.op_type == "funding"

    @pytest.mark.asyncio
    async def test_matching_balance_no_action(self):
        """When ledger balance matches channel balance, no new entry should be created."""
        _setup_quote()
        # Don't drop — reuse the entry from the previous test
        # The ledger has 5_000_000_000 msats from the previous test
        local_msat = Decimal(5_000_000_000)
        channel = ChannelBalance(
            local_balance=LNDAmount(sat=local_msat / 1000, msat=local_msat),
            remote_balance=LNDAmount(sat=Decimal(1_000_000), msat=Decimal(1_000_000_000)),
        )
        mock_balances = NodeBalances(node="example", channel=channel)

        count_before = await LedgerEntry.collection().count_documents({})

        with patch(
            "v4vapp_backend_v2.helpers.opening_balances.fetch_balances",
            new_callable=AsyncMock,
            return_value=mock_balances,
        ):
            await reset_lightning_opening_balance()

        count_after = await LedgerEntry.collection().count_documents({})
        assert count_after == count_before  # No new entries

    @pytest.mark.asyncio
    async def test_adjustment_entry_on_mismatch(self):
        """When existing transactions exist but balance differs, create adjustment."""
        _setup_quote()
        await InternalConfig.db["ledger"].drop()

        # Create an initial opening balance first
        initial_msat = Decimal(5_000_000_000)
        channel_initial = ChannelBalance(
            local_balance=LNDAmount(sat=initial_msat / 1000, msat=initial_msat),
            remote_balance=LNDAmount(sat=Decimal(1_000_000), msat=Decimal(1_000_000_000)),
        )
        mock_initial = NodeBalances(node="example", channel=channel_initial)

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.fetch_balances",
                new_callable=AsyncMock,
                return_value=mock_initial,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
        ):
            await reset_lightning_opening_balance()

        # Now add a second dummy entry so has_transactions returns True
        # (has_transactions requires combined_balance length > 1)
        quote = TrackedBaseModel.last_quote
        dummy_conv = CryptoConversion(
            conv_from=Currency.MSATS, value=Decimal(1000), quote=quote
        ).conversion
        dummy_entry = LedgerEntry(
            cust_id="",
            short_id="dummy",
            op_type="funding",
            ledger_type=LedgerType.FUNDING,
            group_id=f"dummy-{datetime.now(tz=timezone.utc).isoformat()}",
            timestamp=datetime.now(tz=timezone.utc),
            description="Dummy entry to trigger has_transactions",
            debit=AssetAccount(name="External Lightning Payments", sub="example"),
            debit_unit=Currency.MSATS,
            debit_amount=Decimal(1000),
            debit_conv=dummy_conv,
            credit=LiabilityAccount(name="Owner Loan Payable", sub="example"),
            credit_unit=Currency.MSATS,
            credit_amount=Decimal(1000),
            credit_conv=dummy_conv,
        )
        await dummy_entry.save()

        # Now channel balance changed
        new_local_msat = Decimal(6_000_000_000)  # 6M sats
        channel = ChannelBalance(
            local_balance=LNDAmount(sat=new_local_msat / 1000, msat=new_local_msat),
            remote_balance=LNDAmount(sat=Decimal(1_000_000), msat=Decimal(1_000_000_000)),
        )
        mock_balances = NodeBalances(node="example", channel=channel)

        count_before = await LedgerEntry.collection().count_documents({})

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.fetch_balances",
                new_callable=AsyncMock,
                return_value=mock_balances,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
        ):
            await reset_lightning_opening_balance()

        count_after = await LedgerEntry.collection().count_documents({})
        assert count_after == count_before + 1  # One adjustment entry added

        # Check the adjustment entry
        adjustment = await LedgerEntry.collection().find_one({"short_id": "adjustment"})
        assert adjustment is not None
        assert "Balance adjustment" in adjustment["description"]

        # Verify the adjustment amount is the difference
        existing_ledger_msats = initial_msat + Decimal(1000)  # opening + dummy
        expected_adjustment = new_local_msat - existing_ledger_msats
        entry = LedgerEntry.model_validate(adjustment)
        assert entry.debit_amount == expected_adjustment


# ---------------------------------------------------------------------------
# Tests for reset_exchange_opening_balance
# ---------------------------------------------------------------------------


class TestResetExchangeOpeningBalance:
    """Tests for the Binance/Exchange opening-balance function."""

    @pytest.mark.asyncio
    async def test_zero_balances_logs_warning(self):
        """When both SATS and HIVE are 0, function should return early."""
        _setup_quote()
        await InternalConfig.db["ledger"].drop()

        mock_adapter = _make_mock_adapter(
            btc=Decimal(0), hive=Decimal(0), name="binance_testnet"
        )

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        count = await LedgerEntry.collection().count_documents({})
        assert count == 0

    @pytest.mark.asyncio
    async def test_initial_sats_only_opening_balance(self):
        """Should create a single MSATS opening entry when only SATS are present."""
        _setup_quote()
        await InternalConfig.db["ledger"].drop()

        sats_value = Decimal(100_462_981)
        mock_adapter = _make_mock_adapter(
            btc=Decimal("1.00462981"), hive=Decimal(0), name="binance_testnet"
        )

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        entries = await LedgerEntry.collection().find({}).to_list()
        assert len(entries) == 1

        entry = LedgerEntry.model_validate(entries[0])
        assert entry.debit.name == "Exchange Holdings"
        assert entry.debit.sub == "binance_testnet"
        assert entry.credit.name == "Owner Loan Payable"
        assert entry.credit.sub == "binance_testnet"
        # SATS → MSATS: 100_462_981 sats * 1000 = 100_462_981_000 msats
        expected_msats = Decimal(int(sats_value) * 1000)
        assert entry.debit_amount == expected_msats
        assert entry.short_id == "open"
        assert entry.op_type == "funding"

    @pytest.mark.asyncio
    async def test_initial_hive_only_opening_balance(self):
        """Should create a single HIVE opening entry when only HIVE is present."""
        _setup_quote()
        await InternalConfig.db["ledger"].drop()

        hive_value = Decimal("1875.000")
        mock_adapter = _make_mock_adapter(
            btc=Decimal(0), hive=hive_value, name="binance_testnet"
        )

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        entries = await LedgerEntry.collection().find({}).to_list()
        assert len(entries) == 1

        entry = LedgerEntry.model_validate(entries[0])
        assert entry.debit.name == "Exchange Holdings"
        assert entry.debit.sub == "binance_testnet"
        assert entry.debit_amount == hive_value
        assert entry.debit_unit == Currency.HIVE
        assert entry.short_id == "open"

    @pytest.mark.asyncio
    async def test_both_sats_and_hive_opening_balance(self):
        """Should create TWO entries when both SATS and HIVE have non-zero balances."""
        _setup_quote()
        await InternalConfig.db["ledger"].drop()

        sats_value = Decimal(100_462_981)
        hive_value = Decimal("1875.000")
        mock_adapter = _make_mock_adapter(
            btc=Decimal("1.00462981"), hive=hive_value, name="binance_testnet"
        )

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        entries = await LedgerEntry.collection().find({}).to_list()
        assert len(entries) == 2

        # Verify both entries target Exchange Holdings
        for raw_entry in entries:
            entry = LedgerEntry.model_validate(raw_entry)
            assert entry.debit.name == "Exchange Holdings"
            assert entry.debit.sub == "binance_testnet"
            assert entry.credit.name == "Owner Loan Payable"
            assert entry.credit.sub == "binance_testnet"
            assert entry.short_id == "open"

    @pytest.mark.asyncio
    async def test_matching_balance_no_action(self):
        """When ledger already matches exchange balance, no new entry should be created."""
        _setup_quote()
        # Don't drop — reuse the entries from the previous test
        count_before = await LedgerEntry.collection().count_documents({})

        sats_value = Decimal(100_462_981)
        hive_value = Decimal("1875.000")
        mock_adapter = _make_mock_adapter(
            btc=Decimal("1.00462981"), hive=hive_value, name="binance_testnet"
        )

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        count_after = await LedgerEntry.collection().count_documents({})
        assert count_after == count_before  # No new entries

    @pytest.mark.asyncio
    async def test_adjustment_on_sats_mismatch(self):
        """When SATS balance changes, should create an adjustment entry."""
        _setup_quote()
        # Current ledger has previous sats balance — now it changed
        new_sats = Decimal(110_000_000)  # Changed from 100_462_981
        hive_value = Decimal("1875.000")  # Same HIVE
        mock_adapter = _make_mock_adapter(
            btc=Decimal("1.10000000"), hive=hive_value, name="binance_testnet"
        )

        count_before = await LedgerEntry.collection().count_documents({})

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        count_after = await LedgerEntry.collection().count_documents({})
        # Should add 1 adjustment for sats (hive is still matching)
        assert count_after == count_before + 1

        # Find the adjustment entry
        adjustment = await LedgerEntry.collection().find_one({"short_id": "adjustment"})
        assert adjustment is not None
        assert "adjustment" in adjustment["description"].lower()

    @pytest.mark.asyncio
    async def test_custom_exchange_sub(self):
        """Should use the adapter's exchange_name as the sub-account."""
        _setup_quote()
        await InternalConfig.db["ledger"].drop()

        mock_adapter = _make_mock_adapter(
            btc=Decimal("0.5"), hive=Decimal(0), name="binance_mainnet"
        )

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        entries = await LedgerEntry.collection().find({}).to_list()
        assert len(entries) == 1

        entry = LedgerEntry.model_validate(entries[0])
        assert entry.debit.sub == "binance_mainnet"
        assert entry.credit.sub == "binance_mainnet"

    @pytest.mark.asyncio
    async def test_ledger_entry_fields_correct(self):
        """Verify all essential fields are populated correctly on the ledger entry."""
        _setup_quote()
        await InternalConfig.db["ledger"].drop()

        sats_value = Decimal(50_000_000)
        mock_adapter = _make_mock_adapter(
            btc=Decimal("0.5"), hive=Decimal(0), name="binance_testnet"
        )

        with (
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.TrackedBaseModel.update_quote",
                new_callable=AsyncMock,
            ),
            patch(
                "v4vapp_backend_v2.helpers.opening_balances.get_exchange_adapter",
                return_value=mock_adapter,
            ),
        ):
            await reset_exchange_opening_balance()

        entries = await LedgerEntry.collection().find({}).to_list()
        assert len(entries) == 1

        entry = LedgerEntry.model_validate(entries[0])
        assert entry.ledger_type.value == "funding"
        assert entry.op_type == "funding"
        assert "open" in entry.group_id
        assert entry.description == "Initial opening balance for binance_testnet (sats)"
        # Verify conversion was computed (non-zero values)
        assert entry.debit_conv.msats > 0
        assert entry.debit_conv.hive > 0
