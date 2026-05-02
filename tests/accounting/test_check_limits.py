"""
Tests for check_hive_conversion_limits and get_next_limit_expiry.

Uses AsyncMock/MagicMock to patch LedgerEntry.collection() so no real MongoDB
connection is required. V4VConfig is also mocked to avoid Hive network calls.

Test data is based on real output from check_limits.py run against production
data on 2026-04-27, covering three distinct scenarios:

  - jza: only the 4h period is exceeded (114%); expiry comes from the 4h window.
  - azurecherenkov: only the 72h period is exceeded (115%); expiry from 72h window.
  - v4vapp-test: all periods within limits; limit_ok=True, get_next_limit_expiry
    returns None.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    get_next_limit_expiry,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.hive.v4v_config import V4VConfigRateLimits

# ---------------------------------------------------------------------------
# Rate limits matching the production config used in sample output
# ---------------------------------------------------------------------------
RATE_LIMITS = [
    V4VConfigRateLimits(hours=4, sats=Decimal(400_000)),
    V4VConfigRateLimits(hours=72, sats=Decimal(800_000)),
    V4VConfigRateLimits(hours=168, sats=Decimal(1_200_000)),
]


# ---------------------------------------------------------------------------
# Module-level config path fixtures (no real DB connection needed)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def module_monkeypatch():
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()


@pytest.fixture(autouse=True, scope="module")
def set_base_config_path(module_monkeypatch):
    test_config_path = Path("tests/data/config")
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    module_monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


# ---------------------------------------------------------------------------
# Helpers to build pipeline result dicts matching the real MongoDB output
# ---------------------------------------------------------------------------


def _period(
    sats,
    msats,
    hive,
    hbd,
    usd,
    limit_hours,
    limit_sats,
    details=None,
):
    """Build a single period dict that matches what limit_check_pipeline returns."""
    data = {
        "msats": Decimal(str(msats)),
        "sats": Decimal(str(sats)),
        "usd": Decimal(str(usd)),
        "hive": Decimal(str(hive)),
        "hbd": Decimal(str(hbd)),
        "limit_hours": str(limit_hours),
        "limit_sats": str(limit_sats),
        "limit_ok": True,  # computed by MongoDB $lt; model property is the source of truth
    }
    if details is not None:
        data["details"] = details
    return data


def _pipeline_result(cust_id, periods):
    """Wrap periods into the top-level list that aggregate().to_list() returns."""
    return [{"cust_id": cust_id, "periods": periods}]


def _detail_entry(timestamp: datetime, msats: int, cust_id: str = "test") -> dict:
    """
    Build a minimal raw ledger-entry dict suitable for use as a details entry.
    Only the fields read by get_next_limit_expiry are required:
        - timestamp
        - credit_conv.msats
    Other fields default to zero/empty so LedgerEntry.model_validate succeeds
    (credit_debit_equality passes because both amounts are 0).
    """
    return {
        "timestamp": timestamp,
        "credit_conv": {"msats": Decimal(str(msats))},
        "cust_id": cust_id,
        "ledger_type": "h_conv_k",
    }


# ---------------------------------------------------------------------------
# Mock collection factory
# ---------------------------------------------------------------------------


def _make_cursor(results):
    cursor = MagicMock()
    cursor.to_list = AsyncMock(return_value=results)
    return cursor


def _make_collection(*result_batches):
    """
    Return a mock async collection whose successive aggregate() calls return
    the provided result_batches in order.  The last batch is repeated if more
    calls are made than batches provided.
    """
    col = MagicMock()
    cursors = [_make_cursor(r) for r in result_batches]

    call_count = {"n": 0}

    async def _aggregate(**_kwargs):
        i = min(call_count["n"], len(cursors) - 1)
        call_count["n"] += 1
        return cursors[i]

    col.aggregate = _aggregate
    return col


# ---------------------------------------------------------------------------
# V4VConfig mock fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_v4v_config():
    """
    Patch V4VConfig in both account_balances and simple_pipelines modules so
    that no Hive blockchain connection is attempted, and the default production
    rate limits are returned.
    """
    mock_data = MagicMock()
    mock_data.lightning_rate_limits = RATE_LIMITS
    mock_data.check_and_sort_rate_limits.return_value = (True, 168)
    mock_data.max_rate_limit_hours = 168

    mock_instance = MagicMock()
    mock_instance.data = mock_data

    with (
        patch(
            "v4vapp_backend_v2.accounting.account_balances.V4VConfig",
            return_value=mock_instance,
        ),
        patch(
            "v4vapp_backend_v2.accounting.pipelines.simple_pipelines.V4VConfig",
            return_value=mock_instance,
        ),
    ):
        yield RATE_LIMITS


# ===========================================================================
# Scenario data
# ===========================================================================

NOW = datetime.now(tz=timezone.utc)

# --- jza: 4h exceeded, 72h/168h within limits ---
# All three periods saw 459,914 sats in the past 4 hours (same transactions).
JZA_SATS = Decimal("459914")
JZA_MSATS = Decimal("459914026")
JZA_HIVE = Decimal("5852.38")
JZA_HBD = Decimal("366.34")
JZA_USD = Decimal("364.31")

JZA_PERIODS_NO_DETAILS = {
    "4": _period(JZA_SATS, JZA_MSATS, JZA_HIVE, JZA_HBD, JZA_USD, 4, 400_000),
    "72": _period(JZA_SATS, JZA_MSATS, JZA_HIVE, JZA_HBD, JZA_USD, 72, 800_000),
    "168": _period(JZA_SATS, JZA_MSATS, JZA_HIVE, JZA_HBD, JZA_USD, 168, 1_200_000),
}

# Oldest tx in 4h window was placed 3h45m ago; it will expire in ~15 minutes.
JZA_OLDEST_TS = NOW - timedelta(hours=3, minutes=45)
JZA_DETAIL_MSATS = 152_976_000  # 152,976 sats freed when this tx leaves the window

JZA_PERIODS_WITH_DETAILS = {
    "4": _period(
        JZA_SATS,
        JZA_MSATS,
        JZA_HIVE,
        JZA_HBD,
        JZA_USD,
        4,
        400_000,
        details=[
            _detail_entry(JZA_OLDEST_TS, JZA_DETAIL_MSATS, "jza"),
            _detail_entry(NOW - timedelta(hours=2), 306_938_026, "jza"),
        ],
    ),
    "72": _period(
        JZA_SATS,
        JZA_MSATS,
        JZA_HIVE,
        JZA_HBD,
        JZA_USD,
        72,
        800_000,
        details=[
            _detail_entry(JZA_OLDEST_TS, JZA_DETAIL_MSATS, "jza"),
            _detail_entry(NOW - timedelta(hours=2), 306_938_026, "jza"),
        ],
    ),
    "168": _period(
        JZA_SATS,
        JZA_MSATS,
        JZA_HIVE,
        JZA_HBD,
        JZA_USD,
        168,
        1_200_000,
        details=[
            _detail_entry(JZA_OLDEST_TS, JZA_DETAIL_MSATS, "jza"),
            _detail_entry(NOW - timedelta(hours=2), 306_938_026, "jza"),
        ],
    ),
}

# --- azurecherenkov: 72h exceeded, 4h/168h within limits ---
AZURE_SATS = Decimal("925000")
AZURE_MSATS = Decimal("925000000")
AZURE_HIVE = Decimal("11500.65")
AZURE_HBD = Decimal("722.19")
AZURE_USD = Decimal("718.70")

AZURE_PERIODS_NO_DETAILS = {
    "4": _period(0, 0, 0, 0, 0, 4, 400_000),
    "72": _period(AZURE_SATS, AZURE_MSATS, AZURE_HIVE, AZURE_HBD, AZURE_USD, 72, 800_000),
    "168": _period(AZURE_SATS, AZURE_MSATS, AZURE_HIVE, AZURE_HBD, AZURE_USD, 168, 1_200_000),
}

# Oldest tx in 72h window was placed 25 hours ago; expires in 47 hours.
AZURE_OLDEST_TS = NOW - timedelta(hours=25)
AZURE_DETAIL_MSATS = 155_000_000  # 155,000 sats freed

AZURE_PERIODS_WITH_DETAILS = {
    "4": _period(0, 0, 0, 0, 0, 4, 400_000, details=[]),
    "72": _period(
        AZURE_SATS,
        AZURE_MSATS,
        AZURE_HIVE,
        AZURE_HBD,
        AZURE_USD,
        72,
        800_000,
        details=[
            _detail_entry(AZURE_OLDEST_TS, AZURE_DETAIL_MSATS, "azurecherenkov"),
            _detail_entry(NOW - timedelta(hours=10), 770_000_000, "azurecherenkov"),
        ],
    ),
    "168": _period(
        AZURE_SATS,
        AZURE_MSATS,
        AZURE_HIVE,
        AZURE_HBD,
        AZURE_USD,
        168,
        1_200_000,
        details=[
            _detail_entry(AZURE_OLDEST_TS, AZURE_DETAIL_MSATS, "azurecherenkov"),
            _detail_entry(NOW - timedelta(hours=10), 770_000_000, "azurecherenkov"),
        ],
    ),
}

# --- v4vapp-test: all periods within limits ---
TEST_PERIODS_NO_DETAILS = {
    "4": _period(0, 0, 0, 0, 0, 4, 400_000),
    "72": _period(0, 0, 0, 0, 0, 72, 800_000),
    "168": _period(0, 0, 0, 0, 0, 168, 1_200_000),
}

TEST_PERIODS_WITH_DETAILS = {
    "4": _period(0, 0, 0, 0, 0, 4, 400_000, details=[]),
    "72": _period(0, 0, 0, 0, 0, 72, 800_000, details=[]),
    "168": _period(0, 0, 0, 0, 0, 168, 1_200_000, details=[]),
}


# ===========================================================================
# Tests for check_hive_conversion_limits
# ===========================================================================


@pytest.mark.asyncio
async def test_check_limits_all_within_limits(mock_v4v_config):
    """v4vapp-test has zero conversions – all periods within limits."""
    mock_col = _make_collection(
        _pipeline_result("v4vapp-test", TEST_PERIODS_NO_DETAILS),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await check_hive_conversion_limits("v4vapp-test")

    assert result.limit_ok is True
    assert result.next_limit_expiry == ""
    assert result.expiry is None
    assert result.sats_freed is None

    assert result.periods["4"].limit_ok is True
    assert result.periods["72"].limit_ok is True
    assert result.periods["168"].limit_ok is True


@pytest.mark.asyncio
async def test_check_limits_jza_4h_exceeded(mock_v4v_config):
    """
    jza: 459,914 sats in 4h (limit 400,000) → limit exceeded.
    72h and 168h are within limits.
    The function should populate next_limit_expiry from the 4h window.
    """
    mock_col = _make_collection(
        # First call: check_hive_conversion_limits (no details)
        _pipeline_result("jza", JZA_PERIODS_NO_DETAILS),
        # Second call: get_next_limit_expiry (with details)
        _pipeline_result("jza", JZA_PERIODS_WITH_DETAILS),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await check_hive_conversion_limits("jza")

    assert result.limit_ok is False
    assert result.periods["4"].limit_ok is False
    assert result.periods["72"].limit_ok is True
    assert result.periods["168"].limit_ok is True

    assert result.periods["4"].limit_percent == 114
    assert result.periods["72"].limit_percent == 57
    assert result.periods["168"].limit_percent == 38

    # Expiry info should be populated from the 4h window
    assert result.expiry is not None
    expected_expiry = JZA_OLDEST_TS + timedelta(hours=4)
    assert abs((result.expiry - expected_expiry).total_seconds()) < 2

    assert result.sats_freed == Decimal(JZA_DETAIL_MSATS) // 1000  # 152,976

    assert "freeing" in result.next_limit_expiry
    assert "152,976 sats" in result.next_limit_expiry


@pytest.mark.asyncio
async def test_check_limits_azurecherenkov_72h_exceeded(mock_v4v_config):
    """
    azurecherenkov: 925,000 sats in 72h (limit 800,000) → limit exceeded.
    4h and 168h are within limits.
    The function should populate next_limit_expiry from the 72h window.
    """
    mock_col = _make_collection(
        # First call: check_hive_conversion_limits (no details)
        _pipeline_result("azurecherenkov", AZURE_PERIODS_NO_DETAILS),
        # Second call: get_next_limit_expiry (with details)
        _pipeline_result("azurecherenkov", AZURE_PERIODS_WITH_DETAILS),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await check_hive_conversion_limits("azurecherenkov")

    assert result.limit_ok is False
    assert result.periods["4"].limit_ok is True
    assert result.periods["72"].limit_ok is False
    assert result.periods["168"].limit_ok is True

    assert result.periods["4"].limit_percent == 0
    assert result.periods["72"].limit_percent == 115
    assert result.periods["168"].limit_percent == 77

    # Expiry info should come from the 72h window
    assert result.expiry is not None
    expected_expiry = AZURE_OLDEST_TS + timedelta(hours=72)
    assert abs((result.expiry - expected_expiry).total_seconds()) < 2

    assert result.sats_freed == Decimal(AZURE_DETAIL_MSATS) // 1000  # 155,000

    assert "freeing" in result.next_limit_expiry
    assert "155,000 sats" in result.next_limit_expiry


@pytest.mark.asyncio
async def test_check_limits_no_results_in_db(mock_v4v_config):
    """When the aggregate pipeline returns no documents, limit_ok defaults to True."""
    mock_col = _make_collection([])  # empty results
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await check_hive_conversion_limits("unknown-user")

    assert result.limit_ok is True
    assert result.periods == {}


# ===========================================================================
# Tests for get_next_limit_expiry
# ===========================================================================


@pytest.mark.asyncio
async def test_get_next_limit_expiry_no_rate_limits(mock_v4v_config):
    """When lightning_rate_limits is empty, returns None immediately."""
    # Override the fixture's rate limits to be empty
    with patch("v4vapp_backend_v2.accounting.account_balances.V4VConfig") as mock_cls:
        mock_cls.return_value.data.lightning_rate_limits = []
        result = await get_next_limit_expiry("jza")

    assert result is None


@pytest.mark.asyncio
async def test_get_next_limit_expiry_no_results_in_db(mock_v4v_config):
    """When the aggregate returns no documents, returns None."""
    mock_col = _make_collection([])
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await get_next_limit_expiry("unknown-user")

    assert result is None


@pytest.mark.asyncio
async def test_get_next_limit_expiry_all_within_limits(mock_v4v_config):
    """When all periods are within limits, returns None (no expiry needed)."""
    mock_col = _make_collection(
        _pipeline_result("v4vapp-test", TEST_PERIODS_WITH_DETAILS),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        result = await get_next_limit_expiry("v4vapp-test")

    assert result is None


@pytest.mark.asyncio
async def test_get_next_limit_expiry_jza_4h_only_exceeded(mock_v4v_config):
    """
    jza: only 4h exceeded.  The expiry should be oldest_4h_tx + 4 hours,
    and sats_freed should be the msats of that oldest transaction ÷ 1000.
    """
    mock_col = _make_collection(
        _pipeline_result("jza", JZA_PERIODS_WITH_DETAILS),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        expiry_info = await get_next_limit_expiry("jza")

    assert expiry_info is not None
    expiry, sats_freed = expiry_info

    expected_expiry = JZA_OLDEST_TS + timedelta(hours=4)
    assert abs((expiry - expected_expiry).total_seconds()) < 2

    expected_sats = Decimal(JZA_DETAIL_MSATS) // 1000  # 152,976
    assert sats_freed == expected_sats


@pytest.mark.asyncio
async def test_get_next_limit_expiry_azurecherenkov_72h_only_exceeded(mock_v4v_config):
    """
    azurecherenkov: only 72h exceeded.  The expiry must come from the 72h
    window (not from the shorter 4h window, which is within limits).
    sats_freed should be the msats of the oldest tx in the 72h window ÷ 1000.
    """
    mock_col = _make_collection(
        _pipeline_result("azurecherenkov", AZURE_PERIODS_WITH_DETAILS),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        expiry_info = await get_next_limit_expiry("azurecherenkov")

    assert expiry_info is not None
    expiry, sats_freed = expiry_info

    expected_expiry = AZURE_OLDEST_TS + timedelta(hours=72)
    assert abs((expiry - expected_expiry).total_seconds()) < 2

    expected_sats = Decimal(AZURE_DETAIL_MSATS) // 1000  # 155,000
    assert sats_freed == expected_sats

    # Crucially: the 4h period was within limits, so the expiry must NOT come
    # from a 4h calculation.  The expiry is ~47 hours from now, not ~15 minutes.
    assert (expiry - NOW) > timedelta(hours=1), (
        "Expiry should be roughly 47 hours away (72h window), not minutes away"
    )


@pytest.mark.asyncio
async def test_get_next_limit_expiry_returns_soonest_when_multiple_exceeded(mock_v4v_config):
    """
    When both a 4h period and a 72h period are exceeded, get_next_limit_expiry
    should return the SOONEST expiry (the 4h one).
    """
    both_exceeded_ts_4h = NOW - timedelta(hours=3, minutes=30)  # expires in 30 min
    both_exceeded_ts_72h = NOW - timedelta(hours=50)  # expires in 22 hours

    periods_both_exceeded = {
        "4": _period(
            450_000,
            Decimal("450000000"),
            5000,
            350,
            360,
            4,
            400_000,
            details=[
                _detail_entry(both_exceeded_ts_4h, 100_000_000, "testuser"),
            ],
        ),
        "72": _period(
            850_000,
            Decimal("850000000"),
            8000,
            600,
            700,
            72,
            800_000,
            details=[
                _detail_entry(both_exceeded_ts_72h, 200_000_000, "testuser"),
                _detail_entry(NOW - timedelta(hours=20), 650_000_000, "testuser"),
            ],
        ),
        "168": _period(
            850_000,
            Decimal("850000000"),
            8000,
            600,
            700,
            168,
            1_200_000,
            details=[
                _detail_entry(both_exceeded_ts_72h, 200_000_000, "testuser"),
                _detail_entry(NOW - timedelta(hours=20), 650_000_000, "testuser"),
            ],
        ),
    }

    mock_col = _make_collection(
        _pipeline_result("testuser", periods_both_exceeded),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        expiry_info = await get_next_limit_expiry("testuser")

    assert expiry_info is not None
    expiry, sats_freed = expiry_info

    # Should be the 4h expiry (soonest), not the 72h one
    expected_4h_expiry = both_exceeded_ts_4h + timedelta(hours=4)
    assert abs((expiry - expected_4h_expiry).total_seconds()) < 2

    expected_sats = Decimal(100_000_000) // 1000  # 100,000
    assert sats_freed == expected_sats


@pytest.mark.asyncio
async def test_get_next_limit_expiry_period_with_no_details_is_skipped(mock_v4v_config):
    """
    If an over-limit period has an empty details list, it should be skipped
    and the next period with details should be used instead.
    """
    ts_168 = NOW - timedelta(hours=100)

    periods = {
        "4": _period(0, 0, 0, 0, 0, 4, 400_000, details=[]),  # within limit
        "72": _period(
            900_000,
            Decimal("900000000"),
            8000,
            600,
            700,
            72,
            800_000,
            details=[],  # exceeded but NO details (edge case)
        ),
        "168": _period(
            1_300_000,
            Decimal("1300000000"),
            10000,
            800,
            900,
            168,
            1_200_000,
            details=[
                _detail_entry(ts_168, 300_000_000, "edgeuser"),
            ],
        ),
    }

    mock_col = _make_collection(
        _pipeline_result("edgeuser", periods),
    )
    with patch.object(LedgerEntry, "collection", return_value=mock_col):
        expiry_info = await get_next_limit_expiry("edgeuser")

    assert expiry_info is not None
    expiry, sats_freed = expiry_info

    # 72h period had no details → falls through to 168h period
    expected_expiry = ts_168 + timedelta(hours=168)
    assert abs((expiry - expected_expiry).total_seconds()) < 2
    assert sats_freed == Decimal(300_000_000) // 1000  # 300,000
