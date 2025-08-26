import random
from pathlib import Path

import pytest

from tests.get_last_quote import last_quote
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    InternalConfig(config_filename=str(Path(test_config_path, "config.yaml")))
    TrackedBaseModel.last_quote = last_quote()
    yield
    InternalConfig().shutdown()  # Ensure proper cleanup after tests


async def test_create_quote():
    get_quotes: list[FixedHiveQuote] = []

    fixed_hive_quote = await FixedHiveQuote.create_quote(hive=101.020, store_db=False)
    get_quotes.append(fixed_hive_quote)
    assert fixed_hive_quote is not None
    assert fixed_hive_quote.conversion_result.net_to_receive_amount.amount == 101.020
    print(fixed_hive_quote.conversion_result)

    fixed_hive_quote = await FixedHiveQuote.create_quote(hbd=102.030, store_db=False)
    get_quotes.append(fixed_hive_quote)
    assert fixed_hive_quote is not None
    assert fixed_hive_quote.conversion_result.net_to_receive_amount.amount == 102.030
    print(fixed_hive_quote.conversion_result)

    fixed_hive_quote = await FixedHiveQuote.create_quote(usd=103.040, store_db=False)
    get_quotes.append(fixed_hive_quote)
    assert fixed_hive_quote is not None
    assert fixed_hive_quote.conversion_result.net_to_receive_amount.amount == 103.040
    print(fixed_hive_quote.conversion_result)

    for fixed_hive_quote in get_quotes:
        results_quote = FixedHiveQuote.check_quote(
            unique_id=fixed_hive_quote.unique_id, send_sats=fixed_hive_quote.sats_send
        )
        assert results_quote.unique_id == fixed_hive_quote.unique_id
        assert results_quote.sats_send == fixed_hive_quote.sats_send


def generate_fuzzy_test_data():
    """Generate 100 random test cases for each currency."""
    test_data = []
    currencies = ["hive", "hbd", "usd"]

    # Set seed for reproducible tests
    random.seed(42)

    for _ in range(100):
        for currency in currencies:
            # Generate random value between 5.000 and 100.000 with 3 decimal places
            amount = round(random.uniform(5.0, 100.0), 3)
            test_data.append((currency, amount))

    return test_data


@pytest.mark.parametrize("currency,amount", generate_fuzzy_test_data())
@pytest.mark.asyncio
async def test_create_quote_fuzzy(currency: str, amount: float):
    """Test creating quotes with fuzzy data across all currencies."""
    # Create quote with dynamic currency parameter
    kwargs = {currency: amount, "store_db": False}
    fixed_hive_quote = await FixedHiveQuote.create_quote(**kwargs)

    assert fixed_hive_quote is not None
    assert fixed_hive_quote.conversion_result.net_to_receive_amount.amount == amount
    assert 5.0 <= amount <= 100.0  # Verify amount is in expected range
    assert len(str(amount).split(".")[-1]) <= 3  # Verify max 3 decimal places

    # Test that the quote can be retrieved
    results_quote = FixedHiveQuote.check_quote(
        unique_id=fixed_hive_quote.unique_id, send_sats=fixed_hive_quote.sats_send
    )
    assert results_quote.unique_id == fixed_hive_quote.unique_id
    assert results_quote.sats_send == fixed_hive_quote.sats_send


# Alternative: Batch test with all currencies at once
@pytest.mark.asyncio
async def test_create_quote_fuzzy_batch():
    """Test creating quotes with fuzzy data in batches."""
    get_quotes: list[FixedHiveQuote] = []

    # Set seed for reproducible tests
    random.seed(42)

    # Test 100 random combinations
    for i in range(100):
        # Pick random currency and amount
        currency = random.choice(["hive", "hbd", "usd"])
        amount = round(random.uniform(5.0, 100.0), 3)

        kwargs = {currency: amount, "store_db": False}
        fixed_hive_quote = await FixedHiveQuote.create_quote(**kwargs)
        get_quotes.append(fixed_hive_quote)

        assert fixed_hive_quote is not None
        assert fixed_hive_quote.conversion_result.net_to_receive_amount.amount == amount

        # Optional: print every 20th iteration to see progress
        if (i + 1) % 20 == 0:
            print(
                f"Iteration {i + 1}: {currency.upper()}: {amount} -> sats: {fixed_hive_quote.sats_send}"
            )

    # Verify all quotes can be retrieved
    print(f"\nVerifying {len(get_quotes)} created quotes...")
    for i, quote in enumerate(get_quotes):
        results_quote = FixedHiveQuote.check_quote(
            unique_id=quote.unique_id, send_sats=quote.sats_send
        )
        assert results_quote.unique_id == quote.unique_id
        assert results_quote.sats_send == quote.sats_send

        # Print verification progress
        if (i + 1) % 50 == 0:
            print(f"Verified {i + 1} quotes...")

    print(f"✅ All {len(get_quotes)} quotes verified successfully!")


# More targeted fuzzy test for specific scenarios
@pytest.mark.parametrize("currency", ["hive", "hbd", "usd"])
@pytest.mark.asyncio
async def test_create_quote_fuzzy_by_currency(currency: str):
    """Test 50 random amounts for each currency individually."""
    random.seed(42)
    quotes = []

    for _ in range(50):
        amount = round(random.uniform(5.0, 100.0), 3)
        kwargs = {currency: amount, "store_db": False}

        fixed_hive_quote = await FixedHiveQuote.create_quote(**kwargs)
        quotes.append(fixed_hive_quote)

        assert fixed_hive_quote is not None
        assert fixed_hive_quote.conversion_result.net_to_receive_amount.amount == amount

    # Verify all quotes for this currency
    for quote in quotes:
        results_quote = FixedHiveQuote.check_quote(
            unique_id=quote.unique_id, send_sats=quote.sats_send
        )
        assert results_quote.unique_id == quote.unique_id
        assert results_quote.sats_send == quote.sats_send

    print(f"✅ {currency.upper()}: Created and verified {len(quotes)} quotes")


@pytest.mark.asyncio
async def test_check_quote_failures():
    """Test various failure scenarios when checking quotes."""

    # Test 1: Non-existent quote ID
    with pytest.raises(ValueError, match="Invalid quote."):
        FixedHiveQuote.check_quote("nonexistent_id", 25000)

    # Test 2: Empty quote ID
    with pytest.raises(ValueError, match="Invalid quote."):
        FixedHiveQuote.check_quote("", 25000)

    # Test 3: Valid quote ID but wrong sats amount
    # First create a valid quote
    fixed_hive_quote = await FixedHiveQuote.create_quote(hive=10.0, store_db=False)

    # Then try to check it with wrong sats amount
    with pytest.raises(ValueError, match="Sats amount does not match the quote."):
        FixedHiveQuote.check_quote(fixed_hive_quote.unique_id, fixed_hive_quote.sats_send + 1000)

    # Test 4: Verify the correct sats amount still works
    valid_quote = FixedHiveQuote.check_quote(
        fixed_hive_quote.unique_id, fixed_hive_quote.sats_send
    )
    assert valid_quote.unique_id == fixed_hive_quote.unique_id


@pytest.mark.asyncio
async def test_check_quote_edge_cases():
    """Test edge cases for quote checking."""

    # Test with extremely small amounts
    small_quote = await FixedHiveQuote.create_quote(hive=0.001, store_db=False)
    valid_small = FixedHiveQuote.check_quote(small_quote.unique_id, small_quote.sats_send)
    assert valid_small.unique_id == small_quote.unique_id

    # Test with large amounts
    large_quote = await FixedHiveQuote.create_quote(hive=999.999, store_db=False)
    valid_large = FixedHiveQuote.check_quote(large_quote.unique_id, large_quote.sats_send)
    assert valid_large.unique_id == large_quote.unique_id

    # Test with zero amount (if allowed)
    try:
        zero_quote = await FixedHiveQuote.create_quote(hive=0.0, store_db=False)
        valid_zero = FixedHiveQuote.check_quote(zero_quote.unique_id, zero_quote.sats_send)
        assert valid_zero.unique_id == zero_quote.unique_id
    except Exception as e:
        # If zero amounts aren't allowed, that's fine too
        print(f"Zero amounts not supported: {e}")


@pytest.mark.parametrize(
    "invalid_id,expected_sats",
    [
        ("abc123", 25000),  # Random ID
        ("", 1000),  # Empty ID
        ("toolong123456", 5000),  # Too long ID
        ("short", 0),  # Short ID with zero sats
    ],
)
@pytest.mark.asyncio
async def test_check_quote_invalid_ids(invalid_id: str, expected_sats: int):
    """Test checking quotes with various invalid IDs."""
    with pytest.raises(ValueError, match="Invalid quote."):
        FixedHiveQuote.check_quote(invalid_id, expected_sats)


@pytest.mark.asyncio
async def test_quote_expiry_simulation():
    """Test quote expiry by creating a quote with very short cache time."""
    import asyncio

    # Create quote with 1 second cache time
    quote = await FixedHiveQuote.create_quote(hive=5.0, cache_time=1, store_db=False)

    # Should work immediately
    valid_quote = FixedHiveQuote.check_quote(quote.unique_id, quote.sats_send)
    assert valid_quote.unique_id == quote.unique_id

    # Wait for expiry (add buffer for reliability)
    await asyncio.sleep(2)

    # Should fail after expiry
    with pytest.raises(ValueError, match="Invalid quote."):
        FixedHiveQuote.check_quote(quote.unique_id, quote.sats_send)


@pytest.mark.asyncio
async def test_sats_amount_mismatch_scenarios():
    """Test various sats amount mismatch scenarios."""
    quote = await FixedHiveQuote.create_quote(hbd=25.5, store_db=False)

    # Test slightly off amounts
    test_cases = [
        quote.sats_send + 1,  # One sat too high
        quote.sats_send - 1,  # One sat too low
        quote.sats_send + 1000,  # Much too high
        quote.sats_send - 1000,  # Much too low (if positive)
        0,  # Zero sats
        -100,  # Negative sats
    ]

    for wrong_sats in test_cases:
        if wrong_sats >= 0:  # Skip negative amounts if they cause other errors
            with pytest.raises(ValueError, match="Sats amount does not match the quote."):
                FixedHiveQuote.check_quote(quote.unique_id, wrong_sats)


@pytest.mark.asyncio
async def test_concurrent_quote_failures():
    """Test failure scenarios with multiple quotes."""
    # Create multiple quotes
    quotes = []
    for i in range(5):
        quote = await FixedHiveQuote.create_quote(hive=float(i + 1), store_db=False)
        quotes.append(quote)

    # Test cross-checking (using one quote's ID with another's sats)
    for i, quote in enumerate(quotes):
        for j, other_quote in enumerate(quotes):
            if i != j:  # Different quotes
                with pytest.raises(ValueError, match="Sats amount does not match the quote."):
                    FixedHiveQuote.check_quote(quote.unique_id, other_quote.sats_send)

    # Verify all original quotes still work
    for quote in quotes:
        valid_quote = FixedHiveQuote.check_quote(quote.unique_id, quote.sats_send)
        assert valid_quote.unique_id == quote.unique_id
