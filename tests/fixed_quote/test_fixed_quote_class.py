import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError

from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConvV1
from v4vapp_backend_v2.helpers.crypto_prices import Currency, HiveRatesDB, QuoteResponse

# @pytest.fixture(autouse=True, scope="module")
# def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
#     test_config_path = Path("tests/data/config")
#     monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
#     test_config_logging_path = Path(test_config_path, "logging/")
#     monkeypatch.setattr(
#         "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
#         test_config_logging_path,
#     )
#     monkeypatch.setattr(
#         "v4vapp_backend_v2.lnd_grpc.lnd_connection.InternalConfig._instance",
#         None,
#     )
#     monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing."""
    redis_mock = MagicMock()
    return redis_mock


@pytest.fixture
def sample_quote_response():
    """Sample QuoteResponse for testing."""
    return QuoteResponse(
        hive_usd=0.25,
        hbd_usd=1.0,
        btc_usd=50000.0,
        hive_hbd=0.25,
        raw_response={},
        source="TestSource",
        fetch_date=datetime.now(tz=timezone.utc),
        error="",
        error_details={},
    )


@pytest.fixture
def sample_hive_rates_db():
    """Sample HiveRatesDB for testing."""
    return HiveRatesDB(
        hive_usd=0.25,
        hbd_usd=1.0,
        btc_usd=50000.0,
        hive_hbd=0.25,
        sats_hive=200.0,
        sats_hbd=50.0,
        sats_usd=115000.0,
        timestamp=datetime.now(tz=timezone.utc),
    )


@pytest.fixture
def sample_crypto_conv():
    """Sample CryptoConv for testing."""
    return CryptoConv(
        sats=25000,
        hive=0.1,
        hbd=0.025,
        usd=0.025,
    )


@pytest.fixture
def sample_fixed_quote(sample_crypto_conv, sample_hive_rates_db):
    """Sample FixedHiveQuote for testing."""
    return FixedHiveQuote(
        unique_id="abc123",
        sats_send=25000,
        conv=sample_crypto_conv.v1(),
        quote_record=sample_hive_rates_db,
    )


class TestFixedHiveQuote:
    """Test class for FixedHiveQuote functionality."""

    def test_model_creation(self, sample_crypto_conv, sample_hive_rates_db):
        """Test basic model creation."""
        quote = FixedHiveQuote(
            unique_id="test123",
            sats_send=1000,
            conv=sample_crypto_conv.v1(),
            quote_record=sample_hive_rates_db,
        )

        assert quote.unique_id == "test123"
        assert quote.sats_send == 1000
        assert isinstance(quote.conv, CryptoConvV1)
        assert quote.quote_record == sample_hive_rates_db
        assert isinstance(quote.timestamp, datetime)

    def test_model_validation_error(self):
        """Test model validation with invalid data."""
        with pytest.raises(ValidationError):
            FixedHiveQuote(
                unique_id="",  # Invalid empty string
                sats_send="invalid",  # Invalid type
                conv={},  # Invalid conv data
            )

    @pytest.mark.asyncio
    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    async def test_create_quote_with_hive(
        self, mock_internal_config, mock_redis, sample_quote_response, sample_hive_rates_db
    ):
        """Test creating a quote with HIVE amount."""
        # Setup mocks
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.setex.return_value = True

        mock_all_quotes = AsyncMock()
        mock_all_quotes.get_all_quotes = AsyncMock()
        mock_all_quotes.db_store_quote = AsyncMock(return_value=sample_hive_rates_db)
        mock_all_quotes.quote = sample_quote_response

        with patch(
            "v4vapp_backend_v2.fixed_quote.fixed_quote_class.AllQuotes",
            return_value=mock_all_quotes,
        ):
            with patch(
                "v4vapp_backend_v2.fixed_quote.fixed_quote_class.CryptoConversion"
            ) as mock_crypto_conversion:
                mock_conversion = MagicMock()
                mock_conversion.sats = 25000
                mock_conversion.v1.return_value = CryptoConvV1(
                    conv_from=Currency.HIVE, sats=25000, HIVE=0.1, HBD=0.025, USD=0.025
                )
                mock_crypto_conversion.return_value.conversion = mock_conversion

                quote = await FixedHiveQuote.create_quote(hive=0.1)

                assert isinstance(quote, FixedHiveQuote)
                assert quote.sats_send == 25000
                assert len(quote.unique_id) == 6
                assert quote.quote_record == sample_hive_rates_db

                # Verify mocks were called correctly
                mock_all_quotes.get_all_quotes.assert_called_once_with(
                    store_db=False, use_cache=True
                )
                mock_all_quotes.db_store_quote.assert_called_once()
                mock_redis.setex.assert_called_once()

    @pytest.mark.asyncio
    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    async def test_create_quote_with_hbd(
        self, mock_internal_config, mock_redis, sample_quote_response, sample_hive_rates_db
    ):
        """Test creating a quote with HBD amount."""
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.setex.return_value = True

        mock_all_quotes = AsyncMock()
        mock_all_quotes.get_all_quotes = AsyncMock()
        mock_all_quotes.db_store_quote = AsyncMock(return_value=sample_hive_rates_db)
        mock_all_quotes.quote = sample_quote_response

        with patch(
            "v4vapp_backend_v2.fixed_quote.fixed_quote_class.AllQuotes",
            return_value=mock_all_quotes,
        ):
            with patch(
                "v4vapp_backend_v2.fixed_quote.fixed_quote_class.CryptoConversion"
            ) as mock_crypto_conversion:
                mock_conversion = MagicMock()
                mock_conversion.sats = 50000
                mock_conversion.v1.return_value = CryptoConvV1(
                    conv_from=Currency.HBD, sats=50000, HIVE=0.2, HBD=0.05, USD=0.05
                )
                mock_crypto_conversion.return_value.conversion = mock_conversion

                quote = await FixedHiveQuote.create_quote(hbd=0.05)

                assert isinstance(quote, FixedHiveQuote)
                assert quote.sats_send == 50000

    @pytest.mark.asyncio
    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    async def test_create_quote_with_usd(
        self, mock_internal_config, mock_redis, sample_quote_response, sample_hive_rates_db
    ):
        """Test creating a quote with USD amount."""
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.setex.return_value = True

        mock_all_quotes = AsyncMock()
        mock_all_quotes.get_all_quotes = AsyncMock()
        mock_all_quotes.db_store_quote = AsyncMock(return_value=sample_hive_rates_db)
        mock_all_quotes.quote = sample_quote_response

        with patch(
            "v4vapp_backend_v2.fixed_quote.fixed_quote_class.AllQuotes",
            return_value=mock_all_quotes,
        ):
            with patch(
                "v4vapp_backend_v2.fixed_quote.fixed_quote_class.CryptoConversion"
            ) as mock_crypto_conversion:
                mock_conversion = MagicMock()
                mock_conversion.sats = 2000
                mock_conversion.v1.return_value = CryptoConvV1(
                    conv_from=Currency.USD, sats=2000, HIVE=0.008, HBD=0.002, USD=0.001
                )
                mock_crypto_conversion.return_value.conversion = mock_conversion

                quote = await FixedHiveQuote.create_quote(usd=0.001)

                assert isinstance(quote, FixedHiveQuote)
                assert quote.sats_send == 2000

    @pytest.mark.asyncio
    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    async def test_create_quote_no_amounts(
        self, mock_internal_config, mock_redis, sample_quote_response, sample_hive_rates_db
    ):
        """Test creating a quote with no amounts (defaults to 0)."""
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.setex.return_value = True

        mock_all_quotes = AsyncMock()
        mock_all_quotes.get_all_quotes = AsyncMock()
        mock_all_quotes.db_store_quote = AsyncMock(return_value=sample_hive_rates_db)
        mock_all_quotes.quote = sample_quote_response

        with patch(
            "v4vapp_backend_v2.fixed_quote.fixed_quote_class.AllQuotes",
            return_value=mock_all_quotes,
        ):
            with patch(
                "v4vapp_backend_v2.fixed_quote.fixed_quote_class.CryptoConversion"
            ) as mock_crypto_conversion:
                mock_conversion = MagicMock()
                mock_conversion.sats = 0
                mock_conversion.v1.return_value = CryptoConvV1(
                    conv_from=Currency.HIVE, sats=0, HIVE=0.0, HBD=0.0, USD=0.0
                )
                mock_crypto_conversion.return_value.conversion = mock_conversion

                quote = await FixedHiveQuote.create_quote()

                assert isinstance(quote, FixedHiveQuote)
                assert quote.sats_send == 0

    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    def test_check_quote_valid(
        self, mock_internal_config, mock_redis, sample_fixed_quote, sample_hive_rates_db
    ):
        """Test checking a valid quote."""
        mock_internal_config.redis_decoded = mock_redis

        # Mock Redis response - use model_dump_json() to handle datetime serialization
        quote_data = {
            "unique_id": "abc123",
            "sats_send": 25000,
            "quote_record": json.loads(
                sample_hive_rates_db.model_dump_json()
            ),  # Convert to dict with serialized datetime
        }
        mock_redis.get.return_value = json.dumps(quote_data)

        result = FixedHiveQuote.check_quote("abc123", 25000)

        assert isinstance(result, QuoteResponse)
        assert result.hive_usd == 0.25
        assert result.hbd_usd == 1.0
        assert result.btc_usd == 50000.0
        assert result.source == "HiveRatesDB"

    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    def test_check_quote_not_found(self, mock_internal_config, mock_redis):
        """Test checking a quote that doesn't exist."""
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.get.return_value = None

        with pytest.raises(ValueError, match="Invalid quote"):
            FixedHiveQuote.check_quote("nonexistent", 1000)

    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    def test_check_quote_sats_mismatch(
        self, mock_internal_config, mock_redis, sample_hive_rates_db
    ):
        """Test checking a quote with mismatched sats amount."""
        mock_internal_config.redis_decoded = mock_redis

        quote_data = {
            "unique_id": "abc123",
            "sats_send": 25000,
            "quote_record": json.loads(
                sample_hive_rates_db.model_dump_json()
            ),  # Convert to dict with serialized datetime
        }
        mock_redis.get.return_value = json.dumps(quote_data)

        with pytest.raises(ValueError, match="Sats amount does not match the quote"):
            FixedHiveQuote.check_quote("abc123", 30000)  # Different amount

    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    def test_check_quote_invalid_json(self, mock_internal_config, mock_redis):
        """Test checking a quote with invalid JSON data."""
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.get.return_value = "invalid json"

        with pytest.raises(json.JSONDecodeError):
            FixedHiveQuote.check_quote("abc123", 1000)

    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    def test_check_quote_missing_quote_record(self, mock_internal_config, mock_redis):
        """Test checking a quote without quote_record."""
        mock_internal_config.redis_decoded = mock_redis

        quote_data = {
            "unique_id": "abc123",
            "sats_send": 25000,
            # Missing quote_record
        }
        mock_redis.get.return_value = json.dumps(quote_data)

        with pytest.raises(ValueError, match="Invalid quote"):
            FixedHiveQuote.check_quote("abc123", 25000)

    def test_model_dump_json(self, sample_fixed_quote):
        """Test JSON serialization of the model."""
        json_str = sample_fixed_quote.model_dump_json(exclude_none=True)
        data = json.loads(json_str)

        assert "unique_id" in data
        assert "sats_send" in data
        assert "conv" in data
        assert "timestamp" in data
        assert "quote_record" in data

    def test_unique_id_generation(self):
        """Test that unique IDs are properly generated."""
        # Test that UUID is 6 characters
        unique_id = str(uuid4())[:6]
        assert len(unique_id) == 6
        assert all(c in "0123456789abcdef-" for c in unique_id)

    @pytest.mark.asyncio
    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    async def test_create_quote_custom_cache_time(
        self, mock_internal_config, mock_redis, sample_quote_response, sample_hive_rates_db
    ):
        """Test creating a quote with custom cache time."""
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.setex.return_value = True

        mock_all_quotes = AsyncMock()
        mock_all_quotes.get_all_quotes = AsyncMock()
        mock_all_quotes.db_store_quote = AsyncMock(return_value=sample_hive_rates_db)
        mock_all_quotes.quote = sample_quote_response

        with patch(
            "v4vapp_backend_v2.fixed_quote.fixed_quote_class.AllQuotes",
            return_value=mock_all_quotes,
        ):
            with patch(
                "v4vapp_backend_v2.fixed_quote.fixed_quote_class.CryptoConversion"
            ) as mock_crypto_conversion:
                mock_conversion = MagicMock()
                mock_conversion.sats = 1000
                mock_conversion.v1.return_value = CryptoConvV1(
                    conv_from=Currency.HIVE, sats=1000, HIVE=0.004, HBD=0.001, USD=0.001
                )
                mock_crypto_conversion.return_value.conversion = mock_conversion

                await FixedHiveQuote.create_quote(hive=0.004, cache_time=1200)

                # Check that setex was called with the correct cache time
                mock_redis.setex.assert_called_once()
                call_args = mock_redis.setex.call_args
                assert call_args[1]["time"] == 1200  # Custom cache time

    @pytest.mark.asyncio
    @patch("v4vapp_backend_v2.fixed_quote.fixed_quote_class.InternalConfig")
    async def test_create_quote_no_cache(
        self, mock_internal_config, mock_redis, sample_quote_response, sample_hive_rates_db
    ):
        """Test creating a quote with use_cache=False."""
        mock_internal_config.redis_decoded = mock_redis
        mock_redis.setex.return_value = True

        mock_all_quotes = AsyncMock()
        mock_all_quotes.get_all_quotes = AsyncMock()
        mock_all_quotes.db_store_quote = AsyncMock(return_value=sample_hive_rates_db)
        mock_all_quotes.quote = sample_quote_response

        with patch(
            "v4vapp_backend_v2.fixed_quote.fixed_quote_class.AllQuotes",
            return_value=mock_all_quotes,
        ):
            with patch(
                "v4vapp_backend_v2.fixed_quote.fixed_quote_class.CryptoConversion"
            ) as mock_crypto_conversion:
                mock_conversion = MagicMock()
                mock_conversion.sats = 1000
                mock_conversion.v1.return_value = CryptoConvV1(
                    conv_from=Currency.HIVE, sats=1000, HIVE=0.004, HBD=0.001, USD=0.001
                )
                mock_crypto_conversion.return_value.conversion = mock_conversion

                await FixedHiveQuote.create_quote(hive=0.004, use_cache=False)

                # Verify get_all_quotes was called with use_cache=False
                mock_all_quotes.get_all_quotes.assert_called_once_with(
                    store_db=False, use_cache=False
                )
