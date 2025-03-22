import json
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from beem.amount import Amount

from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.hive.hive_extras import get_event_id, get_hive_client
from v4vapp_backend_v2.hive_models.op_types_enums import TransferOpTypes
from v4vapp_backend_v2.models.hive_transfer_model import HiveTransaction

HIVE_ACC_TEST = os.environ.get("HIVE_ACC_TEST", "alice")
HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "TEST_KEY")


@pytest.fixture
def sample_post():
    return {
        "type": "transfer",
        "from": "bdhivesteemspot",
        "to": "bdhivesteem",
        "amount": {"amount": "87122", "precision": 3, "nai": "@@000000021"},
        "memo": "103236048",
        "_id": "c9c72da6ab40ff062ae2aa18867b13f0ae05bc7d",
        "timestamp": datetime(2025, 3, 13, 8, 3, 54, tzinfo=timezone.utc),
        "block_num": 94094107,
        "trx_num": 7,
        "trx_id": "242bfb6cefdbcd5e4e5182538b3d97166225d46a",
        "op_in_trx": 0,
    }


@pytest.fixture
def sample_quote() -> QuoteResponse:
    with open("tests/data/crypto_prices/Binance.json") as f:
        binance_json = json.load(f)

    binance_json_filtered = {
        "hive_usd": binance_json["hive_usd"],
        "hbd_usd": binance_json["hbd_usd"],
        "btc_usd": binance_json["btc_usd"],
        "hive_hbd": binance_json["hive_hbd"],
        "raw_response": binance_json["raw_response"],
        "source": binance_json["source"],
        "fetch_date": datetime.now(tz=timezone.utc),
    }
    binance_resp = QuoteResponse(**binance_json_filtered)
    return binance_resp


@pytest.mark.asyncio
async def test_hive_transaction_initialization(sample_post, sample_quote):
    HiveTransaction.last_quote = sample_quote
    hive_trx = HiveTransaction(**sample_post)
    assert hive_trx.id == get_event_id(sample_post)
    assert hive_trx.trx_id == sample_post["trx_id"]
    assert hive_trx.timestamp == sample_post["timestamp"]
    assert hive_trx.type == sample_post["type"]
    assert hive_trx.op_in_trx == sample_post["op_in_trx"]
    assert hive_trx.hive_from == sample_post["from"]
    assert hive_trx.hive_to == sample_post["to"]
    assert hive_trx.amount == sample_post["amount"]
    assert hive_trx.memo == sample_post["memo"]
    assert hive_trx.block_num == sample_post["block_num"]
    assert hive_trx.amount_str == str(Amount(sample_post["amount"]))
    assert hive_trx.amount_decimal == str(Amount(sample_post["amount"]).amount_decimal)
    assert hive_trx.amount_symbol == Amount(sample_post["amount"]).symbol
    assert hive_trx.amount_value == Amount(sample_post["amount"]).amount
    assert hive_trx.conv.hive == 87.122
    assert hive_trx.conv.usd == 20.857004
    assert hive_trx.conv.sats == 25137
    assert hive_trx.conv.sats_hive == 288.5293


def mock_all_quotes_fixture(all_quotes):
    for quote in all_quotes["quotes"].values():
        quote["fetch_date"] = datetime.now(tz=timezone.utc)
    all_quotes["fetch_date"] = datetime.now(tz=timezone.utc)

    mock_all_quotes = AllQuotes(**all_quotes)

    return mock_all_quotes


def test_instantiation_mock_all_quote_calls(sample_post):
    with open("tests/data/crypto_prices/all_quotes.json", "r") as f:
        all_quotes = json.load(f)
        mock_all_quotes = mock_all_quotes_fixture(all_quotes)

        with patch(
            "v4vapp_backend_v2.models.hive_transfer_model.AllQuotes",
            return_value=mock_all_quotes,
        ):
            with patch(
                "v4vapp_backend_v2.helpers.crypto_prices.AllQuotes.get_all_quotes",
                new=AsyncMock(return_value=None),
            ):
                hive_trx = HiveTransaction(**sample_post)

                assert hive_trx.id == get_event_id(sample_post)
                assert hive_trx.trx_id == sample_post["trx_id"]
                assert hive_trx.timestamp == sample_post["timestamp"]
                assert hive_trx.type == sample_post["type"]
                assert hive_trx.op_in_trx == sample_post["op_in_trx"]
                assert hive_trx.hive_from == sample_post["from"]
                assert hive_trx.hive_to == sample_post["to"]
                assert hive_trx.amount == sample_post["amount"]
                assert hive_trx.memo == sample_post["memo"]
                assert hive_trx.block_num == sample_post["block_num"]
                assert hive_trx.amount_str == str(Amount(sample_post["amount"]))
                assert hive_trx.amount_decimal == str(
                    Amount(sample_post["amount"]).amount_decimal
                )
                assert hive_trx.amount_symbol == Amount(sample_post["amount"]).symbol
                assert hive_trx.amount_value == Amount(sample_post["amount"]).amount
                assert hive_trx.conv.hive == 87.122
                assert hive_trx.conv.usd == 20.857004
                assert hive_trx.conv.sats == 25137
                assert hive_trx.conv.sats_hive == 288.5293


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


@pytest.mark.skipif(
    HIVE_MEMO_TEST_KEY == "TEST_KEY",
    reason="No test key provided.",
)
@pytest.mark.asyncio
async def test_many_hive_transactions(sample_quote, set_base_config_path):
    with open("tests/data/hive/sample_hive_transactions.pkl", "rb") as f:
        all_posts = pickle.load(f)

    # Set up with the sample_quote
    HiveTransaction.last_quote = sample_quote
    hive_inst = get_hive_client(keys=[HIVE_MEMO_TEST_KEY])
    for post in all_posts:
        hive_trx = HiveTransaction(**post, hive_inst=hive_inst)
        assert hive_trx.id == get_event_id(post)
        assert hive_trx.conv.msats > 0
        if hive_trx.hive_from == HIVE_ACC_TEST or hive_trx.hive_to == HIVE_ACC_TEST:
            if hive_trx.memo.startswith("#"):
                assert hive_trx.d_memo != hive_trx.memo
            assert hive_trx.log_str is not None
            assert hive_trx.notification_str is not None
