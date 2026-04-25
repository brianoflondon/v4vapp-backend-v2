import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from tests.get_last_quote import last_quote
from tests.helpers.test_crypto_prices import mock_binance
from tests.load_data import load_hive_events
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.helpers.general_purpose_funcs import find_short_id
from v4vapp_backend_v2.hive_models.op_all import op_any, op_any_or_base, trx_unpack
from v4vapp_backend_v2.hive_models.op_base import HiveExp, OpBase
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_transfer import Transfer

# A valid 40-character lowercase hex Hive transaction ID for use in trx_unpack tests.
FAKE_TRX_ID = "a" * 40


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


def test_all_validate():
    TrackedBaseModel.last_quote = last_quote()

    with httpx.Client() as httpx_client:
        counter = 0
        for hive_event in load_hive_events():
            counter += 1
            try:
                op = op_any(hive_event)
                assert op.op_type == op.name()
                assert op.markdown_link
                if op.link and counter % 10 == 0:
                    if not os.getenv("GITHUB_ACTIONS") == "true":
                        response = httpx_client.head(op.link)
                        assert response.status_code == 200

            except ValueError as e:
                assert "Unknown operation type" in str(e) or "Invalid CustomJson data" in str(e)
            except Exception as e:
                print(e)
                assert False


def test_op_any_or_base():
    TrackedBaseModel.last_quote = last_quote()

    for hive_event in load_hive_events():
        try:
            op = op_any_or_base(hive_event)
            assert isinstance(op, OpBase)
            if op.op_type == "transfer":
                assert isinstance(op, Transfer)
            if op.op_type == "producer_reward":
                assert isinstance(op, ProducerReward)
            assert op.markdown_link

        except ValueError as e:
            assert "Unknown operation type" in str(e) or "Invalid CustomJson data" in str(e)
        except Exception as e:
            print(e)
            assert False


def test_all_block_explorer_links(mocker):
    _ = mock_binance(mocker)
    TrackedBaseModel.last_quote = last_quote()

    for block_explorer in HiveExp:
        tested_type = []
        OpBase.block_explorer = block_explorer
        with httpx.Client() as httpx_client:
            for hive_event in load_hive_events():
                if hive_event.get("type") in tested_type:
                    continue
                try:
                    tested_type.append(hive_event.get("type"))
                    op = op_any(hive_event)
                    assert op.op_type == op.name()
                    print(hive_event.get("type"), op.op_type, op.link)
                    if op.link and not os.getenv("GITHUB_ACTIONS") == "true":
                        response = httpx_client.get(op.link)
                        assert response.status_code == 200

                except ValueError as e:
                    assert "Unknown operation type" in str(e) or "Invalid CustomJson data" in str(
                        e
                    )
                except Exception as e:
                    print(e)
                    assert False


def test_short_id(mocker):
    _ = mock_binance(mocker)
    TrackedBaseModel.last_quote = last_quote()
    for hive_event in load_hive_events():
        try:
            op = op_any(hive_event)
            memo = f"This is a test memo with a short id | § {op.short_id} and some more text."
            print(op.short_id, OpBase.short_id_query(op.short_id), memo)
            assert find_short_id(memo) == op.short_id

        except ValueError as e:
            assert "Unknown operation type" in str(e) or "Invalid CustomJson data" in str(e)
        except Exception as e:
            print(e)
            assert False


def test_hive_account_name_links(mocker):
    _ = mock_binance(mocker)
    TrackedBaseModel.last_quote = last_quote()
    with httpx.Client() as httpx_client:
        for hive_event in load_hive_events():
            try:
                op = op_any(hive_event)
                print(op.short_id, OpBase.short_id_query(op.short_id))
                assert op.op_type == op.name()
                if op.op_type == "transfer" and not os.getenv("GITHUB_ACTIONS") == "true":
                    assert isinstance(op, Transfer)
                    if link_from := op.from_account.link:
                        response = httpx_client.head(link_from)
                        assert response.status_code == 200
                        link_to = op.to_account.link
                        response = httpx_client.head(link_to)
                        assert response.status_code == 200

            except ValueError as e:
                assert "Unknown operation type" in str(e) or "Invalid CustomJson data" in str(e)
            except Exception as e:
                print(e)
                assert False


# ---------------------------------------------------------------------------
# trx_unpack tests — Blockchain.get_transaction is mocked
# ---------------------------------------------------------------------------


def _make_blockchain_mock(response: dict) -> MagicMock:
    """Return a mock whose get_transaction() returns *response*."""
    mock_blockchain = MagicMock()
    mock_blockchain.get_transaction.return_value = response
    return mock_blockchain


def test_trx_unpack_single_custom_json():
    """A transaction with one custom_json_operation is unpacked to [CustomJson]."""
    fake_trx = {
        "block_num": 12345,
        "transaction_num": 1,
        "operations": [
            {
                "type": "custom_json_operation",
                "value": {
                    "required_auths": [],
                    "required_posting_auths": ["alice"],
                    "id": "v4v",
                    "json": '{"memo":"test"}',
                },
            }
        ],
    }
    with patch(
        "v4vapp_backend_v2.hive_models.op_all.Blockchain",
        return_value=_make_blockchain_mock(fake_trx),
    ):
        ops = trx_unpack(FAKE_TRX_ID)

    assert len(ops) == 1
    assert isinstance(ops[0], CustomJson)
    assert ops[0].trx_id == FAKE_TRX_ID
    assert ops[0].block_num == 12345
    assert ops[0].op_in_trx == 1


def test_trx_unpack_multiple_ops():
    """A transaction with a transfer followed by a custom_json is unpacked in order."""
    fake_trx = {
        "block_num": 99999,
        "transaction_num": 3,
        "operations": [
            {
                "type": "transfer_operation",
                "value": {
                    "from": "alice",
                    "to": "bob",
                    "amount": {"amount": "1000", "nai": "@@000000021", "precision": 3},
                    "memo": "",
                },
            },
            {
                "type": "custom_json_operation",
                "value": {
                    "required_auths": [],
                    "required_posting_auths": ["alice"],
                    "id": "v4v",
                    "json": '{"app":"v4v"}',
                },
            },
        ],
    }
    with patch(
        "v4vapp_backend_v2.hive_models.op_all.Blockchain",
        return_value=_make_blockchain_mock(fake_trx),
    ):
        ops = trx_unpack(FAKE_TRX_ID)

    assert len(ops) == 2
    assert isinstance(ops[0], Transfer)
    assert isinstance(ops[1], CustomJson)
    # op_in_trx is 1-based
    assert ops[0].op_in_trx == 1
    assert ops[1].op_in_trx == 2
    # trx_id is propagated to every op
    assert ops[0].trx_id == FAKE_TRX_ID
    assert ops[1].trx_id == FAKE_TRX_ID


def test_trx_unpack_empty_transaction():
    """A transaction with no operations returns an empty list."""
    fake_trx = {"block_num": 1, "transaction_num": 0, "operations": []}
    with patch(
        "v4vapp_backend_v2.hive_models.op_all.Blockchain",
        return_value=_make_blockchain_mock(fake_trx),
    ):
        ops = trx_unpack(FAKE_TRX_ID)

    assert ops == []


def test_trx_unpack_strips_operation_suffix():
    """The '_operation' suffix in op type names is stripped before dispatch."""
    fake_trx = {
        "block_num": 1,
        "transaction_num": 0,
        "operations": [
            {
                "type": "custom_json_operation",  # suffix present
                "value": {
                    "required_auths": [],
                    "required_posting_auths": ["alice"],
                    "id": "v4v",
                    "json": "{}",
                },
            }
        ],
    }
    with patch(
        "v4vapp_backend_v2.hive_models.op_all.Blockchain",
        return_value=_make_blockchain_mock(fake_trx),
    ):
        ops = trx_unpack(FAKE_TRX_ID)

    # Would be OpBase if the suffix were NOT stripped (unknown type "custom_json_operation")
    assert isinstance(ops[0], CustomJson)


def test_trx_unpack_unknown_op_is_skipped():
    """An unrecognised op type is skipped with a warning; the list omits that op."""
    fake_trx = {
        "block_num": 1,
        "transaction_num": 0,
        "operations": [
            {
                "type": "some_future_operation",
                "value": {"data": "xyz"},
            }
        ],
    }
    with patch(
        "v4vapp_backend_v2.hive_models.op_all.Blockchain",
        return_value=_make_blockchain_mock(fake_trx),
    ):
        ops = trx_unpack(FAKE_TRX_ID)

    assert ops == []
