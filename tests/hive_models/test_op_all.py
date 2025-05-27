from pathlib import Path

import httpx
import pytest

from tests.get_last_quote import last_quote
from tests.helpers.test_crypto_prices import mock_binance
from tests.load_data import load_hive_events
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.hive_models.op_all import op_any, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base import HiveExp, OpBase
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_transfer import Transfer


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
        for hive_event in load_hive_events():
            try:
                op = op_any(hive_event)
                assert op.type == op.name()
                assert op.markdown_link
                if op.link:
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
            if op.type == "transfer":
                assert isinstance(op, Transfer)
            if op.type == "producer_reward":
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
                    assert op.type == op.name()
                    print(hive_event.get("type"), op.type, op.link)
                    if op.link:
                        response = httpx_client.get(op.link)
                        assert response.status_code == 200

                except ValueError as e:
                    assert "Unknown operation type" in str(e) or "Invalid CustomJson data" in str(
                        e
                    )
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
                assert op.type == op.name()
                if op.type == "transfer":
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
