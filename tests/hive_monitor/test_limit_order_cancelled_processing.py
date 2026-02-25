import asyncio

import pytest
from v4vapp_backend_v2.hive_monitor_v2 import OpBase, combined_logging

from v4vapp_backend_v2.hive_models.op_limit_order_cancelled import LimitOrderCancelled


class Dummy:
    called = False


@pytest.mark.asyncio
async def test_combined_logging_for_cancelled(monkeypatch, caplog):
    # Prepare a sample event and set watch_users to include seller
    sample = {
        "type": "limit_order_cancelled",
        "amount_back": {"amount": "1.000", "nai": "@@000000021", "precision": 3},
        "orderid": 42,
        "seller": "v4vapp",
        "trx_id": "0000",
        "block_num": 100,
    }
    op = LimitOrderCancelled.model_validate(sample)
    OpBase.watch_users = ["v4vapp"]

    # stub db_store_op so we can detect invocation
    async def fake_store(op_arg):
        Dummy.called = True
        return None

    monkeypatch.setattr("v4vapp_backend_v2.hive_monitor_v2.db_store_op", fake_store)

    # call combined_logging with flags indicating the branch triggered
    await combined_logging(op, log_it=True, notification=True, db_store=True, extra_bots=None)

    # wait a bit for the background task to run
    await asyncio.sleep(0.1)

    assert Dummy.called, "db_store_op should have been scheduled"
    # log message should include seller and orderid
    assert "v4vapp" in caplog.text
    assert "42" in caplog.text
