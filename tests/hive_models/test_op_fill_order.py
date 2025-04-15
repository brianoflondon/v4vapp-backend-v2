import json

from nectar.amount import Amount

from tests.load_data import load_hive_events
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive_models.op_all import op_any
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_model_validate_op_fill_order():
    for hive_event in load_hive_events(OpTypes.FILL_ORDER):
        if hive_event["type"] == "fill_order":
            op_fill_order = FillOrder.model_validate(hive_event)
            assert op_fill_order.trx_id == hive_event["trx_id"]
            assert op_fill_order.current_pays.amount == hive_event["current_pays"]["amount"]
            assert op_fill_order.open_pays.amount == hive_event["open_pays"]["amount"]
            assert op_fill_order.current_pays.symbol == Amount(hive_event["current_pays"]).symbol
            print(op_fill_order.log_str)


def test_log_op_fill_order():
    for hive_event in load_hive_events(OpTypes.FILL_ORDER):
        if hive_event["type"] == "fill_order":
            op_fill_order = FillOrder.model_validate(hive_event)
            assert op_fill_order.log_extra
            logger.info(op_fill_order.log_str, extra=op_fill_order.log_extra)


def test_model_dump_fill_order():
    for hive_event in load_hive_events(OpTypes.FILL_ORDER):
        if hive_event["type"] == "fill_order":
            op_fill_order = FillOrder.model_validate(hive_event)
            fill_order = op_fill_order.model_dump()
            assert "log_internal" not in fill_order
            assert "trx_id" in fill_order
            assert "current_pays" in fill_order


def test_create_order_fill_order():
    """
    Test the process of creating and filling an order.

    This test reads a JSONL file containing operation data for orders, processes
    the data to simulate order creation and filling, and verifies the expected
    behavior. It ensures that:
    - At least one log entry is generated.
    - There are multiple logs containing the text "|has been filled".

    Steps:
    1. Load operation data from a JSONL file.
    2. Extract and clean relevant fields from the data.
    3. Simulate operations and collect notification logs.
    4. Assert that logs are generated and contain the expected content.

    Assertions:
    - The number of logs generated is greater than zero.
    - The number of logs containing the text "|has been filled" is greater than one.

    Note:
    - The file `tests/data/hive_models/complete_sell_fill.jsonl` is used as input.
    - The `LimitOrderCreate.watch_users` is set to `["v4vapp"]` for the test.
    """
    LimitOrderCreate.watch_users = ["v4vapp"]
    FillOrder.watch_users = ["v4vapp"]
    filename = "tests/data/hive_models/complete_sell_fill.jsonl"
    LimitOrderCreate.open_order_ids = {}
    LimitOrderCreate.watch_users = ["v4vapp"]
    all_logs = []
    with open(filename, "r") as f:
        for line in f:
            line_json = json.loads(line)
            op_data = line_json.get("fill_order", None)
            if op_data is None:
                op_data = line_json.get("limit_order_create", None)
            if op_data is None:
                continue
            if op_data.get("amount_remaining", None) is not None:
                del op_data["amount_remaining"]
                del op_data["link"]
            if op_data.get("log_str", None) is not None:
                del op_data["log_str"]

            op = op_any(op_data)
            assert op.is_watched
            all_logs.append(op.notification_str)
    assert len(all_logs) > 0
    for log in all_logs:
        print(log)
    # count text "|has been filled" in all_logs
    assert len([log for log in all_logs if "has been filled" in log]) == 2


def test_expire_orders():
    filename = "tests/data/hive_models/complete_sell_fill.jsonl"
    LimitOrderCreate.watch_users = ["v4vapp"]
    with open(filename, "r") as f:
        for line in f:
            line_json = json.loads(line)
            op_data = line_json.get("limit_order_create", None)
            if op_data:
                if op_data.get("amount_remaining", None) is not None:
                    del op_data["amount_remaining"]
                    del op_data["link"]
                op = LimitOrderCreate.model_validate(op_data)
                op.expire_orders()
