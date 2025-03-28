from beem.amount import Amount  # type: ignore

from tests.load_data import load_hive_events
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_model_validate_op_fill_order():
    for hive_event in load_hive_events(OpTypes.FILL_ORDER):
        if hive_event["type"] == "fill_order":
            op_fill_order = FillOrder.model_validate(hive_event)
            assert op_fill_order.trx_id == hive_event["trx_id"]
            assert (
                op_fill_order.current_pays.amount
                == hive_event["current_pays"]["amount"]
            )
            assert op_fill_order.open_pays.amount == hive_event["open_pays"]["amount"]
            assert (
                op_fill_order.current_pays.symbol
                == Amount(hive_event["current_pays"]).symbol
            )
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
