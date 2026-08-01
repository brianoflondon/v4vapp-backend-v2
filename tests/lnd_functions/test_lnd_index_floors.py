"""Tests for optional LND index floors (start_add_index / start_payment_index)."""

from lnd_monitor_v2 import (
    LndIndexFloors,
    apply_invoice_index_floors,
    get_lnd_index_floors,
    should_ignore_invoice_index,
    should_ignore_payment_index,
)
from v4vapp_backend_v2.config.setup import InternalConfig


def test_apply_invoice_index_floors_raises_to_config_floor():
    floors = LndIndexFloors(add_index=100, settle_index=50)
    assert apply_invoice_index_floors(10, 5, floors) == (100, 50)
    assert apply_invoice_index_floors(200, 80, floors) == (200, 80)
    assert apply_invoice_index_floors(None, None, floors) == (100, 50)


def test_apply_invoice_index_floors_noop_when_zero():
    floors = LndIndexFloors()
    assert apply_invoice_index_floors(12, 3, floors) == (12, 3)
    assert apply_invoice_index_floors(0, 0, floors) == (0, 0)


def test_should_ignore_invoice_index_exclusive_floor():
    floors = LndIndexFloors(add_index=10)
    assert should_ignore_invoice_index(10, floors) is True
    assert should_ignore_invoice_index(9, floors) is True
    assert should_ignore_invoice_index(11, floors) is False
    assert should_ignore_invoice_index(0, floors) is True
    assert should_ignore_invoice_index(None, floors) is True


def test_should_ignore_payment_index_exclusive_floor():
    floors = LndIndexFloors(payment_index=5)
    assert should_ignore_payment_index(5, floors) is True
    assert should_ignore_payment_index(6, floors) is False
    assert should_ignore_payment_index(1, floors) is True


def test_should_ignore_noop_when_floor_unset():
    floors = LndIndexFloors()
    assert should_ignore_invoice_index(1, floors) is False
    assert should_ignore_payment_index(1, floors) is False


def test_get_lnd_index_floors_from_test_config():
    """example2 in tests/data/config/config.yaml has floors set."""
    InternalConfig(config_filename="config.yaml")
    floors = get_lnd_index_floors("example2")
    assert floors.add_index == 42
    assert floors.payment_index == 7
    # settle must NOT default to add_index (independent LND sequence)
    assert floors.settle_index == 0


def test_get_lnd_index_floors_missing_fields_are_zero():
    InternalConfig(config_filename="config.yaml")
    floors = get_lnd_index_floors("example")
    assert floors.add_index == 0
    assert floors.settle_index == 0
    assert floors.payment_index == 0
    assert floors.has_any is False
