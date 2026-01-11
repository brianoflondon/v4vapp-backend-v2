from pathlib import Path

import pytest

from v4vapp_backend_v2.events.event import (
    clear_subscribers,
    get_subscribers,
    get_subscribers_for_event,
    publish,
    remove_all_subscribers,
    remove_subscriber,
    subscribe,
)
from v4vapp_backend_v2.events.event_models import Events


@pytest.fixture(autouse=True)
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
    # Unpatch the monkeypatch
    monkeypatch.undo()


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


# Fixture to clear the subscribers before each test
@pytest.fixture(autouse=True)
def clear_subscribers_fixture():
    clear_subscribers()
    yield
    clear_subscribers()


def test_subscribe():
    def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    subscribe(Events.LND_INVOICE, subscriber)
    subscribers = get_subscribers()
    assert Events.LND_INVOICE in subscribers
    assert subscriber in subscribers[Events.LND_INVOICE]


def test_publish():
    def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    subscribe(Events.LND_PAYMENT, subscriber)
    publish(Events.LND_PAYMENT, 1, 2)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT]


def test_publish_wrong_args():
    def subscriber(x: int, y: int):
        print("Subscriber called with", x, y)
        pass

    subscribe(Events.LND_PAYMENT, subscriber)
    try:
        publish(Events.LND_PAYMENT, 1)
    except Exception as e:
        print(e)
    publish(Events.LND_PAYMENT, 1, 2)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT]


def test_clear_subscribers():
    def subscriber():
        pass

    subscribe(Events.LND_PAYMENT, subscriber)
    clear_subscribers()
    subscribers = get_subscribers()
    assert not subscribers


def test_get_subscribers():
    def subscriber():
        pass

    subscribe(Events.LND_PAYMENT, subscriber)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT in subscribers
    assert subscriber in subscribers[Events.LND_PAYMENT]


def test_get_subscribers_for_event():
    def subscriber():
        pass

    subscribe(Events.LND_PAYMENT, subscriber)
    subscribers = get_subscribers_for_event(Events.LND_PAYMENT)
    assert subscriber in subscribers


def test_remove_subscriber():
    def subscriber():
        pass

    subscribe(Events.LND_PAYMENT, subscriber)
    remove_subscriber(Events.LND_PAYMENT, subscriber)
    subscribers = get_subscribers()
    assert Events.LND_PAYMENT not in subscribers


def test_remove_all_subscribers():
    def subscriber():
        pass

    subscribe(Events.LND_PAYMENT, subscriber)
    remove_all_subscribers()
    subscribers = get_subscribers()
    assert not subscribers
