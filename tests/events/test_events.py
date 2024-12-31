import pytest
from v4vapp_backend_v2.events.event import (
    subscribe,
    publish,
    clear_subscribers,
    get_subscribers,
    get_subscribers_for_event,
    remove_subscriber,
    remove_all_subscribers,
)
from v4vapp_backend_v2.events.event_models import Events


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
