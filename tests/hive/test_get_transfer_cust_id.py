from types import SimpleNamespace

from v4vapp_backend_v2.hive.hive_extras import get_transfer_cust_id


def make_hive_cfg(server, treasury, funding, exchange):
    return SimpleNamespace(all_account_names=[server, treasury, funding, exchange])


def test_server_to_treasury():
    cfg = make_hive_cfg("server", "treasury", "funding", "exchange")
    assert get_transfer_cust_id("server", "treasury", hive_config=cfg) == "treasury"


def test_treasury_to_server():
    cfg = make_hive_cfg("server", "treasury", "funding", "exchange")
    assert get_transfer_cust_id("treasury", "server", hive_config=cfg) == "treasury"


def test_server_to_expense():
    cfg = make_hive_cfg("server", "treasury", "funding", "exchange")
    assert get_transfer_cust_id("server", "privex", hive_config=cfg) == "privex"


def test_server_withdrawal_to_customer():
    cfg = make_hive_cfg("server", "treasury", "funding", "exchange")
    assert get_transfer_cust_id("server", "alice", hive_config=cfg) == "alice"


def test_customer_deposit_to_server():
    cfg = make_hive_cfg("server", "treasury", "funding", "exchange")
    assert get_transfer_cust_id("bob", "server", hive_config=cfg) == "bob"


def test_defensive_missing_accounts():
    cfg = SimpleNamespace(all_account_names=[])
    assert get_transfer_cust_id("fromx", "tox", hive_config=cfg) == "tox->fromx"


def test_unknown_pair_returns_colon_format():
    cfg = make_hive_cfg("server", "treasury", "funding", "exchange")
    assert get_transfer_cust_id("foo", "bar", hive_config=cfg) == "bar:foo"
