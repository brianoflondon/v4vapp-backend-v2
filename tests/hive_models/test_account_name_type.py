import pytest

from v4vapp_backend_v2.hive_models.account_name_type import AccName


@pytest.mark.parametrize(
    "value,expected",
    [
        ("devser.v4vapp", True),
        ("hive:devser.v4vapp", False),
        ("v4vapp", True),
        ("x", False),
        ("1abc", False),
        ("invalid!name", False),
    ],
)
def test_acc_name_is_hive(value: str, expected: bool):
    assert AccName(value).is_hive is expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("0x0123456789abcdef0123456789abcdef01234567", True),
        ("0X0123456789abcdef0123456789abcdef01234567", False),
        ("0x01234", False),
        ("devser.v4vapp", False),
    ],
)
def test_acc_name_is_evm(value: str, expected: bool):
    assert AccName(value).is_evm is expected


def test_acc_name_magi_prefix_for_hive_names():
    assert AccName("devser.v4vapp").magi_prefix == "hive:devser.v4vapp"
    assert AccName("hive:devser.v4vapp").magi_prefix == "hive:devser.v4vapp"


def test_acc_name_magi_prefix_for_evm():
    evm_address = "0x0123456789abcdef0123456789abcdef01234567"
    assert (
        AccName(evm_address).magi_prefix
        == "did:pkh:eip155:1:0x0123456789abcdef0123456789abcdef01234567"
    )


def test_acc_name_magi_prefix_raises_for_invalid_value():
    with pytest.raises(ValueError):
        AccName("invalid!name").magi_prefix


def test_acc_name_links():
    account = AccName("devser.v4vapp")
    assert account.link == "https://hivehub.dev/@devser.v4vapp"
    assert account.markdown_link == "[devser.v4vapp](https://hivehub.dev/@devser.v4vapp)"
