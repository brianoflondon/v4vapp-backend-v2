from decimal import Decimal
from nectar.amount import Amount

from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


def test_amount_pyd():
    data = {"amount": "22000", "precision": 3, "nai": "@@000000021"}
    amount_pyd = AmountPyd.model_validate(data)
    assert amount_pyd.beam == Amount("22.000 HIVE")
    assert isinstance(amount_pyd.beam, Amount)
    assert isinstance(str(amount_pyd), str)
    assert len(amount_pyd.fixed_width_str(21)) == 21


def test_amount_hive():
    nectar_amount = Amount("42.333 HIVE")
    amount_pyd = AmountPyd.model_validate(nectar_amount)
    assert str(amount_pyd) == "42.333 HIVE"
    assert amount_pyd.amount_decimal == Decimal(nectar_amount.amount_decimal)


def test_amount_to_pyd():
    nectar_amount = Amount("42.333 HIVE")
    amount_pyd = AmountPyd.model_validate(nectar_amount)
    assert amount_pyd.beam == nectar_amount
    assert isinstance(amount_pyd.beam, Amount)
    assert isinstance(str(amount_pyd), str)
    assert len(amount_pyd.fixed_width_str(21)) == 21
    assert amount_pyd.amount_decimal == Decimal(nectar_amount.amount_decimal)


def test_amount_pyd_minus_minimum():
    nectar_hive = Amount("42.333 HIVE")
    amount_pyd = AmountPyd.model_validate(nectar_hive)
    assert float(amount_pyd.minus_minimum.amount_decimal) == 42.332

    nectar_hbd = Amount("42.333 HBD")
    amount_pyd = AmountPyd.model_validate(nectar_hbd)
    assert float(amount_pyd.minus_minimum.amount_decimal) == 42.332


def test_amount_pyd_upper_lower():
    nectar_hive = Amount("42.333 HIVE")
    amount_pyd = AmountPyd.model_validate(nectar_hive)
    assert amount_pyd.symbol_lower == "hive"
    assert amount_pyd.symbol_upper == "HIVE"

    nectar_hbd = Amount("42.333 HBD")
    amount_pyd = AmountPyd.model_validate(nectar_hbd)
    assert amount_pyd.symbol_lower == "hbd"
    assert amount_pyd.symbol_upper == "HBD"
