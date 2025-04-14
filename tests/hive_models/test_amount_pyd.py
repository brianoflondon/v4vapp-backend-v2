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
    str(amount_pyd) == "42.333 HIVE"
    assert amount_pyd.amount_decimal == float(nectar_amount.amount_decimal)
