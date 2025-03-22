from beem.amount import Amount  # type: ignore

from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


def test_amount_pyd():
    data = {"amount": "22000", "precision": 3, "nai": "@@000000021"}
    amount_pyd = AmountPyd.model_validate(data)
    assert amount_pyd.beam == Amount("22000 HIVE")
    assert isinstance(amount_pyd.beam, Amount)
    assert isinstance(str(amount_pyd), str)
    assert len(amount_pyd.fixed_width_str(14)) == 14 + 5
