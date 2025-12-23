import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tests.utils import clear_database
from v4vapp_backend_v2.accounting.account_balances import account_balance_printout
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.currency_class import Currency


@pytest.mark.asyncio
async def test_contra_and_noncontra_rows_show_up():
    """Ensure both contra=True and contra=False rows appear in the account printout."""
    await clear_database()

    node = "umbrel_test_contra"
    ts = datetime.now(tz=timezone.utc)

    opening_msats = Decimal("1609456828000")
    conv_open = CryptoConv(
        msats=opening_msats,
        sats=opening_msats / Decimal(1000),
        hive=Decimal("15002.047"),
        hbd=Decimal("1415.443"),
        usd=Decimal("1415.443"),
        conv_from=Currency.MSATS,
        value=opening_msats,
    )

    opening = LedgerEntry(
        cust_id="opening_balance",
        short_id="opening_balance",
        op_type="funding",
        ledger_type=LedgerType.FUNDING,
        group_id="test_opening",
        timestamp=ts,
        description=f"Resetting opening balance for {node} for testing",
        debit=AssetAccount(name="External Lightning Payments", sub=node),
        debit_unit=Currency.MSATS,
        debit_amount=opening_msats,
        debit_conv=conv_open,
        credit=LiabilityAccount(name="Owner Loan Payable (funding)", sub=node),
        credit_unit=Currency.MSATS,
        credit_amount=opening_msats,
        credit_conv=conv_open,
    )
    await opening.save()

    # small sleep to ensure timestamp ordering
    await asyncio.sleep(0.01)

    withdraw_msats = Decimal("10000000")  # 10,000 sats
    conv_withdraw = CryptoConv(
        msats=withdraw_msats,
        sats=withdraw_msats / Decimal(1000),
        hive=Decimal("93.000"),
        hbd=Decimal("8.7"),
        usd=Decimal("8.7"),
        conv_from=Currency.MSATS,
        value=withdraw_msats,
    )

    withdraw = LedgerEntry(
        cust_id="withdraw",
        short_id="withdraw_1",
        op_type="withdraw",
        ledger_type=LedgerType.WITHDRAW_LIGHTNING,
        group_id="test_withdraw",
        timestamp=datetime.now(tz=timezone.utc),
        description="Send 10000 sats to Node kappa (fee: 0)",
        debit=LiabilityAccount(name="Owner Loan Payable (funding)", sub=node),
        debit_unit=Currency.MSATS,
        debit_amount=withdraw_msats,
        debit_conv=conv_withdraw,
        credit=AssetAccount(name="External Lightning Payments", sub=node, contra=True),
        credit_unit=Currency.MSATS,
        credit_amount=withdraw_msats,
        credit_conv=conv_withdraw,
    )
    await withdraw.save()

    # Now fetch the printout and assert both descriptions appear
    printout, details = await account_balance_printout(
        account=AssetAccount(name="External Lightning Payments", sub=node),
        line_items=True,
        user_memos=False,
        as_of_date=datetime.now(tz=timezone.utc),
    )

    assert "Resetting opening balance for" in printout
    assert "Send 10000 sats to Node kappa" in printout
