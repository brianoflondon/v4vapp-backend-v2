from v4vapp_backend_v2.accounting.account_balances import list_all_accounts
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.config.decorators import async_time_decorator


@async_time_decorator
async def get_accounts_by_type_for_selector() -> dict[str, list[LedgerAccount]]:
    """Load all accounts, fallback to demo data, group and sort by account type."""
    try:
        all_accounts = await list_all_accounts()
    except Exception:
        from v4vapp_backend_v2.accounting.ledger_account_classes import (
            AssetAccount,
            ExpenseAccount,
            LiabilityAccount,
            RevenueAccount,
        )

        # Required for the account selector dropdown in balance_result.html
        # (selected account/pagination and quick switch features rely on this.)
        all_accounts = [
            AssetAccount(name="Customer Deposits Hive", sub="devser.v4vapp"),
            AssetAccount(name="Treasury Lightning", sub="from_keepsats"),
            LiabilityAccount(name="VSC Liability", sub="v4vapp-test"),
            LiabilityAccount(name="VSC Liability", sub="v4vapp.qrc"),
            RevenueAccount(name="Fee Income Keepsats", sub="from_keepsats"),
            ExpenseAccount(name="Fee Expenses Lightning", sub=""),
        ]

    accounts_by_type: dict[str, list[LedgerAccount]] = {}
    for acc in all_accounts:
        account_type = acc.account_type.value
        if account_type not in accounts_by_type:
            accounts_by_type[account_type] = []
        accounts_by_type[account_type].append(acc)

    for account_type in accounts_by_type:
        accounts_by_type[account_type].sort(key=lambda x: (x.name, x.sub))

    return accounts_by_type
