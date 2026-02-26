from datetime import datetime, timezone
from decimal import Decimal

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.models.lnd_balance_models import fetch_balances


async def reset_lightning_opening_balance():
    """
    Reset the Lightning node's opening balance for testing purposes.

    This function retrieves the current Lightning node channel balance and creates a ledger entry
    to record the opening balance. It performs the following operations:

    1. Fetches the current channel balance from the default LND node configuration
    2. If channel balance exists:
        - Logs the current channel balance in sats
        - Updates the crypto conversion quote
        - Creates a CryptoConversion from msat to other currencies using the latest quote
        - Creates and saves a LedgerEntry that records the opening balance as a funding operation
        - Performs a diagnostic check to verify the ledger entry was saved and is queryable
    3. If no channel balance is available, logs a warning

    The ledger entry uses:
    - Asset Account: "External Lightning Payments" (for debit)
    - Liability Account: "Owner Loan Payable" (for credit)
    - Both sides record the full local msat balance with appropriate conversions

    Raises:
         Any exceptions from database operations or balance fetching are logged but not re-raised.

    Note:
         Diagnostic checks log warnings or debug messages if the ledger entry retrieval fails.
         This function is intended for testing purposes only.
    """
    node = InternalConfig().config.lnd_config.default
    balances = await fetch_balances()
    if balances.channel is None:
        logger.warning(
            "No channel balance found for the default LND node. Cannot reset opening balance."
        )
        return

    check_account = AssetAccount(name="External Lightning Payments", sub=node)
    # Bypass cache — opening balance checks must reflect current DB state
    account_ledger_balance = await one_account_balance(check_account, use_cache=False)
    if account_ledger_balance.msats == balances.channel.local_msat:
        logger.info(
            f"Ledger balance for {check_account.name} (Sub: {check_account.sub}) is {account_ledger_balance.sats:,.0f} sats, "
            f"which does matches the channel local balance of {balances.channel.local_sats:,.0f} sats. "
            "No action needed."
        )
        return

    if account_ledger_balance.has_transactions:
        logger.warning(
            f"Ledger balance for {check_account.name} (Sub: {check_account.sub}) is {account_ledger_balance.sats:,.0f} sats, "
            f"which does not match the channel local balance of {balances.channel.local_sats:,.0f} sats. "
            "However, there are existing transactions in the ledger for this account. "
            "Resetting the opening balance may lead to discrepancies. Please review the ledger entries before proceeding."
        )
        reason = f"Balance adjustment required for {node}"
        adjustment_msats = balances.channel.local_msat - account_ledger_balance.msats
        adjustment_msats = adjustment_msats.quantize(
            Decimal("1")
        )  # round to nearest 1 msat to avoid tiny adjustments
        short_id = "adjustment"
    else:
        reason = f"Initial opening balance for {node}"
        adjustment_msats = balances.channel.local_msat
        adjustment_msats = adjustment_msats.quantize(
            Decimal("1")
        )  # round to nearest 1 msat to avoid tiny adjustments
        short_id = "open"

    logger.info(f"Current Channel balance: {balances.channel.local_sats:,.0f} sats")
    await TrackedBaseModel.update_quote()
    quote = TrackedBaseModel.last_quote

    opening_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=adjustment_msats,
        quote=quote,
    ).conversion

    opening_balance = LedgerEntry(
        cust_id="",
        short_id=short_id,
        op_type="funding",
        ledger_type=LedgerType.FUNDING,
        group_id=f"{short_id}-{datetime.now(tz=timezone.utc).isoformat()}-{LedgerType.FUNDING.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=reason,
        debit=AssetAccount(name="External Lightning Payments", sub=node),
        debit_unit=Currency.MSATS,
        debit_amount=adjustment_msats,
        debit_conv=opening_conv,
        credit=LiabilityAccount(name="Owner Loan Payable", sub=node),
        credit_unit=Currency.MSATS,
        credit_amount=adjustment_msats,
        credit_conv=opening_conv,
    )
    await opening_balance.save()
    # Diagnostic check: confirm the opening balance was saved and is visible for the node
    try:
        found = (
            await LedgerEntry.collection()
            .find(
                {
                    "$or": [
                        {"debit.name": "External Lightning Payments", "debit.sub": node},
                        {"credit.name": "External Lightning Payments", "credit.sub": node},
                    ]
                }
            )
            .to_list()
        )
        logger.info(
            f"Diagnostic: Found {len(found)} ledger entries for External Lightning Payments - Sub: {node}",
            extra={"notification": False},
        )
        # Log timestamp and description for quick inspection
        for doc in found:
            ts = doc.get("timestamp")
            desc = doc.get("description")
            logger.debug(f"Entry: {ts} - {desc}", extra={"notification": False})
    except Exception as e:
        logger.warning(f"Diagnostic check failed: {e}", extra={"notification": False})


async def reset_exchange_opening_balance(
    exchange_sub: str = "binance_testnet",
) -> None:
    """
    Reset the exchange (e.g. Binance) opening balance for the Exchange Holdings account.

    This function takes a balances dict (as returned by ``get_balances``) containing
    at least ``"SATS"`` and ``"HIVE"`` keys, and creates ledger entries to record
    the opening (or adjustment) balance for each asset that has a non-zero balance.

    For each asset with a balance:
        - Converts the balance into a ``CryptoConversion`` using the latest quote
        - Creates a ``LedgerEntry`` that debits *Exchange Holdings* (Asset) and
          credits *Owner Loan Payable* (Liability), both with the exchange sub-account
        - If the ledger already has the correct balance, no entry is created
        - If the ledger has existing transactions but a mismatched balance, an adjustment
          entry is created for the difference

    Args:
        balances: Dict with string keys (asset symbols) and Decimal values.
            Expected keys: ``"SATS"`` (BTC value in satoshis), ``"HIVE"``.
            Other keys are ignored.
        exchange_sub: The sub-account name for the exchange, e.g. ``"binance_testnet"``.

    Raises:
        Any exceptions from database operations or balance fetching are logged but
        not re-raised.

    Note:
        Diagnostic checks log warnings or debug messages if the ledger entry
        retrieval fails.  This function is intended for operational/testing purposes.
    """
    try:
        binance_adaptor = get_exchange_adapter()
    except Exception as e:
        logger.error(f"Failed to initialize exchange adapter: {e}", extra={"error": str(e)})
        return

    exchange_sub = binance_adaptor.exchange_name

    btc_balance = binance_adaptor.get_balance("BTC")
    sats_balance = btc_balance * Decimal(1e8)  # Convert BTC to satoshis
    hive_balance = binance_adaptor.get_balance("HIVE")

    if sats_balance == 0 and hive_balance == 0:
        logger.warning(
            f"No SATS or HIVE balance found in Binance balances for {exchange_sub}. "
            "Cannot reset opening balance."
        )
        return

    await TrackedBaseModel.update_quote()
    quote = TrackedBaseModel.last_quote

    check_account = AssetAccount(name="Exchange Holdings", sub=exchange_sub)

    # Process each asset: SATS → MSATS ledger entry, HIVE → HIVE ledger entry
    asset_entries: list[tuple[str, Currency, Decimal]] = []
    if sats_balance > 0:
        # Convert sats to msats for ledger consistency (1 sat = 1000 msats)
        msats_balance = int(sats_balance) * 1000
        asset_entries.append(("sats", Currency.MSATS, Decimal(msats_balance)))
    if hive_balance > 0:
        asset_entries.append(("hive", Currency.HIVE, hive_balance))

    for asset_label, currency, balance_value in asset_entries:
        # Bypass cache — opening balance checks must reflect current DB state
        account_ledger_balance = await one_account_balance(check_account, use_cache=False)

        # Determine existing balance in the relevant unit
        if currency == Currency.MSATS:
            existing_balance = account_ledger_balance.msats
        else:
            existing_balance = account_ledger_balance.hive

        # check if match within 0.1% to avoid creating adjustment entries for tiny differences due to conversion or timing
        if (
            existing_balance != Decimal(0)
            and abs(existing_balance - balance_value) / balance_value < 0.001
        ):
            logger.info(
                f"Ledger balance for {check_account.name} (Sub: {check_account.sub}) "
                f"{asset_label} is close enough to the exchange balance ({existing_balance:,.0f} vs {balance_value:,.0f} {currency.value}), "
                "no adjustment needed."
            )
            continue

        if account_ledger_balance.has_transactions:
            logger.warning(
                f"Ledger balance for {check_account.name} (Sub: {check_account.sub}) "
                f"{asset_label} is {existing_balance:,.0f} {currency.value}, "
                f"which does not match the exchange balance of {balance_value:,.0f} {currency.value}. "
                "Existing transactions found — creating adjustment entry."
            )
            reason = f"Balance adjustment for {exchange_sub} ({asset_label})"
            adjustment_value = balance_value - existing_balance
            if currency == Currency.MSATS:
                # round to nearest 1000 msats using Decimal quantize to avoid floating point issues, then convert back to Decimal
                adjustment_value = (adjustment_value / Decimal(1000)).quantize(
                    Decimal("1")
                ) * Decimal(1000)
            elif currency == Currency.HIVE:
                # round to 3 decimal places for Hive
                adjustment_value = adjustment_value.quantize(Decimal("0.001"))
            short_id = "adjustment"
        else:
            reason = f"Initial opening balance for {exchange_sub} ({asset_label})"
            adjustment_value = balance_value
            short_id = "open"

        logger.info(
            f"Exchange {exchange_sub} {asset_label} balance: {balance_value:,.0f} {currency.value}, "
            f"adjustment: {adjustment_value:,.0f} {currency.value}"
        )

        opening_conv = CryptoConversion(
            conv_from=currency,
            value=adjustment_value,
            quote=quote,
        ).conversion

        opening_balance = LedgerEntry(
            cust_id="",
            short_id=short_id,
            op_type="funding",
            ledger_type=LedgerType.FUNDING,
            group_id=f"{short_id}-{exchange_sub}-{asset_label}-{datetime.now(tz=timezone.utc).isoformat()}-{LedgerType.FUNDING.value}",
            timestamp=datetime.now(tz=timezone.utc),
            description=reason,
            debit=AssetAccount(name="Exchange Holdings", sub=exchange_sub),
            debit_unit=currency,
            debit_amount=adjustment_value,
            debit_conv=opening_conv,
            credit=LiabilityAccount(name="Owner Loan Payable", sub=exchange_sub),
            credit_unit=currency,
            credit_amount=adjustment_value,
            credit_conv=opening_conv,
        )
        await opening_balance.save()

        # Diagnostic check
        try:
            found = (
                await LedgerEntry.collection()
                .find(
                    {
                        "$or": [
                            {"debit.name": "Exchange Holdings", "debit.sub": exchange_sub},
                            {"credit.name": "Exchange Holdings", "credit.sub": exchange_sub},
                        ]
                    }
                )
                .to_list()
            )
            logger.info(
                f"Diagnostic: Found {len(found)} ledger entries for "
                f"Exchange Holdings - Sub: {exchange_sub}",
                extra={"notification": False},
            )
            for doc in found:
                ts = doc.get("timestamp")
                desc = doc.get("description")
                logger.debug(f"Entry: {ts} - {desc}", extra={"notification": False})
        except Exception as e:
            logger.warning(f"Diagnostic check failed: {e}", extra={"notification": False})
