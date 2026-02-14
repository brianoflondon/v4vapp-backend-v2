#!/usr/bin/env python3
"""
Interactive script to test the BinanceSwapAdapter (Convert API).

This script allows you to interactively swap HIVE <-> BTC using the
Binance Convert API with LIVE mainnet credentials.

⚠️  WARNING: This uses REAL money. Start with small amounts (10 HIVE).
⚠️  The Convert API does NOT support testnet.

Usage:
    python scripts/test_binance_swap.py [--config CONFIG]

    Default config: devhive.config.yaml

The script will:
1. Initialize InternalConfig with your config file
2. Show your current balances (HIVE, BTC/SATS)
3. Let you interactively:
   a. Check convert pair minimums
   b. Request a quote (without executing)
   c. Sell HIVE for BTC (default: 10 HIVE)
   d. Buy HIVE with BTC (default: 10 HIVE worth)
   e. Check balances again
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal

# Ensure the src directory is importable
sys.path.insert(0, "src")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive Binance Convert (swap) tester")
    parser.add_argument(
        "-c",
        "--config",
        default="devhive.config.yaml",
        help="Config filename (default: devhive.config.yaml)",
    )
    return parser.parse_args()


def print_separator():
    print("\n" + "=" * 60)


def print_balances(adapter):
    """Fetch and display current balances."""
    print_separator()
    print("📊 Current Balances")
    print("-" * 40)
    balances = adapter.get_balances(["BTC", "HIVE", "HBD"])
    for asset, balance in sorted(balances.items()):
        if asset == "SATS":
            print(f"  {asset:>6}: {int(balance):>15,}")
        elif asset == "BTC":
            print(f"  {asset:>6}: {balance:.8f}")
        else:
            print(f"  {asset:>6}: {balance:.3f}")
    print_separator()
    return balances


def show_minimums(adapter):
    """Show Convert pair minimums for HIVE/BTC."""
    print_separator()
    print("📏 Convert Pair Minimums (HIVE <-> BTC)")
    print("-" * 40)
    try:
        mins = adapter.get_min_order_requirements("HIVE", "BTC")
        print(f"  Min quantity (HIVE): {mins.min_qty}")
        print(f"  Min notional (BTC):  {mins.min_notional}")
        if mins.step_size > 0:
            print(f"  Step size:           {mins.step_size}")
    except Exception as e:
        print(f"  ❌ Error: {e}")
    print_separator()


def request_quote_only(adapter):
    """Request a quote without executing it (for inspection)."""
    print_separator()
    print("💬 Request Quote (inspect only, will NOT execute)")
    print("-" * 40)

    direction = input("  Direction? [1] Sell HIVE for BTC  [2] Buy HIVE with BTC: ").strip()
    if direction not in ("1", "2"):
        print("  ❌ Invalid choice")
        return

    amount_str = input("  Amount of HIVE (default 10): ").strip()
    amount = Decimal(amount_str) if amount_str else Decimal("10")

    try:
        if direction == "1":
            # Sell HIVE → BTC
            quote = adapter.send_quote_request(
                from_asset="HIVE",
                to_asset="BTC",
                from_amount=amount,
                valid_time="10s",
            )
            print(f"\n  Quote ID:       {quote.quote_id}")
            print(f"  From:           {quote.from_amount} HIVE")
            print(f"  To:             {quote.to_amount:.8f} BTC")
            sats = int(quote.to_amount * Decimal("100000000"))
            print(f"                  ({sats:,} sats)")
            print(f"  Ratio:          {quote.ratio}")
            print(f"  Inverse Ratio:  {quote.inverse_ratio}")
            print(f"  Expires in:     {quote.expires_in_seconds:.1f}s")
        else:
            # Buy HIVE ← BTC
            quote = adapter.send_quote_request(
                from_asset="BTC",
                to_asset="HIVE",
                to_amount=amount,
                valid_time="10s",
            )
            print(f"\n  Quote ID:       {quote.quote_id}")
            print(f"  From:           {quote.from_amount:.8f} BTC")
            sats = int(quote.from_amount * Decimal("100000000"))
            print(f"                  ({sats:,} sats)")
            print(f"  To:             {quote.to_amount} HIVE")
            print(f"  Ratio:          {quote.ratio}")
            print(f"  Inverse Ratio:  {quote.inverse_ratio}")
            print(f"  Expires in:     {quote.expires_in_seconds:.1f}s")

        print("\n  ℹ️  Quote NOT accepted (will expire)")
    except Exception as e:
        print(f"  ❌ Error: {e}")

    print_separator()


def sell_hive(adapter):
    """Sell HIVE for BTC using Convert API."""
    print_separator()
    print("🔴 SELL HIVE for BTC (LIVE TRADE)")
    print("-" * 40)

    amount_str = input("  Amount of HIVE to sell (default 10): ").strip()
    amount = Decimal(amount_str) if amount_str else Decimal("10")

    # Show a preview quote first
    print(f"\n  Requesting quote to sell {amount} HIVE...")
    try:
        quote = adapter.send_quote_request(
            from_asset="HIVE",
            to_asset="BTC",
            from_amount=amount,
            valid_time="30s",
        )
        sats = int(quote.to_amount * Decimal("100000000"))
        print(f"  Quote: {quote.from_amount} HIVE → {quote.to_amount:.8f} BTC ({sats:,} sats)")
        print(f"  Rate:  {quote.ratio}")
        print(f"  Expires in: {quote.expires_in_seconds:.1f}s")
    except Exception as e:
        print(f"  ❌ Error getting quote: {e}")
        return

    confirm = (
        input(f"\n  ⚠️  CONFIRM sell {amount} HIVE for ~{sats:,} sats? [y/N]: ").strip().lower()
    )
    if confirm != "y":
        print("  Cancelled (quote will expire)")
        return

    print("\n  Accepting quote and executing swap...")
    try:
        # Accept the quote we already got
        accept = adapter.accept_quote(quote.quote_id)
        print(f"  Order ID: {accept.order_id}")
        print(f"  Initial status: {accept.order_status}")

        # Wait for completion
        status = adapter._wait_for_order_completion(accept.order_id)
        print("\n  ✅ Swap completed!")
        print(f"     From: {status.from_amount} {status.from_asset}")
        print(f"     To:   {status.to_amount} {status.to_asset}")
        sats_received = int(status.to_amount * Decimal("100000000"))
        print(f"           ({sats_received:,} sats)")
        print(f"     Rate: {status.ratio}")
        print(f"     Status: {status.order_status}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

    print_separator()


def buy_hive(adapter):
    """Buy HIVE with BTC using Convert API."""
    print_separator()
    print("🟢 BUY HIVE with BTC (LIVE TRADE)")
    print("-" * 40)

    amount_str = input("  Amount of HIVE to buy (default 10): ").strip()
    amount = Decimal(amount_str) if amount_str else Decimal("10")

    # Show a preview quote first
    print(f"\n  Requesting quote to buy {amount} HIVE...")
    try:
        quote = adapter.send_quote_request(
            from_asset="BTC",
            to_asset="HIVE",
            to_amount=amount,
            valid_time="30s",
        )
        sats = int(quote.from_amount * Decimal("100000000"))
        print(f"  Quote: {quote.from_amount:.8f} BTC ({sats:,} sats) → {quote.to_amount} HIVE")
        print(f"  Rate:  {quote.ratio}")
        print(f"  Expires in: {quote.expires_in_seconds:.1f}s")
    except Exception as e:
        print(f"  ❌ Error getting quote: {e}")
        return

    confirm = (
        input(f"\n  ⚠️  CONFIRM buy {amount} HIVE for ~{sats:,} sats? [y/N]: ").strip().lower()
    )
    if confirm != "y":
        print("  Cancelled (quote will expire)")
        return

    print("\n  Accepting quote and executing swap...")
    try:
        accept = adapter.accept_quote(quote.quote_id)
        print(f"  Order ID: {accept.order_id}")
        print(f"  Initial status: {accept.order_status}")

        # Wait for completion
        status = adapter._wait_for_order_completion(accept.order_id)
        print("\n  ✅ Swap completed!")
        print(f"     From: {status.from_amount:.8f} {status.from_asset}")
        sats_spent = int(status.from_amount * Decimal("100000000"))
        print(f"           ({sats_spent:,} sats)")
        print(f"     To:   {status.to_amount} {status.to_asset}")
        print(f"     Rate: {status.ratio}")
        print(f"     Status: {status.order_status}")
    except Exception as e:
        print(f"  ❌ Error: {e}")

    print_separator()


def full_round_trip(adapter):
    """Sell 10 HIVE then buy 10 HIVE back, showing the spread cost."""
    print_separator()
    print("🔄 ROUND TRIP: Sell 10 HIVE → BTC → Buy 10 HIVE (LIVE)")
    print("-" * 40)

    confirm = input("  ⚠️  This will execute TWO live trades. Continue? [y/N]: ").strip().lower()
    if confirm != "y":
        print("  Cancelled")
        return

    amount = Decimal("10")
    balances_before = adapter.get_balances(["BTC", "HIVE"])
    print(
        f"\n  Before: {balances_before.get('HIVE', 0):.3f} HIVE, {balances_before.get('SATS', 0):,} sats"
    )

    # Step 1: Sell HIVE
    print(f"\n  Step 1: Selling {amount} HIVE for BTC...")
    try:
        sell_result = adapter.market_sell("HIVE", "BTC", amount)
        sell_sats = int(sell_result.quote_qty * Decimal("100000000"))
        print(
            f"    ✅ Sold {sell_result.executed_qty} HIVE for {sell_result.quote_qty:.8f} BTC ({sell_sats:,} sats)"
        )
        print(f"    Rate: {sell_result.avg_price:.8f} BTC/HIVE")
    except Exception as e:
        print(f"    ❌ Sell failed: {e}")
        return

    # Step 2: Buy HIVE back
    print(f"\n  Step 2: Buying {amount} HIVE with BTC...")
    try:
        buy_result = adapter.market_buy("HIVE", "BTC", amount)
        buy_sats = int(buy_result.quote_qty * Decimal("100000000"))
        print(
            f"    ✅ Bought {buy_result.executed_qty} HIVE for {buy_result.quote_qty:.8f} BTC ({buy_sats:,} sats)"
        )
        print(f"    Rate: {buy_result.avg_price:.8f} BTC/HIVE")
    except Exception as e:
        print(f"    ❌ Buy failed: {e}")
        return

    # Summary
    balances_after = adapter.get_balances(["BTC", "HIVE"])
    print(
        f"\n  After:  {balances_after.get('HIVE', 0):.3f} HIVE, {balances_after.get('SATS', 0):,} sats"
    )

    hive_diff = balances_after.get("HIVE", Decimal(0)) - balances_before.get("HIVE", Decimal(0))
    sats_diff = int(balances_after.get("SATS", 0)) - int(balances_before.get("SATS", 0))
    print("\n  📊 Round-trip cost:")
    print(f"     HIVE change: {hive_diff:+.3f}")
    print(f"     Sats change: {sats_diff:+,}")
    spread_cost_sats = sell_sats - buy_sats
    print(
        f"     Spread cost: {abs(spread_cost_sats):,} sats (sell got {sell_sats:,}, buy cost {buy_sats:,})"
    )

    print_separator()


def main():
    args = parse_args()

    print("=" * 60)
    print("  Binance Convert (Swap) API — Interactive Tester")
    print("  ⚠️  WARNING: This uses LIVE mainnet credentials!")
    print("=" * 60)

    # Initialize config
    print(f"\n  Loading config: {args.config}")
    try:
        from v4vapp_backend_v2.config.setup import InternalConfig

        InternalConfig(config_filename=args.config)
        print("  ✅ Config loaded successfully")
    except Exception as e:
        print(f"  ❌ Failed to load config: {e}")
        sys.exit(1)

    # Create the swap adapter
    from v4vapp_backend_v2.conversion.binance_swap_adapter import BinanceSwapAdapter

    adapter = BinanceSwapAdapter(testnet=False)

    # Show initial balances
    try:
        print_balances(adapter)
    except Exception as e:
        print(f"  ❌ Failed to get balances: {e}")
        print("  Check that your API key has permission and IP is whitelisted.")
        sys.exit(1)

    # Interactive menu
    while True:
        print("\n  Menu:")
        print("  [1] Show balances")
        print("  [2] Show convert pair minimums (HIVE/BTC)")
        print("  [3] Request quote only (no trade)")
        print("  [4] Sell HIVE for BTC")
        print("  [5] Buy HIVE with BTC")
        print("  [6] Round trip: sell 10 HIVE & buy 10 HIVE back")
        print("  [q] Quit")

        choice = input("\n  Choice: ").strip().lower()

        if choice == "1":
            print_balances(adapter)
        elif choice == "2":
            show_minimums(adapter)
        elif choice == "3":
            request_quote_only(adapter)
        elif choice == "4":
            sell_hive(adapter)
        elif choice == "5":
            buy_hive(adapter)
        elif choice == "6":
            full_round_trip(adapter)
        elif choice in ("q", "quit", "exit"):
            print("\n  Goodbye! 👋")
            break
        else:
            print("  ❌ Invalid choice, try again")


if __name__ == "__main__":
    main()
