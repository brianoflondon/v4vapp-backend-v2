from decimal import Decimal
from time import perf_counter

import httpx

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.magi.magi_classes import ICON, MagiBTCBalance

BTC_BALANCE_QUERY = """query BtcBalanceByAccount($account: String!) {
  btc_mapping_balances(where: { account: { _eq: $account } }) {
    account
    balance_sats
  }
}"""

MAGI_ENDPOINTS = [
    "http://legion-witness:8081/v1/graphql",
    "https://magi-api.v4v.app/hasura/v1/graphql",
    "https://vsc.techcoderx.com/hasura/v1/graphql",
    "https://api.okinoko.io/hasura/v1/graphql",
]


async def get_magi_btc_balance_by_account(
    account: str | AccName,
    endpoint: str | None = None,
) -> MagiBTCBalance:
    """Fetch BTC balance data for a Hive account from the Hasura GraphQL endpoint."""
    account_str = AccName(account).magi_prefix

    payload = {
        "query": BTC_BALANCE_QUERY,
        "variables": {"account": account_str},
    }

    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
    }

    if endpoint is None:
        endpoints = MAGI_ENDPOINTS
    elif endpoint in MAGI_ENDPOINTS:
        endpoints = [endpoint] + [e for e in MAGI_ENDPOINTS if e != endpoint]
    else:
        endpoints = [endpoint]

    timeout = httpx.Timeout(10.0, connect=5.0)
    endpoint_errors: list[str] = []

    for attempt_endpoint in endpoints:
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(attempt_endpoint, json=payload, headers=headers)
                response.raise_for_status()
                result = response.json()

            response_errors = result.get("errors")
            if response_errors:
                raise RuntimeError(f"GraphQL errors: {response_errors}")

            balances = result.get("data", {}).get("btc_mapping_balances", [])
            if not balances:
                logger.info(f"{ICON} No MAGI BTC balance {account_str} at {attempt_endpoint}")
                return MagiBTCBalance(account=account_str, balance_sats=Decimal(0))
            balance_record = balances[0]

            magi_balance = MagiBTCBalance(
                account=balance_record.get("account", ""),
                balance_sats=Decimal(balance_record.get("balance_sats", 0)),
            )
            logger.info(
                f"{ICON} MAGI BTC balance {account_str} {magi_balance.balance_sats:,.0f} from {attempt_endpoint}"
            )
            return magi_balance
        except (httpx.HTTPError, ValueError, RuntimeError) as exc:
            logger.warning(
                f"{ICON} Failed to fetch MAGI BTC balance from {attempt_endpoint}: {exc}"
            )
            endpoint_errors.append(f"{attempt_endpoint}: {exc}")
            continue

    error_message = (
        "; ".join(endpoint_errors) if endpoint_errors else "Unknown MAGI BTC balance error"
    )
    return MagiBTCBalance(account=account_str, balance_sats=Decimal(0), error=error_message)


async def main_test():
    test_accounts = [
        "devser.v4vapp",
        "v4vapp.pool",
        "v4vapp.vsc",
        "0x3Bb63EDd3Ff0F285997C52D8ee362dd40d3B2AAd",
    ]

    endpoint_stats: dict[str, dict[str, list[float] | int]] = {
        endpoint: {"durations": [], "errors": 0} for endpoint in MAGI_ENDPOINTS
    }

    for account in test_accounts:
        print(f"\nQuerying account {account} across {len(MAGI_ENDPOINTS)} endpoints")

        account_results: dict[str, MagiBTCBalance | None | Exception] = {}

        for endpoint in MAGI_ENDPOINTS:
            start = perf_counter()
            try:
                result = await get_magi_btc_balance_by_account(account, endpoint=endpoint)
                elapsed = perf_counter() - start
                endpoint_stats[endpoint]["durations"].append(elapsed)
                account_results[endpoint] = result
                print(f"{endpoint}: {elapsed:.3f}s -> {result}")
            except Exception as exc:
                elapsed = perf_counter() - start
                endpoint_stats[endpoint]["errors"] += 1
                account_results[endpoint] = exc
                print(f"{endpoint}: {elapsed:.3f}s -> ERROR: {exc}")

        successful_results = {
            endpoint: value
            for endpoint, value in account_results.items()
            if not isinstance(value, Exception)
        }

        if len(successful_results) < len(MAGI_ENDPOINTS):
            print("One or more endpoints failed for this account; skipping comparison.")
            continue

        unique_results = {
            repr(result): endpoint for endpoint, result in successful_results.items()
        }
        if len(unique_results) == 1:
            print("All endpoints returned the same answer.")
        else:
            print("Mismatch detected between endpoints:")
            for endpoint, result in successful_results.items():
                print(f"  {endpoint}: {result}")

    print("\nEndpoint performance summary:")
    for endpoint, stats in endpoint_stats.items():
        durations = stats["durations"]
        errors = stats["errors"]
        avg = sum(durations) / len(durations) if durations else 0.0
        print(
            f"{endpoint}: average={avg:.3f}s over {len(durations)} successful runs, "
            f"errors={errors}"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main_test())
