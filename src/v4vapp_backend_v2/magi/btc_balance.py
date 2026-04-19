from decimal import Decimal
from pprint import pprint

import httpx
from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName

BTC_BALANCE_QUERY = """query BtcBalanceByAccount($account: String!) {
  btc_mapping_balances(where: { account: { _eq: $account } }) {
    account
    balance_sats
  }
}"""

MAGI_ENDPOINTS = [
    "https://vsc.techcoderx.com/hasura/v1/graphql",
    "http://legion-witness:8080/api/v1/graphql",  # Fails,
]


class MagiBTCBalance(BaseModel):
    account: str
    balance_sats: Decimal


async def get_btc_balance_by_account(
    account: str | AccName,
    endpoint: str | None = None,
) -> MagiBTCBalance | None:
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
                pprint(result)

            response_errors = result.get("errors")
            if response_errors:
                raise RuntimeError(f"GraphQL errors: {response_errors}")

            balances = result.get("data", {}).get("btc_mapping_balances", [])
            if not balances:
                return None
            balance_record = balances[0]

            return MagiBTCBalance(
                account=balance_record.get("account", ""),
                balance_sats=Decimal(balance_record.get("balance_sats", 0)),
            )
        except (httpx.HTTPError, ValueError, RuntimeError) as exc:
            logger.warning(f"Failed to fetch BTC balance from {attempt_endpoint}: {exc}")
            endpoint_errors.append(f"{attempt_endpoint}: {exc}")
            continue

    raise RuntimeError(
        f"Failed to fetch BTC balance for account {account} from endpoints {endpoints}. "
        f"Last error: {endpoint_errors[-1] if endpoint_errors else 'unknown error'}"
    )


async def main_test():
    test_accounts = [
        "devser.v4vapp",
        "v4vapp.pool",
        "v4vapp.vsc",
        "0x3Bb63EDd3Ff0F285997C52D8ee362dd40d3B2AAd",
    ]
    tasks = [get_btc_balance_by_account(account) for account in test_accounts]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for test_account, result in zip(test_accounts, results):
        if isinstance(result, Exception):
            print(f"Error fetching BTC balance for account {test_account}: {result}")
        else:
            balance_info = result
            if balance_info:
                print(f"BTC Balance for {balance_info.account}: {balance_info.balance_sats} sats")
            else:
                print(f"No BTC balance found for account {test_account}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main_test())
