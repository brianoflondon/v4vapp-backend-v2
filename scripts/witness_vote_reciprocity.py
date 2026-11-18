"""Who a Hive account votes for as a witness, and who votes back.

Direct witness votes only. A witness proxy is reported, but proxy chains
are not expanded: the chain counts the proxy's votes, not the account's
own votes, while a proxy is set.

The outgoing list is the same ``database_api.list_witness_votes`` query
nectar's ``WitnessesVotedByAccount`` uses (``by_account_witness``).
``database_api.find_accounts`` no longer returns the ``witness_votes``
array, so that query is the source of the names. Incoming votes use
``by_witness_account``. ``find_witnesses`` (in batches of 100; a single
call of several hundred names is truncated) marks which voters are
themselves witnesses.

Usage
-----
From the repository root:

    uv run python scripts/witness_vote_reciprocity.py brianoflondon
    uv run python scripts/witness_vote_reciprocity.py brianoflondon --node https://api.hive.blog
"""

from __future__ import annotations

import argparse
import sys

from nectar.amount import Amount
from nectar.hive import Hive
from nectarapi.exceptions import NumRetriesReached

# Same public nodes as hive_extras.DEFAULT_GOOD_NODES, without the endpoints
# that file excludes. Imported here as a literal so this script does not open
# Redis or the beacon client.
DEFAULT_NODES = [
    "https://api.hive.blog",
    "https://hive-api.arcange.eu",
    "https://api.syncad.com",
    "https://techcoderx.com",
    "https://api.c0ff33a.uk",
    "https://hiveapi.actifit.io",
]

# Hive caps an account at 30 witness votes.
OUTGOING_LIMIT = 30
INCOMING_PAGE = 1000
# A find_witnesses call with a few hundred names comes back truncated.
WITNESS_BATCH = 100
DISABLED_SIGNING_KEY = "STM1111111111111111111111111111111114T1Anm"


def _rpc_votes(hive: Hive, start: list[str], limit: int, order: str) -> list[dict]:
    page = hive.rpc.list_witness_votes({"start": start, "limit": limit, "order": order})
    votes = page["votes"] if isinstance(page, dict) else page
    if not isinstance(votes, list):
        raise TypeError(f"Unexpected list_witness_votes payload: {page!r}")
    return votes


def votes_cast_by(hive: Hive, account: str) -> list[str]:
    """Witnesses ``account`` votes for, in chain order."""
    rows = _rpc_votes(hive, [account, ""], OUTGOING_LIMIT + 1, "by_account_witness")
    names = [row["witness"] for row in rows if row.get("account") == account]
    if len(names) > OUTGOING_LIMIT:
        raise RuntimeError(
            f"{account} returned more than {OUTGOING_LIMIT} witness votes; raise the page size."
        )
    return names


def accounts_voting_for(hive: Hive, witness: str) -> list[str]:
    """Accounts with a direct witness vote for ``witness``, alphabetical."""
    voters: list[str] = []
    start = [witness, ""]
    while True:
        rows = _rpc_votes(hive, start, INCOMING_PAGE, "by_witness_account")
        own = [row for row in rows if row.get("witness") == witness]
        for row in own:
            name = row.get("account")
            if isinstance(name, str) and (not voters or name != voters[-1]):
                voters.append(name)
        if len(own) < INCOMING_PAGE:
            return voters
        nxt = [witness, own[-1]["account"]]
        if nxt == start:
            return voters
        start = nxt


def find_witnesses(hive: Hive, names: list[str]) -> dict[str, dict]:
    """Witness objects keyed by owner. Missing accounts are omitted."""
    found: dict[str, dict] = {}
    for offset in range(0, len(names), WITNESS_BATCH):
        chunk = names[offset : offset + WITNESS_BATCH]
        if not chunk:
            continue
        page = hive.rpc.find_witnesses({"owners": chunk})
        witnesses = page["witnesses"] if isinstance(page, dict) else page
        if not isinstance(witnesses, list):
            raise TypeError(f"Unexpected find_witnesses payload: {page!r}")
        for witness in witnesses:
            if isinstance(witness, dict) and witness.get("owner"):
                found[witness["owner"]] = witness
    return found


def account_record(hive: Hive, name: str) -> dict | None:
    page = hive.rpc.find_accounts({"accounts": [name]})
    accounts = page["accounts"] if isinstance(page, dict) else page
    if not isinstance(accounts, list) or not accounts:
        return None
    return accounts[0]


def hive_per_vest(hive: Hive) -> float:
    props = hive.get_dynamic_global_properties(use_stored_data=False)
    fund = Amount(props["total_vesting_fund_hive"], blockchain_instance=hive)
    shares = Amount(props["total_vesting_shares"], blockchain_instance=hive)
    share_amount = float(shares)
    if share_amount == 0:
        return 0.0
    return float(fund) / share_amount


def approval_hp(witness: dict | None, hp_per_vest: float) -> int | None:
    if not witness or hp_per_vest == 0:
        return None
    raw_votes = witness.get("votes")
    if raw_votes is None:
        return None
    # witness.votes is raw vesting shares (6 decimal places).
    return int((int(raw_votes) / 1_000_000) * hp_per_vest)


def _sort_names(names: list[str], witnesses: dict[str, dict], hp_per_vest: float) -> list[str]:
    def key(name: str) -> tuple[int, str]:
        hp = approval_hp(witnesses.get(name), hp_per_vest)
        return (-(hp if hp is not None else -1), name)

    return sorted(names, key=key)


def _format_row(name: str, witness: dict | None, hp: int | None) -> str:
    hp_text = f"{hp:,.0f} HP" if hp is not None else ""
    version = ""
    disabled = ""
    if witness:
        version = str(witness.get("running_version") or "")
        if witness.get("signing_key") == DISABLED_SIGNING_KEY:
            disabled = "disabled"
    extra = "  ".join(part for part in (version, disabled) if part)
    return f"  {name:<16} {hp_text:>14}  {extra}".rstrip()


def _print_group(
    title: str,
    names: list[str],
    witnesses: dict[str, dict],
    hp_per_vest: float,
) -> None:
    print(f"{title} ({len(names)})")
    if not names:
        print("  (none)")
        return
    for name in _sort_names(names, witnesses, hp_per_vest):
        witness = witnesses.get(name)
        print(_format_row(name, witness, approval_hp(witness, hp_per_vest)))


def report(hive: Hive, account: str) -> int:
    record = account_record(hive, account)
    if record is None:
        print(f"No Hive account named {account}", file=sys.stderr)
        return 1

    outgoing = votes_cast_by(hive, account)
    voters = accounts_voting_for(hive, account)
    witnesses = find_witnesses(hive, list(dict.fromkeys([*outgoing, *voters])))
    hp_per_vest = hive_per_vest(hive)

    outgoing_set = set(outgoing)
    voter_set = set(voters)
    self_vote = account in outgoing_set
    voted_for = [name for name in outgoing if name != account]
    # Reciprocation is membership of the vote index, not find_witnesses.
    # find_witnesses only decides who among the incoming voters is a witness.
    witness_voters = [name for name in voters if name in witnesses and name != account]

    mutual = [name for name in voted_for if name in voter_set]
    no_return = [name for name in voted_for if name not in voter_set]
    not_returned = [name for name in witness_voters if name not in outgoing_set]

    proxy = record.get("proxy") or ""
    print(f"Witness vote reciprocity for {account}")
    print(f"Node: {hive.rpc.url}")
    if proxy:
        print(
            f"Witness proxy is set to {proxy}. "
            "The chain follows that proxy, not the direct votes below."
        )
    else:
        print("No witness proxy. These are the direct witness votes.")
    print(f"Votes cast: {len(outgoing)}" + (" (includes a self vote)" if self_vote else ""))
    print(f"Accounts voting for {account}: {len(voters)} ({len(witness_voters)} other witnesses)")
    print("HP is each witness's total approval, not that account's own stake.")
    print()
    _print_group("Reciprocated", mutual, witnesses, hp_per_vest)
    print()
    _print_group("Votes for them, they do not vote back", no_return, witnesses, hp_per_vest)
    print()
    _print_group("They vote for you, you do not vote back", not_returned, witnesses, hp_per_vest)
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("account", help="Hive account, for example brianoflondon")
    parser.add_argument(
        "--node",
        action="append",
        default=None,
        help="Hive RPC node. Repeat to set a failover list. Defaults to the public nodes.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    account = args.account.strip().lower()
    if not account:
        print("Account name is empty", file=sys.stderr)
        return 1
    hive = Hive(
        node=args.node or DEFAULT_NODES,
        timeout=30,
        num_retries=5,
        num_retries_call=2,
    )
    try:
        return report(hive, account)
    except NumRetriesReached as exc:
        print(f"Hive RPC failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
