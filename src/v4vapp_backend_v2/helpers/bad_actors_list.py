import json
import logging
from os.path import exists
from typing import List

import httpx


async def get_bad_hive_accounts() -> List[str]:
    bad_actors = await fetch_bad_actor_list()
    bad_accounts: List[str] = []
    try:
        if exists("data/bad_hive_accounts.json"):
            with open("data/bad_hive_accounts.json", "r") as f:
                bad_accounts = json.load(f)
    except Exception as e:
        logging.warning(f"Error loading bad accounts: {e}")
        bad_accounts = []
    combined = set(bad_actors + bad_accounts)
    return list(combined)


async def fetch_bad_actor_list() -> List[str]:
    url = "https://gitlab.syncad.com/hive/wallet/-/raw/master/src/app/utils/BadActorList.js?ref_type=heads"
    try:
        # Fetch the content from the URL
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()  # Raise an exception for bad status codes

        # Extract the content between the backticks
        content = response.text
        start = content.find("`") + 1
        end = content.rfind("`")
        if start == 0 or end == -1:
            raise ValueError("Could not find list boundaries in the file")

        # Get the list portion and process it
        list_content = content[start:end].strip()
        # Split into lines and filter out empty lines
        bad_actor_list = [line.strip() for line in list_content.split("\n") if line.strip()]

        return bad_actor_list

    except httpx.HTTPError as e:
        print(f"Error fetching the list: {e}")
        return []
    except ValueError as e:
        print(f"Error parsing the list: {e}")
        return []


# Example usage:
# bad_actors = fetch_bad_actor_list()
# print(bad_actors)

if __name__ == "__main__":
    bad_actors = get_bad_hive_accounts()
