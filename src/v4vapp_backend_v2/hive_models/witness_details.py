from datetime import datetime

from pydantic import BaseModel

from v4vapp_backend_v2.hive_models.account_name_type import AccNameType


class Witness(BaseModel):
    """
    Represents the details of a witness in the Hive blockchain.

    Attributes:
        witness_name (str): The name of the witness.
        rank (int): The rank of the witness.
        url (str): The URL associated with the witness. Defaults to an empty string.
        vests (str): The amount of vests held by the witness.
        votes_daily_change (str): The daily change in votes for the witness.
        voters_num (int): The number of voters for the witness.
        voters_num_daily_change (int): The daily change in the number of voters.
        price_feed (float): The price feed provided by the witness.
        bias (int): The bias of the price feed.
        feed_updated_at (datetime): The timestamp when the feed was last updated.
        block_size (int): The block size set by the witness.
        signing_key (str): The signing key of the witness.
        version (str): The version of the witness software.
        missed_blocks (int): The number of blocks missed by the witness.
        hbd_interest_rate (int): The HBD interest rate set by the witness.
        last_confirmed_block_num (int): The number of the last confirmed block.
        account_creation_fee (int): The account creation fee set by the witness.
    """

    witness_name: AccNameType
    rank: int
    url: str = ""
    vests: str
    votes_daily_change: str
    voters_num: int
    voters_num_daily_change: int
    price_feed: float
    bias: int
    feed_updated_at: datetime
    block_size: int
    signing_key: str
    version: str
    missed_blocks: int
    hbd_interest_rate: int
    last_confirmed_block_num: int
    account_creation_fee: int


class WitnessDetails(BaseModel):
    total_operations: int | None = None
    total_pages: int | None = None
    votes_updated_at: datetime | None = None
    witnesses: list[Witness] | None = None
    witness: Witness | None = None
