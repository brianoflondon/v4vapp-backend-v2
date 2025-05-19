#                 "v4vapp_backend_v2.lnd_grpc.lnd_client.LNDClient.node_get_info",
import os
from random import randint
from unittest.mock import Mock
from uuid import uuid4

import pytest

from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import send_lightning_to_pay_req
from v4vapp_backend_v2.models.payment_models import Payment


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Skipping test in GitHub Actions environment",
)
@pytest.mark.asyncio
async def test_send_lightning_to_pay_req():
    """
    Asynchronously tests sending a Lightning payment to a given pay request.
    This test performs the following steps:
    1. Generates a random payment amount.
    2. Initializes an LND client for the "umbrel" connection.
    3. Decodes a Lightning address into a payment request using the specified amount and a test comment.
    4. Prints the decoded payment request for inspection.
    5. Generates a unique group ID.
    6. Sends a Lightning payment using the decoded pay request, including a test chat message and the group ID.
    Raises:
        Any exceptions encountered during the decoding or payment process.
    """

    receive_callback = Mock()

    # address_to_test_send = "v4vapp.dev@sats.v4v.app"
    address_to_test_send = "brianoflondon@walletofsatoshi.com"
    # address_to_test_send = "lnbc120n1p5zkqvcpp5gvr9ncywjnp4r5tf8sztmpm0yvfuw7w0akn0va5mkfg5zcysg4rqdqqcqzpgxqyz5vqsp52jymfs2vaaundqzp7mxq6m4ng27tzlf7aa63qcuerfq8887h6c8q9qxpqysgqrqza94fcgqaru9ue52udzpuw5tsm8rnvt82xp7c4vh4av907fmsjhwdc64e7lavxgn94kr9kzw0qvm63vfav982glvt6nnu2ll6mnlgpkq3dy7"
    # address_to_test_send = "lnbc1p5zkr5cpp5xvjgz8v3ftfukxzh4ufz86h34dfm9hm3f6djlw0dqxxpmyvr4cwscqzyssp5rg0aadtejwftahx8qnhksxvsxujkw9ykcd7fud5d3lednyl32gxq9q7sqqqqqqqqqqqqqqqqqqqsqqqqqysgqdqqmqz9gxqyjw5qrzjqwryaup9lh50kkranzgcdnn2fgvx390wgj5jd07rwr3vxeje0glclluekcrqntmr4uqqqqlgqqqqqeqqjqdzgxsc90s2x60w658024l7mx34zrjjgwn9tzkq933ndugvm5p4xzs6dd8gw8ndaatt2kghzuca2u2xn9yp8v0q3gqav2exj0s9upf6spa8kren"
    # 0 value Muun address

    random_amt = 500 + randint(1, 99)
    lnd_client = LNDClient(connection_name="umbrel")
    pay_req = await decode_any_lightning_string(
        input=address_to_test_send,
        sats=random_amt,
        lnd_client=lnd_client,
        comment=f"Test comment {random_amt}",
    )
    print(pay_req)
    group_id = str(uuid4())
    await send_lightning_to_pay_req(
        pay_req=pay_req,
        lnd_client=lnd_client,
        chat_message=f"Test chat message on the payment {group_id}",
        group_id=group_id,
        callback=receive_callback,
        amount_msat=random_amt * 1000,
        callback_args={"group_id": group_id},
        fee_limit_ppm=500,
    )
    # check that receive_callback was called
    assert receive_callback.called
    assert receive_callback.call_count == 1
    # check that the first argument of the callback is a Payment object
    assert isinstance(receive_callback.call_args[0][0], Payment)
    # check that the group_id in the callback args matches the one we sent
    assert receive_callback.call_args[1]["group_id"] == group_id
