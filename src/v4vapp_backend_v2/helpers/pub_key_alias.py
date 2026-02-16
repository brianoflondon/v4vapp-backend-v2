from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import get_node_info
from v4vapp_backend_v2.models.payment_models import NodeAlias, Payment

LOCAL_PUB_KEY_ALIAS_CACHE = {}


async def get_all_pub_key_aliases(col_pub_keys: str = "pub_keys") -> dict[str, str]:
    """
    Get all the public key aliases from the database. This creates a local in memory
    cache of all the public key aliases for quick lookups.

        db_client (MongoDBClient): The database client to use for querying.
        col_pub_keys (str,M optional): The collection name containing public keys.
                Defaults to "pub_keys".

        dict[str, str]: A dictionary mapping public keys to their aliases.
    """
    all_pub_key_aliases = {}
    cursor = InternalConfig.db[col_pub_keys].find(filter={}, projection={"pub_key": 1, "alias": 1})
    async for document in cursor:
        all_pub_key_aliases[document["pub_key"]] = document["alias"]
    return all_pub_key_aliases


async def update_payment_route_with_alias(
    lnd_client: LNDClient,
    payment: Payment,
    pub_keys: list[str] = [],
    fill_cache: bool = False,
    force_update: bool = False,
    col_pub_keys: str = "pub_keys",
) -> None:
    """
    Update the payment route with the alias of the public key.

    This function updates the payment route by associating a public key with its alias.
    It can optionally fill a cache with all public key aliases from the database.

    The Payment passed by reference will be updated with the alias of the public key.

    Args:
        db_client (MongoDBClient): The MongoDB client to interact with the database.
        lnd_client (LNDClient): The LND client to interact with the Lightning Network
            Daemon.
        payment (Payment): The payment object to update the route for.
        pub_key (str): The public key to find the alias for.
        fill_cache (bool, optional): Whether to fill the cache with all public key
            aliases. Defaults to False.
        col_pub_keys (str, optional): The collection name for public keys in the
            database. Defaults to "pub_keys".

    Returns:
        None
    """
    if not pub_keys:
        pub_keys = payment.destination_pub_keys
        if not pub_keys:
            return
    if payment.route and not force_update:
        return
    global LOCAL_PUB_KEY_ALIAS_CACHE
    if fill_cache and not LOCAL_PUB_KEY_ALIAS_CACHE:
        LOCAL_PUB_KEY_ALIAS_CACHE = await get_all_pub_key_aliases(col_pub_keys)

    for pub_key in pub_keys:
        if not LOCAL_PUB_KEY_ALIAS_CACHE:
            # Find the alias for the pub key one by one.
            alias = await InternalConfig.db[col_pub_keys].find_one({"pub_key": pub_key})
            if alias:
                LOCAL_PUB_KEY_ALIAS_CACHE = {alias["pub_key"]: alias["alias"]}
            else:
                LOCAL_PUB_KEY_ALIAS_CACHE = {}

        if pub_key not in LOCAL_PUB_KEY_ALIAS_CACHE.keys():
            node_info = await get_node_info(pub_key, lnd_client)
            if node_info.node.alias:
                hop_alias = NodeAlias(pub_key=pub_key, alias=node_info.node.alias)
            else:
                hop_alias = NodeAlias(pub_key=pub_key, alias=f"Unknown {pub_key[-6:]}")
            db_ans = await InternalConfig.db[col_pub_keys].update_one(
                filter={"pub_key": pub_key},
                update={"$set": hop_alias.model_dump()},
                upsert=True,
            )
            logger.debug(
                f"Updated alias for {pub_key} to {hop_alias.alias}",
                extra={"pub_key": pub_key, "alias": hop_alias.alias, "db_ans": db_ans},
            )
            LOCAL_PUB_KEY_ALIAS_CACHE[pub_key] = hop_alias.alias
        else:
            hop_alias = NodeAlias(pub_key=pub_key, alias=LOCAL_PUB_KEY_ALIAS_CACHE[pub_key])
        # Update the payment route with the alias.
        if not payment.route:
            payment.route = []
        payment.route.append(hop_alias)


async def decode_payment_request_and_attach(lnd_client: LNDClient, payment: Payment) -> None:
    """
    Decode a BOLT-11 payment request and attach its `description` field
    to the provided `Payment` object as `invoice_description`.

    This function is safe to call repeatedly and will silently log failures
    without raising so callers can continue normal processing.

    Args:
        lnd_client (LNDClient): Client used to call DecodePayReq
        payment (Payment): Payment model instance to mutate
    """
    try:
        if payment.payment_request:
            decode_request = lnrpc.PayReqString(pay_req=payment.payment_request)
            decode_response: lnrpc.PayReq = await lnd_client.call(
                lnd_client.lightning_stub.DecodePayReq, decode_request
            )
            decoded = MessageToDict(decode_response, preserving_proto_field_name=True)
            # Keep None if missing; callers may set defaults when storing
            invoice_description = decoded.get("description", "")
            user_memo = (
                invoice_description
                or getattr(payment.custom_records, "keysend_message", None)
                or "Not set"
            )
            payment.invoice_description = user_memo
    except Exception as e:
        # Use warning to make issues visible but don't fail the caller
        logger.warning(
            f"{getattr(lnd_client, 'icon', '')} Could not decode payment_request for {payment.payment_hash}: {e}",
            extra={"notification": False},
        )
        return
