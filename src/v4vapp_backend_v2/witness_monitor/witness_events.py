import os
from timeit import default_timer as timer

import httpx
from colorama import Fore, Style

from v4vapp_backend_v2.actions.tracked_any import TrackedProducer
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.witness_details import get_hive_witness_details
from v4vapp_backend_v2.hive_models.op_producer_missed import ProducerMissed
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward

ICON = "📡"


async def process_witness_event(tracked_op: TrackedProducer) -> None:
    """Process a witness-related event, such as ProducerReward or ProducerMissed."""
    logger.info(
        f"Witness Event: {tracked_op.op_type:<16} for {tracked_op.producer}",
        extra={"notification": False},
    )
    if tracked_op.producer not in InternalConfig().config.hive.watch_witnesses:
        return
    witness_config = InternalConfig().config.hive.witness_configs.get(tracked_op.producer, None)
    if not witness_config:
        return
    try:
        if isinstance(tracked_op, ProducerReward):
            logger.info(
                f"{ICON} Processing ProducerReward event: {tracked_op.log_str}",
                extra={"notification": False},
            )
            # Add your processing logic for ProducerReward here

        elif isinstance(tracked_op, ProducerMissed):
            logger.info(
                f"{ICON} Processing ProducerMissed event: {tracked_op.log_str}",
                extra={"notification": False},
            )
            # Add your processing logic for ProducerMissed here

        else:
            logger.warning(
                f"{ICON} Unknown witness event type: {type(tracked_op)}",
                extra={"notification": False},
            )

    except Exception as e:
        logger.exception(
            f"{ICON} Error processing witness event {tracked_op.op_type}: {e}",
            extra={"notification": False, "error": e},
        )


async def check_witness_heartbeat(
    witness: str = "",
) -> None:
    """
    Checks the heartbeat of a Hive witness and logs warnings if the witness is down.

    Args:
        witness (str): The account name of the Hive witness.
        last_block_time (int): The timestamp of the last block produced by the witness.
        current_time (int): The current timestamp.

    Returns:
        None
    """
    witness_config = InternalConfig().config.hive.witness_configs.get(witness, None)
    if not witness_config:
        logger.warning(
            f"{ICON} Witness {witness} configuration not found.",
            extra={"notification": False},
        )
        return

    witness_details = await get_hive_witness_details(hive_accname=witness)

    execution_times = []
    failures = 0
    for machine in witness_config.witness_machines:
        machine_is_primary = False
        if witness_details and witness_details.witness:
            if witness_details.witness.signing_key == machine.signing_key:
                machine_is_primary = True
                logger.info(
                    f"{ICON}{Fore.YELLOW} Witness {witness} signing key held by {machine.name}. {Style.RESET_ALL}",
                    extra={"notification": False},
                )
            else:
                logger.info(
                    f"{ICON} Backup {witness} on {machine.name}.",
                    extra={"notification": False},
                )
        result, execution_time = await call_hive_api(machine.url)
        execution_times.append(execution_time)
        if result is None:
            failures += 1
            if machine_is_primary:
                msg = f"🚨 PRIMARY Witness {witness} machine {machine.name} is down."
                log_func = logger.error
            else:
                msg = f"Backup Witness {witness} machine {machine.name} is down."
                log_func = logger.warning
            log_func(
                f"{ICON} {msg}",
                extra={
                    "notification": machine_is_primary,
                    "extra": {"machine": machine.name, "witness": witness, "result": result},
                },
            )
            await send_kuma_heartbeat(
                witness=witness,
                status="down",
                msg=msg,
                ping=execution_time,
            )
    if failures == 0:
        avg_execution_time = sum(execution_times) / len(execution_times)
        logger.info(
            f"{ICON} All Witness Machines for {witness} are up. Average response time: {avg_execution_time:.3f}s",
            extra={"notification": False},
        )
        await send_kuma_heartbeat(
            witness=witness,
            status="up",
            msg=f"Witness {witness} is operational.",
            ping=avg_execution_time,
        )


async def send_kuma_heartbeat(
    witness: str = "",
    status: str = "up",
    msg: str = "Witness monitor heartbeat",
    ping: float | None = None,
) -> None:
    """
    Sends a heartbeat to the Uptime Kuma webhook URL to indicate the witness monitor is alive.
    Args:
        status (str): The status of the service ("up" or "down").
        msg (str): A message to include with the heartbeat.
        ping (float | None): Optional ping time in milliseconds.
    Returns:
        None
    """
    witness_config = InternalConfig().config.hive.witness_configs.get(witness, None)
    if not witness_config:
        logger.warning(
            f"{ICON} Kuma webhook URL not configured. Skipping heartbeat.",
            extra={"notification": False},
        )
        return
    # Use the config webhook URL as an environment variable if set or use it directly
    # This allows for obfuscation of the webhook URL in github actions
    webhook_url = os.getenv(witness_config.kuma_webhook_url, witness_config.kuma_webhook_url)
    try:
        async with httpx.AsyncClient() as client:
            params = {
                "status": status,
                "msg": msg,
                "ping": f"{ping:.3f}" if ping is not None else "",
            }
            response = await client.get(webhook_url, params=params, timeout=10.0)
            response.raise_for_status()  # Raises an exception for 4xx/5xx status codes
            logger.info(
                f"{ICON} Successfully sent heartbeat to Kuma webhook.",
                extra={"notification": False},
            )
    except httpx.HTTPStatusError as e:
        logger.warning(
            f"{ICON} Failed to send heartbeat to Kuma webhook. Status code: {e.response.status_code}",
            extra={"notification": False, "error": e},
        )
    except (httpx.ConnectTimeout, httpx.ReadTimeout) as e:
        logger.error(
            f"{ICON} Timeout error sending heartbeat to Kuma webhook: {e}",
            extra={"notification": False, "error": e},
        )
    except Exception as e:
        logger.exception(
            f"{ICON} Error sending heartbeat to Kuma webhook: {e}",
            extra={"notification": False, "error": e},
        )


async def call_hive_api(url: str) -> tuple[dict | None, float]:
    """
    Calls the Hive API at the given IP and port with a POST request containing
    the get_dynamic_global_properties JSON-RPC payload. Measures execution time
    and checks for a successful response.

    Args:
        ip (str): The IP address of the Hive node.
        port (int): The port number of the Hive node.

    Returns:
        tuple[dict | None, float]: A tuple containing the API result (dict if successful, None otherwise)
        and the execution time in seconds.
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "condenser_api.get_dynamic_global_properties",
        "params": [],
        "id": 1,
    }

    start_time = timer()
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload)
            execution_time = timer() - start_time

            if response.status_code == 200:
                data = response.json()
                if "result" in data and data.get("id") == 1:
                    logger.info(
                        f"{ICON} Successfully called Hive API at {url}. Execution time: {execution_time:.4f}s",
                        extra={"notification": False},
                    )
                    return data["result"], execution_time
                else:
                    logger.warning(
                        f"{ICON} Invalid response from Hive API at {url}: {data}",
                        extra={"notification": False},
                    )
            else:
                logger.warning(
                    f"{ICON} HTTP error from Hive API at {url}: {response.status_code}",
                    extra={"notification": False},
                )
    except (
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
    ) as e:  # Fixed syntax: tuple for multiple exceptions
        execution_time = timer() - start_time
        logger.error(
            f"{ICON} Timeout error calling Hive API at {url}: {e}",
            extra={"notification": False, "error": e},
        )

    except Exception as e:
        execution_time = timer() - start_time
        logger.exception(
            f"{ICON} Error calling Hive API at {url}: {e}",
            extra={"notification": False, "error": e},
        )

    return None, execution_time


"""

Kuma Webhook Documentation

In Uptime Kuma (an open-source self-hosted uptime monitoring tool),
the provided URL is an example of its Push API endpoint, typically used
for "Push" type monitors. This allows external services or scripts to
proactively report their own health status to Uptime Kuma via a simple
HTTP request (e.g., GET or POST), rather than relying on Uptime Kuma to
actively probe the service.

The endpoint follows the pattern /api/push/<pushToken>, where
<pushToken> (e.g., huSWPvpQ0L in your URL) is a unique token generated
for each push monitor. Query parameters like status, msg, and ping
control the details of the status update. Here's a breakdown of their
uses:

status (string, optional): Indicates the current health of the
monitored service. Valid values are "up" (healthy/operational) or
"down" (unhealthy/failing). If omitted, it defaults to "up". This
triggers notifications, status page updates, or alerts based on your
monitor's configuration. In your URL, it's set to "up", signaling
everything is working.

msg (string, optional): Provides a human-readable description or
details about the status (e.g., "All systems nominal" or "Database
overload"). Limited to ~250 characters. If omitted, it defaults to
"OK". This message appears in Uptime Kuma's logs, notifications, and
status pages for context. Your URL uses the default "OK".

ping (number, optional): Reports the response time/latency of the
service in milliseconds (parsed as a float). If omitted or empty (as
in your URL), it defaults to null, meaning no latency data is recorded.
This helps track performance trends over time, such as average response
times on the monitor's dashboard.

"""
