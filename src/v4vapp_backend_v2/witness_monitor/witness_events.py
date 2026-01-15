import os
from timeit import default_timer as timer
from typing import Any, Dict

import httpx
from colorama import Fore, Style
from nectar.witness import Witness as NectarWitness

from v4vapp_backend_v2.actions.tracked_any import TrackedProducer
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import (
    get_hive_client,
    get_verified_hive_client_for_accounts,
    witness_signing_key,
)
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_producer_missed import ProducerMissed
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward

ICON = "📡"


async def process_witness_event(tracked_op: TrackedProducer) -> None:
    """
    Asynchronously processes a witness event for a tracked producer.

    This function logs the witness event, checks if the producer is being watched and has a valid configuration,
    then handles the event based on its type (ProducerReward or ProducerMissed). If the event type is unknown,
    it logs a warning. Any exceptions during processing are caught and logged.

    Args:
        tracked_op (TrackedProducer): The tracked producer operation containing the event details.

    Returns:
        None

    Raises:
        None: Exceptions are caught internally and logged.
    """
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

        elif isinstance(tracked_op, AccountWitnessVote):
            logger.info(
                f"{ICON} Processing AccountWitnessVote event: {tracked_op.log_str}",
                extra={"notification": False},
            )
            # Add your processing logic for AccountWitnessVote here

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
    witness_name: str = "",
    failure_state: bool = False,
) -> bool:
    """
    Checks the heartbeat of a Hive witness and logs warnings if the witness is down.

    Args:
        witness (str): The account name of the Hive witness.
        last_block_time (int): The timestamp of the last block produced by the witness.
        current_time (int): The current timestamp.

    Returns:
        None
    """
    witness_config = InternalConfig().config.hive.witness_configs.get(witness_name, None)
    if not witness_config:
        logger.warning(
            f"{ICON} Witness {witness_name} configuration not found.",
            extra={"notification": False},
        )
        return False

    primary_failure = False
    primary_machine = ""
    failures = 0
    signing_key = witness_signing_key(witness_name)

    # Loop through each witness machine and check its status
    for machine in witness_config.witness_machines:
        if signing_key == machine.signing_key:
            machine.primary = True
            logger.debug(
                f"{ICON}{Fore.YELLOW} Witness {witness_name} signing key held by {machine.name}. {Style.RESET_ALL}",
                extra={"notification": False},
            )
            primary_machine = machine.name
        else:
            machine.primary = False
            logger.debug(
                f"{ICON} Backup {witness_name} on {machine.name}.",
                extra={"notification": False},
            )
        result, machine.execution_time = await verify_hive_witness_rpc_alive(
            machine.url, machine.name
        )
        if result is None:
            # Failure is detected
            machine.working = False
            failures += 1
            if machine.primary:
                msg = (
                    f"🚨 PRIMARY Witness {witness_name} machine {machine.name} is down. {machine}"
                )
                log_func = logger.error
                primary_failure = True
            else:
                msg = f"Backup Witness {witness_name} machine {machine.name} is down. {machine}"
                log_func = logger.warning
            log_func(
                f"{ICON} {msg}",
                extra={
                    "notification": True,
                    "machine": machine,
                    "witness": witness_name,
                    "result": result,
                    "error_code": "witness_error",
                },
            )
            await send_kuma_heartbeat(
                witness=witness_name,
                status="down",
                msg=msg,
                ping=machine.execution_time,
            )
        else:
            # Success
            machine.working = True

        logger.debug(
            f"{ICON} {machine}",
            extra={"notification": False, "machine": machine},
        )

    avg_execution_time = sum(
        machine.execution_time for machine in witness_config.witness_machines
    ) / len(witness_config.witness_machines)

    if primary_failure:
        logger.error(
            f"{ICON} 🚨 PRIMARY Witness {witness_name} is DOWN! Immediate attention required!",
            extra={"notification": True},
        )
        working_machines = [
            machine.name for machine in witness_config.witness_machines if machine.working
        ]
        logger.warning(
            f"{ICON} Working machines for {witness_name}: {', '.join(working_machines) if working_machines else 'None'}",
            extra={"notification": False},
        )
        if working_machines:
            trx_id = await update_witness_properties_switch_machine(
                witness_name=witness_name,
                machine_name=working_machines[0],
                nobroadcast=False,
            )
            if trx_id:
                await send_kuma_heartbeat(
                    witness=witness_name,
                    status="up",
                    msg=f"PRIMARY Witness {witness_name} switched to machine {working_machines[0]} {trx_id}.",
                    ping=avg_execution_time,
                )
            else:
                await send_kuma_heartbeat(
                    witness=witness_name,
                    status="down",
                    msg=f"🚨 FAILED to SWITCH PRIMARY Witness {witness_name} switched to machine {working_machines[0]}.",
                    ping=avg_execution_time,
                )
        else:
            # No working machines available
            logger.critical(
                f"{ICON} 🚨 No working machines available to switch PRIMARY Witness {witness_name} Disabling Witness"
            )
            trx_id = await update_witness_properties_switch_machine(
                witness_name=witness_name,
                machine_name="",
                nobroadcast=False,
            )
            await send_kuma_heartbeat(
                witness=witness_name,
                status="down",
                msg=f"🚨 No working machines available to switch PRIMARY Witness {witness_name} Disabling Witness {trx_id}",
                ping=avg_execution_time,
            )

    # Everything is working normally.
    if failures == 0:
        working_backups = [
            machine.name
            for machine in witness_config.witness_machines
            if machine.working and machine.name != primary_machine
        ]
        working_backups_str = ", ".join(working_backups) if working_backups else "None"
        msg = f"Witness {witness_name} is operational on {primary_machine}, backup(s) {working_backups_str} working."
        if failure_state:
            msg = f"{Fore.WHITE}RECOVERY: {msg}"
            log_func = logger.info
            notification = True
        else:
            log_func = logger.debug
            notification = False
        log_func(
            f"{ICON} {msg} Average response time: {avg_execution_time:.3f}s {Style.RESET_ALL}",
            extra={"notification": notification, "error_code_clear": "witness_error"},
        )
        await send_kuma_heartbeat(
            witness=witness_name,
            status="up",
            msg=msg,
            ping=avg_execution_time,
        )
        return False
    else:
        failed_machines = [
            machine.name for machine in witness_config.witness_machines if not machine.working
        ]
        message = f"Witness {witness_name} has {failures} failed machine(s) {', '.join(failed_machines)}."
        logger.warning(
            f"{ICON} {message} Average response time: {avg_execution_time:.3f}s",
            extra={
                "notification": True,
                "error_code": "witness_error",
            },
        )
        await send_kuma_heartbeat(
            witness=witness_name,
            status="down",
            msg=message,
            ping=avg_execution_time,
        )
        return True


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
            logger.debug(
                f"{ICON} Successfully sent heartbeat to Kuma webhook.",
                extra={"notification": False},
            )
    except httpx.HTTPStatusError as e:
        logger.warning(
            f"{ICON} Failed to send heartbeat to Kuma webhook. Status code: {e.response.status_code}",
            extra={"notification": False, "error": e},
        )
    except (
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
        httpx.ConnectError,
    ) as e:  # Added httpx.ConnectError here
        logger.error(
            f"{ICON} Connection error sending heartbeat to Kuma webhook: {e}",
            extra={"notification": False, "error": e},
        )
    except Exception as e:
        logger.exception(
            f"{ICON} Unexpected error sending heartbeat to Kuma webhook: {e}",
            extra={"notification": False, "error": e},
        )


async def verify_hive_witness_rpc_alive(url: str, machine_name: str) -> tuple[dict | None, float]:
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
    # Make error_code unique per machine to avoid one machine's success
    # clearing another machine's error
    error_code = f"witness_api_invalid_response_{machine_name}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload)
            execution_time = timer() - start_time

            if response.status_code == 200:
                data = response.json()
                if "result" in data and data.get("id") == 1:
                    logger.debug(
                        f"{ICON} Successfully called {machine_name} Hive API at {url}. Execution time: {execution_time:.4f}s",
                        extra={
                            "notification": False,
                            "error_code_clear": error_code,
                        },
                    )
                    return data["result"], execution_time
                else:
                    logger.warning(
                        f"{ICON} Invalid response from Hive API at {url}",
                        extra={
                            "notification": True,
                            "error_code": error_code,
                            "response_data": data,
                        },
                    )
            else:
                logger.warning(
                    f"{ICON} HTTP error from Hive API {machine_name} at {url}: {response.status_code}",
                    extra={
                        "notification": True,
                        "error_code": error_code,
                    },
                )
    except httpx.HTTPError as e:
        execution_time = timer() - start_time
        logger.error(
            f"{ICON} Timeout error calling Hive API {machine_name}at {url}: {e}",
            extra={
                "notification": True,
                "error": e,
                "error_code": error_code,
            },
        )

    except Exception as e:
        execution_time = timer() - start_time
        logger.exception(
            f"{ICON} Error calling Hive API {machine_name} at {url}: {e}",
            extra={
                "notification": False,
                "error": e,
                "error_code": error_code,
            },
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


async def update_witness_properties_switch_machine(
    witness_name: str, machine_name: str, nobroadcast: bool = False
) -> str | None:
    """
    Updates the witness properties for a given witness switching to a specified machine.

    Args:
        witness_name (str): The name of the witness.
        machine_name (str): The name of the machine to use.

    Returns:
        None
    """
    logger.info(
        f"{ICON} Updating witness properties for {witness_name} on machine {machine_name}.",
        extra={"notification": False},
    )
    # Implementation for updating witness properties goes here
    hive = get_hive_client()
    hive = await get_verified_hive_client_for_accounts(
        accounts=[witness_name], nobroadcast=nobroadcast
    )
    if not hive or not hive.rpc:
        logger.warning(
            f"{ICON} Could not get Hive client for {witness_name}.", extra={"notification": False}
        )
        return
    witness_info: Dict[str, Any] | None = hive.rpc.get_witness_by_account(witness_name)
    if not witness_info or "props" not in witness_info:
        logger.warning(
            f"{ICON} Could not retrieve witness info for {witness_name}.",
            extra={"notification": False},
        )
        return

    witness = NectarWitness(witness_name, blockchain_instance=hive)

    if machine_name == "":
        new_signing_key = "STM1111111111111111111111111111111114T1Anm"  # Disable witness
    else:
        new_signing_key = ""
        for machine in InternalConfig().config.hive.witness_configs[witness_name].witness_machines:
            if machine.name == machine_name:
                new_signing_key = machine.signing_key
                break

    if new_signing_key == witness_info.get("signing_key"):
        logger.info(
            f"{ICON} Witness {witness_name} is already using the signing key for machine {machine_name}. No update needed.",
            extra={"notification": False},
        )
        return

    # Prepare the props dict with current values, updating only what's needed
    props = {
        "account_creation_fee": witness_info["props"]["account_creation_fee"],
        "maximum_block_size": witness_info["props"]["maximum_block_size"],
        "hbd_interest_rate": witness_info["props"]["hbd_interest_rate"],
    }

    try:
        # Use the Witness.update() method to update the signing key
        # Note: This assumes the witness account has the necessary keys set up in the Hive client
        trx = witness.update(
            signing_key=new_signing_key,
            url=witness_info.get("url", ""),
            props=props,
            account=witness_name,
        )
        trx_id: str = trx.get("trx_id", "N/A")
        logger.info(
            f"{ICON} {trx_id} Successfully updated witness {witness_name} signing key to {new_signing_key[:10]}... for machine {machine_name}.",
            extra={"notification": True},
        )
        return trx_id
    except Exception as e:
        logger.exception(
            f"{ICON} Error updating witness {witness_name}: {e}",
            extra={"notification": False, "error": e},
        )
    return
