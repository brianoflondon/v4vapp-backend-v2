import json
from typing import Generator

from pydantic import ValidationError


from v4vapp_backend_v2.depreciated.htlc_event_models import (
    ChannelName,
    HtlcEvent,
    HtlcTrackingList,
)
from v4vapp_backend_v2.models.invoice_models import Invoice


def read_log_file_channel_names(file_path: str) -> Generator[ChannelName, None, None]:
    with open(file_path, "r") as file:
        # Parse each line as JSON and yield the htlc_event data
        for line in file.readlines():
            try:
                log_entry = json.loads(line)
                if "channel_name" in log_entry:
                    yield ChannelName.model_validate(log_entry["channel_name"])

            except ValidationError as e:
                print(e)
                continue
            except Exception as e:
                print(e)
                continue


def read_log_file_htlc_invoice(
    file_path: str,
) -> Generator[HtlcEvent | Invoice, None, None]:
    with open(file_path, "r") as file:
        # Parse each line as JSON and yield the htlc_event data
        for line in file.readlines():
            try:
                log_entry = json.loads(line)
                if "htlc_data" in log_entry:
                    yield HtlcEvent.model_validate(log_entry["htlc_data"])
                if "invoice_data" in log_entry:
                    yield Invoice.model_validate(log_entry["invoice_data"])

            except ValidationError as e:
                print(e)
                continue
            except Exception as e:
                print(e)
                continue


def fill_channel_names(tracking: HtlcTrackingList, file_path: str) -> HtlcTrackingList:
    try:
        for name in read_log_file_channel_names(file_path):
            tracking.add_name(name)
            print(name)

        return tracking
    except Exception as e:
        print(e)
        assert False


def test_read_all_log():
    file_path = "tests/data/combined_test_data.safe_log"
    tracking = HtlcTrackingList()
    tracking = fill_channel_names(tracking, file_path)
    assert tracking.names
    for item in read_log_file_htlc_invoice(file_path):
        if isinstance(item, HtlcEvent):
            htlc_id = tracking.add_event(item)
            complete = tracking.complete_group(htlc_id)
            if complete:
                print(f"✅ Complete group, Delete group {tracking.message(htlc_id)}")
                tracking.delete_event(htlc_id)
        if isinstance(item, Invoice):
            tracking.add_invoice(item)
    print(tracking.events)
    tracking.remove_expired_invoices()
    print(tracking.invoices)
    assert tracking
