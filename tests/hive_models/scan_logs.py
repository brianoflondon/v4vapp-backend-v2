import json
from pprint import pprint

from v4vapp_backend_v2.hive_models.op_models import TransferOpTypes
from v4vapp_backend_v2.hive_models.transfer_op_types import AmountPyd, Transfer

if __name__ == "__main__":
    with open("logs/v4vapp-backend-v2.log.jsonl", "r") as infile:
        buffer = ""
        for line in infile:
            hive_event = None
            line_json = json.loads(line)
            if "hive_event" in line:
                hive_event = json.loads(line)["hive_event"]
            if "witness_vote" in line:
                hive_event = json.loads(line)["witness_vote"]
            if "hive_trx" in line:
                hive_event = json.loads(line)["hive_trx"]

            if hive_event:
                # print(
                #     f"{hive_event['type']} {hive_event.get("op_in_trx", "---" ):>3} {line_json.get('line')}"
                # )
                if hive_event["type"] == "transfer":
                    pprint("--------------------")
                    pprint(hive_event)
                    if not hive_event.get("conv"):
                        transfer = Transfer(**hive_event)
                        pprint(transfer.model_dump())
