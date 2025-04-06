import httpx

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive.hive_extras import HiveExp
from v4vapp_backend_v2.hive_models.op_all import op_any
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_all_validate():
    with httpx.Client() as httpx_client:
        for hive_event in load_hive_events():
            try:
                op = op_any(hive_event)
                assert op.type == op.op_name()
                # print(op.markdown_link)
                # print(hive_event.get("type"), op.type, op.link)
                assert op.markdown_link
                if op.link:
                    response = httpx_client.head(op.link)
                    assert response.status_code == 200

            except ValueError as e:
                assert "Unknown operation type" in str(
                    e
                ) or "Invalid CustomJson data" in str(e)
            except Exception as e:
                print(e)
                assert False


# TODO: #47 Need more work hivehub.dev working but others not so much with blocks and 0000
def test_all_block_exporer_links():
    for block_explorer in HiveExp:
        tested_type = []
        with httpx.Client() as httpx_client:
            for hive_event in load_hive_events():
                if hive_event.get("type") in tested_type:
                    continue
                try:
                    tested_type.append(hive_event.get("type"))
                    op = op_any(hive_event)
                    op.block_explorer = block_explorer
                    assert op.type == op.op_name()
                    print(hive_event.get("type"), op.type, op.link)
                    if op.link:
                        response = httpx_client.head(op.link)
                        assert response.status_code == 200

                except ValueError as e:
                    assert "Unknown operation type" in str(
                        e
                    ) or "Invalid CustomJson data" in str(e)
                except Exception as e:
                    print(e)
                    assert False
