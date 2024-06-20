from unittest.mock import AsyncMock, MagicMock

import pytest
from google.protobuf.json_format import MessageToDict

from v4vapp_backend_v2.lnd_grpc.connect import (  # replace with the actual import
    connect_to_lnd,
    subscribe_invoices,
)


@pytest.mark.asyncio
async def test_connect_to_lnd():
    stub = await connect_to_lnd()
    assert stub is not None
