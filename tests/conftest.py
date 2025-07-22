import asyncio

import pytest

"""
    1. Add a Session-Scoped Event Loop
Create a conftest.py in your tests directory to override the
fixture with session scope:
"""


@pytest.fixture(scope="session")
def event_loop():
    """Create single event loop for entire session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()
