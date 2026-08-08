"""Test environment selection.

By default the tests run against a **time-skipping** test server, which
fast-forwards Timers so a 30-day Workflow finishes in milliseconds. That server
is a binary the SDK downloads from temporal.download on first use.

If that download is blocked (sandboxes, CI without egress, air-gapped machines),
point the tests at a Temporal server you already have running instead:

    TEMPORAL_TEST_ADDRESS=localhost:7233 pytest -q

Timers then run in real time, so the suite takes about ten seconds rather than
one. Tests that need time skipping to be meaningful skip themselves.
"""

import os

import pytest
import pytest_asyncio
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment


@pytest_asyncio.fixture
async def env():
    address = os.environ.get("TEMPORAL_TEST_ADDRESS")
    if address:
        client = await Client.connect(address)
        yield WorkflowEnvironment.from_client(client)
        return

    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as err:  # pragma: no cover - environment dependent
        pytest.skip(
            f"could not start the time-skipping test server ({err}). "
            "Set TEMPORAL_TEST_ADDRESS=localhost:7233 to use a running server."
        )
    async with environment:
        yield environment
