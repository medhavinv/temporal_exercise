"""Test environment selection.

By default the tests run against a **time-skipping** test server, which
fast-forwards Timers so a 30-day Workflow finishes in milliseconds. That server
is a binary the SDK downloads from temporal.download on first use.

If that download is blocked (sandboxes, CI without egress, air-gapped machines),
the tests can run against a Temporal server you already have running. Two
environment variables control that, and the difference matters:

    # fall back to a live server ONLY if time skipping cannot start
    TEMPORAL_TEST_FALLBACK_ADDRESS=localhost:7233 pytest -q

    # force a live server, never even try time skipping
    TEMPORAL_TEST_ADDRESS=localhost:7233 pytest -q

`./lab.sh test` sets the *fallback*, so a machine that can reach
temporal.download gets real time skipping and one that cannot still runs the
suite. Against a live server Timers run in real time, so the suite takes about a
minute rather than a second, and tests that need time skipping to be meaningful
skip themselves.
"""

import os
import warnings

import pytest
import pytest_asyncio
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

# `start_time_skipping()` downloads a binary on first use. This fixture is
# function-scoped, so without caching the outcome a blocked download would be
# retried -- and time out -- once per test in the suite.
_time_skipping_failure: str | None = None


async def _live(address: str):
    client = await Client.connect(address)
    return WorkflowEnvironment.from_client(client)


@pytest_asyncio.fixture
async def env():
    global _time_skipping_failure

    forced = os.environ.get("TEMPORAL_TEST_ADDRESS")
    if forced:
        yield await _live(forced)
        return

    fallback = os.environ.get("TEMPORAL_TEST_FALLBACK_ADDRESS")

    if _time_skipping_failure is None:
        try:
            environment = await WorkflowEnvironment.start_time_skipping()
        except RuntimeError as err:  # pragma: no cover - environment dependent
            _time_skipping_failure = str(err)
            warnings.warn(
                f"could not start the time-skipping test server ({err}). "
                "Timers will run in real time and the tests that need time "
                "skipping will skip themselves.",
                stacklevel=1,
            )
        else:
            async with environment:
                yield environment
            return

    if fallback:
        yield await _live(fallback)
        return

    pytest.skip(
        f"could not start the time-skipping test server "
        f"({_time_skipping_failure}). Set "
        "TEMPORAL_TEST_FALLBACK_ADDRESS=localhost:7233 to use a running server."
    )
