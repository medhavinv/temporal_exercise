"""Workflow tests for the order saga.

By default these run against a time-skipping test server: the 10-second
cancellation window in OrderWorkflow costs no real wall-clock time. Change
CANCELLATION_WINDOW to 30 days and these tests still finish in about a second.

    pytest -q

See tests/conftest.py for how to run them against an already-running server
instead, when the test-server download is unavailable.
"""

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared import OrderInput
from workflows import OrderWorkflow


def an_order() -> OrderInput:
    return OrderInput(
        order_id="order-test",
        customer_id="cust-1",
        item="widget",
        amount_cents=100,
    )


# Activities are mocked by name, so the Workflow logic is tested in isolation
# from whatever the real implementations do. This is the standard way to test
# Workflows: the orchestration is the unit under test, not the side effects.


@activity.defn(name="charge_payment")
async def charge_ok(order: OrderInput) -> str:
    return "pay-1"


@activity.defn(name="reserve_inventory")
async def reserve_ok(order: OrderInput) -> str:
    return "resv-1"


@activity.defn(name="ship_order")
async def ship_ok(order: OrderInput, reservation_id: str) -> str:
    return "trk-1"


@activity.defn(name="ship_order")
async def ship_boom(order: OrderInput, reservation_id: str) -> str:
    raise RuntimeError("carrier is on fire")


@activity.defn(name="refund_payment")
async def refund_ok(payment_id: str) -> None:
    return None


@activity.defn(name="release_inventory")
async def release_ok(reservation_id: str) -> None:
    return None


def worker_for(env: WorkflowEnvironment, ship, task_queue: str) -> Worker:
    return Worker(
        env.client,
        task_queue=task_queue,
        workflows=[OrderWorkflow],
        activities=[charge_ok, reserve_ok, ship, refund_ok, release_ok],
    )


async def test_happy_path_ships_and_returns_tracking_number(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, ship_ok, task_queue):
        result = await env.client.execute_workflow(
            OrderWorkflow.run,
            an_order(),
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        assert result == "trk-1"


async def test_shipping_failure_rolls_back_payment_and_inventory(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, ship_boom, task_queue):
        handle = await env.client.start_workflow(
            OrderWorkflow.run,
            an_order(),
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        with pytest.raises(WorkflowFailureError):
            await handle.result()

        status = await handle.query(OrderWorkflow.status)
        assert status.stage == "failed"
        assert status.compensations_run == ["release_inventory", "refund_payment"]


async def test_cancel_signal_during_window_triggers_compensation(env):
    task_queue = f"tq-{uuid.uuid4()}"
    async with worker_for(env, ship_ok, task_queue):
        handle = await env.client.start_workflow(
            OrderWorkflow.run,
            an_order(),
            id=f"wf-{uuid.uuid4()}",
            task_queue=task_queue,
        )

        # Wait until the Workflow is actually sitting in the window before
        # signalling, so the test does not race the Workflow.
        for _ in range(100):
            status = await handle.query(OrderWorkflow.status)
            if status.stage == "awaiting_cancellation_window":
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("workflow never reached the cancellation window")

        await handle.signal(OrderWorkflow.cancel)
        result = await handle.result()

        assert "cancelled by customer" in result
        status = await handle.query(OrderWorkflow.status)
        assert status.compensations_run == ["release_inventory", "refund_payment"]
