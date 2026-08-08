"""Activities: the side-effecting half of a Temporal Application.

Everything nondeterministic lives here -- network calls, database writes,
randomness, the clock. Activities are retried automatically, so treat them as
*at-least-once*: they must be idempotent.

The FLAKY_ACTIVITIES / FAIL_SHIPPING env vars exist so you can trigger the
failure paths on purpose in Experiment 2 of the README.
"""

import asyncio
import os

from temporalio import activity
from temporalio.exceptions import ApplicationError

from shared import OrderInput


def _maybe_fail(name: str) -> None:
    """Fail the first two attempts when FLAKY_ACTIVITIES=1.

    Keyed on the attempt number rather than randomly, so the retry sequence in
    the Event History is the same every time you run the experiment. Note that
    `activity.info()` is fine here -- Activities have no determinism rules.
    """
    if os.environ.get("FLAKY_ACTIVITIES") != "1":
        return
    attempt = activity.info().attempt
    if attempt < 3:
        raise RuntimeError(f"{name} failed on attempt {attempt} (injected)")


@activity.defn
async def charge_payment(order: OrderInput) -> str:
    _maybe_fail("charge_payment")
    activity.logger.info("Charging %s cents for %s", order.amount_cents, order.order_id)
    await asyncio.sleep(0.2)
    return f"pay-{order.order_id}"


@activity.defn
async def reserve_inventory(order: OrderInput) -> str:
    _maybe_fail("reserve_inventory")
    activity.logger.info("Reserving %s for %s", order.item, order.order_id)
    await asyncio.sleep(0.2)
    return f"resv-{order.order_id}"


@activity.defn
async def ship_order(order: OrderInput, reservation_id: str) -> str:
    """Long enough to be worth heartbeating; also the designated failure point."""
    if os.environ.get("FAIL_SHIPPING") == "1":
        # A non-retryable failure. The Workflow catches this and compensates
        # instead of retrying forever -- see Experiment 2.
        raise ApplicationError(
            f"carrier rejected shipment for {order.order_id}",
            type="ShippingRejected",
            non_retryable=True,
        )

    _maybe_fail("ship_order")
    for i in range(3):
        await asyncio.sleep(0.2)
        # Heartbeats let Temporal detect a dead Worker mid-Activity rather than
        # waiting for the whole start_to_close_timeout to elapse.
        activity.heartbeat(i)
    return f"trk-{order.order_id}"


# --- Compensations -----------------------------------------------------------
# The saga pattern: for each step that can succeed, an inverse that undoes it.


@activity.defn
async def refund_payment(payment_id: str) -> None:
    activity.logger.info("Refunding %s", payment_id)
    await asyncio.sleep(0.1)


@activity.defn
async def release_inventory(reservation_id: str) -> None:
    activity.logger.info("Releasing %s", reservation_id)
    await asyncio.sleep(0.1)


ALL_ACTIVITIES = [
    charge_payment,
    reserve_inventory,
    ship_order,
    refund_payment,
    release_inventory,
]
