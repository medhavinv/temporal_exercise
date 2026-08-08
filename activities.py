"""Activities: the side-effecting half of a Temporal Application.

Everything nondeterministic lives here -- network calls, database writes,
randomness, the clock. Activities are retried automatically, so treat them as
*at-least-once*: they must be idempotent.

You never need to edit this file to change how it behaves. The Worker exposes
--flaky, --fail-shipping and --non-idempotent, which set the environment
variables read below. See `python worker.py --help`.
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


# Stands in for a payments ledger. Module-level state in an Activity is a bad
# idea in real code (Workers are replaceable and there may be many of them); it
# exists here only so the --non-idempotent demo has something to corrupt.
_LEDGER: list[str] = []


@activity.defn
async def charge_payment(order: OrderInput) -> str:
    """Charge the customer.

    THE IDEMPOTENCY POINT. Activities are *at-least-once*: a retry, a Worker
    crash after the charge but before the result is reported, or a lost
    response all cause this to run again for the same logical operation. Only
    the Activity itself can prevent a double charge, and it does so by keying
    the side effect on something stable -- here the order id, which is why the
    payment id is derived from it rather than generated fresh.

    Run the Worker with --non-idempotent --flaky to watch the naive version
    charge three times for one order.
    """
    if os.environ.get("NON_IDEMPOTENT_PAYMENT") == "1":
        # The naive implementation: append unconditionally, so every retry is
        # another real charge.
        _LEDGER.append(order.order_id)
        charges = _LEDGER.count(order.order_id)
        activity.logger.warning(
            "NON-IDEMPOTENT charge for %s -- this order has now been charged %d time(s)",
            order.order_id,
            charges,
        )
        _maybe_fail("charge_payment")
        return f"pay-{order.order_id}-x{charges}"

    # The safe version: deriving the payment id from the order id means a retry
    # produces the same id, so a real gateway would recognise the duplicate.
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
