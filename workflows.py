"""The Workflow: deterministic orchestration code.

Read this file as a state machine that gets *replayed*. Every time a Worker
picks up this Workflow it re-executes this function from the top against the
recorded Event History, then continues past the last recorded event. That is
why the rules are what they are:

  * no datetime.now(), random, uuid4, network calls, or file I/O here
  * no asyncio.sleep -- use workflow.sleep, which becomes a durable Timer
  * no threads, no unordered iteration over sets
  * side effects go in Activities

The Python SDK runs Workflow code in a sandbox that catches most violations for
you, but the sandbox is a safety net, not a guarantee.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

# Imports used by Workflow code are re-validated by the sandbox on every replay.
# Activity *definitions* are passed through untouched -- we only reference them
# by symbol here; the code itself runs in the Activity Worker, not in here.
with workflow.unsafe.imports_passed_through():
    from activities import (
        charge_payment,
        refund_payment,
        release_inventory,
        reserve_inventory,
        ship_order,
    )
    from shared import OrderInput, OrderStatus


# How long we let the customer change their mind before we ship.
CANCELLATION_WINDOW = timedelta(seconds=10)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        # Ordinary instance state. It is NOT persisted -- it is rebuilt by
        # replaying the history. That is the whole trick.
        self._stage = "starting"
        self._payment_id: str | None = None
        self._reservation_id: str | None = None
        self._tracking_number: str | None = None
        self._cancelled = False
        self._compensations: list[str] = []

    @workflow.run
    async def run(self, order: OrderInput) -> str:
        # --- EXPERIMENT 3 (see README) --------------------------------------
        # Uncomment this to break determinism, then replay an old history:
        #
        # import datetime
        # workflow.logger.info("started at %s", datetime.datetime.now())
        # --------------------------------------------------------------------

        retry = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_attempts=5,
        )

        self._stage = "charging_payment"
        self._payment_id = await workflow.execute_activity(
            charge_payment,
            order,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry,
        )

        self._stage = "reserving_inventory"
        self._reservation_id = await workflow.execute_activity(
            reserve_inventory,
            order,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry,
        )

        # A durable Timer. The Worker can be killed for the whole window and
        # the Workflow still wakes up on time -- the Timer lives in the
        # Temporal Service, not in this process.
        self._stage = "awaiting_cancellation_window"
        try:
            await workflow.wait_condition(
                lambda: self._cancelled, timeout=CANCELLATION_WINDOW
            )
        except TimeoutError:
            # Nobody cancelled; the window closed. Carry on.
            pass

        if self._cancelled:
            await self._compensate()
            self._stage = "cancelled"
            return f"order {order.order_id} cancelled by customer"

        self._stage = "shipping"
        try:
            self._tracking_number = await workflow.execute_activity(
                ship_order,
                args=[order, self._reservation_id],
                start_to_close_timeout=timedelta(seconds=60),
                heartbeat_timeout=timedelta(seconds=10),
                retry_policy=retry,
            )
        except ActivityError as err:
            # Retries are exhausted (or the failure was non-retryable). Unwind
            # the completed steps in reverse order, then fail the Workflow.
            self._stage = "compensating"
            await self._compensate()
            self._stage = "failed"
            raise ApplicationError(
                f"order {order.order_id} failed and was rolled back"
            ) from err

        self._stage = "completed"
        return self._tracking_number

    async def _compensate(self) -> None:
        """Saga rollback: undo what succeeded, newest first."""
        if self._reservation_id:
            await workflow.execute_activity(
                release_inventory,
                self._reservation_id,
                start_to_close_timeout=timedelta(seconds=30),
            )
            self._compensations.append("release_inventory")
        if self._payment_id:
            await workflow.execute_activity(
                refund_payment,
                self._payment_id,
                start_to_close_timeout=timedelta(seconds=30),
            )
            self._compensations.append("refund_payment")

    # --- Handlers ------------------------------------------------------------
    # Signals are fire-and-forget writes. Queries are synchronous reads and must
    # not mutate state or await anything. Updates are writes that return a value
    # and can be rejected by a validator.

    @workflow.signal
    def cancel(self) -> None:
        self._cancelled = True

    @workflow.query
    def status(self) -> OrderStatus:
        return OrderStatus(
            stage=self._stage,
            payment_id=self._payment_id,
            reservation_id=self._reservation_id,
            tracking_number=self._tracking_number,
            cancelled=self._cancelled,
            compensations_run=list(self._compensations),
        )
