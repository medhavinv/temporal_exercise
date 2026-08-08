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


# How long we let the customer change their mind before we ship. Because this
# becomes a durable Timer rather than a process-local sleep, the value could be
# 30 days and cost nothing to wait out -- and the time-skipping test environment
# fast-forwards it either way.
CANCELLATION_WINDOW = timedelta(seconds=10)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        """Runs once per Workflow Task, not once per Workflow.

        Every replay constructs a fresh instance and re-runs `run` from the top,
        so these fields are rebuilt from scratch each time and end up back at
        the same values -- provided the code is deterministic.

        Ordinary instance state. It is NOT persisted -- it is rebuilt by
        replaying the history. That is the whole trick.
        """
        self._stage = "starting"
        self._payment_id: str | None = None
        self._reservation_id: str | None = None
        self._tracking_number: str | None = None
        self._cancelled = False
        self._compensations: list[str] = []

    @workflow.run
    async def run(self, order: OrderInput) -> str:
        """The entry point. `@workflow.run` marks the single method Temporal
        invokes when an execution starts; the Client names it (OrderWorkflow.run)
        when it calls start_workflow/execute_workflow.

        The flow, in order:

          1. charge_payment    -- Activity. Records self._payment_id.
          2. reserve_inventory -- Activity. Records self._reservation_id.
          3. cancellation window -- a durable Timer. We wait up to
             CANCELLATION_WINDOW for a `cancel` Signal. Timing out is the
             normal, expected path; being cancelled compensates and returns.
          4. ship_order        -- Activity. On failure, compensate (undo 2 then
             1) and fail the Workflow. On success, return the tracking number.

        Each `await workflow.execute_activity(...)` below is a suspension point:
        the Workflow yields, the Worker dispatches an Activity Task, and when
        the result comes back the Workflow is *replayed from the top* with that
        result already in the Event History. The assignments to `self._*` are
        therefore re-executed on every replay -- which is why they must be
        deterministic and why nothing here is persisted directly.
        """
        # --- EXPERIMENT 3 (see README) --------------------------------------
        # Uncomment this to break determinism, then replay an old history:
        #
        # import datetime
        # workflow.logger.info("started at %s", datetime.datetime.now())
        # --------------------------------------------------------------------

        # Retries are handled by the Temporal Service, not by a loop in here:
        # wait 1s, then 2s, 4s, 8s, giving up after 5 total attempts. The
        # Workflow just sees one await that either returns or raises once the
        # policy is exhausted.
        retry = RetryPolicy(
            initial_interval=timedelta(seconds=1),
            backoff_coefficient=2.0,
            maximum_attempts=5,
        )

        # --- Step 1: take the money -----------------------------------------
        # start_to_close_timeout caps a *single attempt* -- if the Activity runs
        # longer than 30s that attempt is failed and the retry policy applies.
        self._stage = "charging_payment"
        self._payment_id = await workflow.execute_activity(
            charge_payment,
            order,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry,
        )

        # --- Step 2: hold the goods -----------------------------------------
        # Holding both a payment id and a reservation id is what makes the saga
        # necessary: from here on, failing means we owe the customer an undo.
        self._stage = "reserving_inventory"
        self._reservation_id = await workflow.execute_activity(
            reserve_inventory,
            order,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=retry,
        )

        # --- Step 3: the cancellation window --------------------------------
        # A durable Timer. The Worker can be killed for the whole window and
        # the Workflow still wakes up on time -- the Timer lives in the
        # Temporal Service, not in this process.
        #
        # wait_condition re-evaluates the predicate every time the Workflow is
        # woken (e.g. by the `cancel` Signal handler setting self._cancelled)
        # and returns as soon as it is true. The `timeout` puts a Timer on that
        # wait, so exactly one of two things happens: the Signal arrives, or the
        # Timer fires and TimeoutError is raised.
        #
        # Note this stage name is what the tests poll for via the `status`
        # Query, to be sure the Workflow is really parked here before signalling.
        self._stage = "awaiting_cancellation_window"
        try:
            await workflow.wait_condition(
                lambda: self._cancelled, timeout=CANCELLATION_WINDOW
            )
        except TimeoutError:
            # Nobody cancelled; the window closed. Carry on. This is the happy
            # path -- a TimeoutError here is expected, not an error condition.
            pass

        # The customer changed their mind inside the window: undo steps 2 and 1
        # and finish *successfully* (a cancellation is a valid outcome, not a
        # failure), so the Client gets a result rather than an exception.
        if self._cancelled:
            await self._compensate()
            self._stage = "cancelled"
            return f"order {order.order_id} cancelled by customer"

        # --- Step 4: ship it ------------------------------------------------
        self._stage = "shipping"
        try:
            # `args=[...]` (rather than a single positional) is how you pass
            # multiple arguments to an Activity.
            #
            # heartbeat_timeout: ship_order calls activity.heartbeat() as it
            # works. If 10s pass with no heartbeat, Temporal declares the
            # attempt dead and retries it immediately, instead of waiting out
            # the full 60s start_to_close_timeout.
            self._tracking_number = await workflow.execute_activity(
                ship_order,
                args=[order, self._reservation_id],
                start_to_close_timeout=timedelta(seconds=60),
                heartbeat_timeout=timedelta(seconds=10),
                retry_policy=retry,
            )
        # ActivityError is the wrapper Temporal raises into Workflow code when
        # an Activity ultimately fails; the underlying cause is chained on it.
        except ActivityError as err:
            # Retries are exhausted (or the failure was non-retryable). Unwind
            # the completed steps in reverse order, then fail the Workflow.
            self._stage = "compensating"
            await self._compensate()
            self._stage = "failed"
            # ApplicationError is how you deliberately fail a Workflow. The
            # Client sees this as a WorkflowFailureError. We set the terminal
            # stage *before* raising so a post-mortem `status` Query still
            # reports what happened.
            raise ApplicationError(
                f"order {order.order_id} failed and was rolled back"
            ) from err

        self._stage = "completed"
        # The Workflow's return value; the Client's execute_workflow/result()
        # resolves to exactly this.
        return self._tracking_number

    async def _compensate(self) -> None:
        """Saga rollback: undo what succeeded, newest first.

        Nothing here is special to Temporal -- this is an ordinary private
        method, called explicitly from the two places that need it (the
        cancellation branch and the shipping-failure branch). Temporal has no
        notion of "compensation" and will never call this for you.

        The `if` guards make it safe to call at any point: we only undo the
        steps that actually completed. Order matters -- inventory is released
        before the payment is refunded, i.e. the reverse of the order the
        forward steps ran in. `self._compensations` records what ran so the
        `status` Query (and the tests) can see it.
        """
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
        """Signal handler: a customer asked to cancel.

        Signals are asynchronous writes with no return value. This just flips a
        flag; the effect is that the `wait_condition` in step 3 wakes up and
        sees its predicate become true. If the Signal arrives after the window
        has closed, it is simply recorded and ignored -- checking the flag only
        inside the window is a deliberate choice, not an oversight.
        """
        self._cancelled = True

    @workflow.query
    def status(self) -> OrderStatus:
        """Query handler: read the Workflow's current state from outside.

        Queries are synchronous, must not mutate state, and must not await --
        they are served by replaying history, so a mutation here would corrupt
        the execution. Note `list(self._compensations)` returns a copy so the
        caller cannot reach into Workflow state.

        This works on closed Workflows too, which is how the tests inspect the
        final stage after a failure.
        """
        return OrderStatus(
            stage=self._stage,
            payment_id=self._payment_id,
            reservation_id=self._reservation_id,
            tracking_number=self._tracking_number,
            cancelled=self._cancelled,
            compensations_run=list(self._compensations),
        )
