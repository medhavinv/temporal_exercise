"""One small Workflow per Temporal concept the order saga doesn't reach.

`workflows.py` is the realistic example. This file is the reference shelf: each
Workflow here is deliberately tiny so the concept is the only thing in view.
Drive them with `./lab.sh <name>` and read the resulting Event History.

Covered here:
    CounterWorkflow        Update (+ validator), Continue-As-New, Signal, Query
    ParentWorkflow         Child Workflows, parallel execution
    DeterminismWorkflow    workflow.now / uuid4 / random -- deterministic under replay
    CancellationWorkflow   real Workflow cancellation + cleanup that still runs
    LocalActivityWorkflow  Local Activities, and when not to use them
    VersionedWorkflow      workflow.patched() for safe deploys mid-flight
    SearchableWorkflow     Search Attributes and Memos (the Visibility store)
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import SearchAttributeKey

with workflow.unsafe.imports_passed_through():
    pass


# --- Activities used only by these examples ----------------------------------


@activity.defn
async def say_hello(name: str) -> str:
    return f"hello {name}"


@activity.defn
async def slow_task(seconds: int) -> str:
    """Cancellable: it heartbeats, so it learns about cancellation promptly."""
    try:
        for i in range(seconds):
            activity.heartbeat(i)
            await asyncio.sleep(1)
        return "finished"
    except asyncio.CancelledError:
        activity.logger.info("slow_task was cancelled")
        raise


@activity.defn
async def cleanup(reason: str) -> str:
    activity.logger.info("cleaning up: %s", reason)
    return f"cleaned up after {reason}"


@activity.defn
async def add_numbers(a: int, b: int) -> int:
    """Trivial and pure -- the profile that suits a Local Activity."""
    return a + b


CONCEPT_ACTIVITIES = [say_hello, slow_task, cleanup, add_numbers]


# --- Update, Continue-As-New -------------------------------------------------


@workflow.defn
class CounterWorkflow:
    """An 'entity Workflow': runs indefinitely, accumulating state.

    Update is the third client interaction primitive, and the one people miss:
      * Signal -- write, fire-and-forget, cannot return a value or be rejected
      * Query  -- read, synchronous, must not mutate or await
      * Update -- write, synchronous, returns a value, CAN be rejected by a
                  validator before it is ever written to the history

    Because it never ends on its own, it would grow an unbounded Event History.
    Continue-As-New is the fix: atomically end this run and start a fresh one
    with the same Workflow ID and a carried-over starting state. The Workflow
    ID stays stable for clients; the history resets to empty.
    """

    # @workflow.init makes __init__ receive the same arguments as run(), and is
    # the fix for a genuine trap: Signal and Update handlers can execute BEFORE
    # the first line of the run method. If this class initialised `_total` at
    # the top of run() instead, an Update that arrived in the very first
    # Workflow Task would be applied and then immediately overwritten. Always
    # initialise Workflow state in __init__.
    @workflow.init
    def __init__(self, start: int = 0) -> None:
        self._total = start
        self._done = False

    @workflow.run
    async def run(self, start: int = 0) -> int:
        # Wake up when someone finishes us, or when the history gets long
        # enough that we should roll over into a new run.
        await workflow.wait_condition(
            lambda: self._done or workflow.info().get_current_history_length() > 40
        )

        if self._done:
            return self._total

        # Drain in-flight handlers first. Continue-As-New ends the run
        # immediately, so any Update still executing would be lost and its
        # caller would see a failure. This is the other half of the same trap.
        await workflow.wait_condition(workflow.all_handlers_finished)

        # Carry the state forward. Everything else -- history, local variables,
        # pending handlers -- is discarded.
        workflow.logger.info("history is long; continuing as new with %s", self._total)
        workflow.continue_as_new(self._total)

    @workflow.update
    def add(self, amount: int) -> int:
        """Returns the new total to the caller -- a Signal could not do this."""
        self._total += amount
        return self._total

    @add.validator
    def validate_add(self, amount: int) -> None:
        """Runs BEFORE the update is admitted to the history.

        Raising here rejects the caller's Update without recording anything and
        without advancing Workflow state. Validators must not mutate or await.
        """
        if amount < 0:
            raise ValueError("amount must be non-negative")
        if amount > 1000:
            raise ValueError("amount too large")

    @workflow.signal
    def finish(self) -> None:
        self._done = True

    @workflow.query
    def total(self) -> int:
        return self._total


# --- Child Workflows ---------------------------------------------------------


@workflow.defn
class GreetingChildWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            say_hello, name, start_to_close_timeout=timedelta(seconds=10)
        )


@workflow.defn
class ParentWorkflow:
    """Child Workflows vs Activities: reach for a Child when the sub-task needs
    its own Event History, its own Workflow ID, its own retry/timeout policy, or
    is large enough to deserve independent visibility. Otherwise use an
    Activity -- Children are heavier and their events land in the parent too.
    """

    @workflow.run
    async def run(self, names: list[str]) -> list[str]:
        # asyncio.gather is deterministic here: results come back in argument
        # order regardless of completion order, so replay is stable.
        return list(
            await asyncio.gather(
                *[
                    workflow.execute_child_workflow(
                        GreetingChildWorkflow.run,
                        name,
                        id=f"{workflow.info().workflow_id}-child-{name}",
                    )
                    for name in names
                ]
            )
        )


# --- Determinism helpers -----------------------------------------------------


@workflow.defn
class DeterminismWorkflow:
    """The SDK's deterministic replacements for the things you must not call.

    Each of these is recorded on first execution and replayed identically
    afterwards, which is why the same Workflow re-run from history produces the
    same values rather than new ones.
    """

    @workflow.run
    async def run(self) -> dict[str, str]:
        return {
            # Not datetime.now(): this is the Workflow Task's recorded time.
            "now": str(workflow.now()),
            # Not uuid.uuid4(): seeded deterministically from the run.
            "uuid": str(workflow.uuid4()),
            # Not random.random(): a seeded, replay-stable PRNG.
            "random": str(workflow.random().random()),
            "run_id": workflow.info().run_id,
            "attempt": str(workflow.info().attempt),
        }


# --- Cancellation ------------------------------------------------------------


@workflow.defn
class CancellationWorkflow:
    """Cancellation is cooperative, and different from a Signal you invent.

    `workflows.py` uses a custom `cancel` Signal -- fine, but it only works if
    the Workflow author wrote a handler. This is *real* Temporal cancellation
    (`temporal workflow cancel`), which surfaces in Python as a
    CancelledError raised at the current await point.

    The catch: once cancelled, new Activities are cancelled immediately too. To
    run cleanup you must shield it, which is why the `finally` below wraps the
    cleanup Activity in asyncio.shield().
    """

    @workflow.run
    async def run(self, seconds: int = 60) -> str:
        try:
            return await workflow.execute_activity(
                slow_task,
                seconds,
                start_to_close_timeout=timedelta(seconds=seconds + 10),
                heartbeat_timeout=timedelta(seconds=5),
                # Ask Temporal to actually interrupt the running Activity
                # rather than just abandoning it.
                cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            )
        except asyncio.CancelledError:
            # Without the shield this Activity would be cancelled before it ran.
            result = await asyncio.shield(
                workflow.execute_activity(
                    cleanup,
                    "cancellation",
                    start_to_close_timeout=timedelta(seconds=10),
                )
            )
            workflow.logger.info("cleanup done: %s", result)
            raise  # re-raise so the Workflow ends as CANCELLED, not COMPLETED


# --- Local Activities --------------------------------------------------------


@workflow.defn
class LocalActivityWorkflow:
    """Local Activities skip the Matching service round trip.

    They're cheaper but weaker: no independent Task Queue routing, no long
    heartbeating, and a much shorter practical duration. Use them for fast,
    pure, low-risk work. Anything that calls a network service should be a
    regular Activity.
    """

    @workflow.run
    async def run(self, a: int, b: int) -> int:
        return await workflow.execute_local_activity(
            add_numbers,
            args=[a, b],
            start_to_close_timeout=timedelta(seconds=5),
        )


# --- Versioning / patching ---------------------------------------------------


@workflow.defn
class VersionedWorkflow:
    """How to change Workflow code without breaking in-flight executions.

    `workflow.patched("id")` returns True on new runs and False when replaying a
    history recorded before the patch existed -- so old runs keep taking the old
    branch while new runs take the new one. The marker is written into history.

    Lifecycle: ship with patched() -> wait for old runs to drain ->
    deprecate_patch() -> eventually delete the branch. Worker Versioning is the
    heavier alternative that pins whole runs to a Worker build ID.
    """

    @workflow.run
    async def run(self, name: str) -> str:
        if workflow.patched("greeting-v2"):
            return await workflow.execute_activity(
                say_hello, name.upper(), start_to_close_timeout=timedelta(seconds=10)
            )
        return await workflow.execute_activity(
            say_hello, name, start_to_close_timeout=timedelta(seconds=10)
        )


# --- Search Attributes and Memos ---------------------------------------------

CATEGORY_KEY = SearchAttributeKey.for_keyword("Category")


@workflow.defn
class SearchableWorkflow:
    """Visibility: how you find Workflows without knowing their IDs.

    Search Attributes are indexed and queryable with `temporal workflow list
    --query "Category = 'urgent'"`. They must be registered on the Namespace
    first, and are limited in type and number.

    Memos are arbitrary attached data -- returned when you describe a Workflow,
    but NOT indexed and NOT queryable. Use a Memo when you just want to carry
    context along, a Search Attribute when you need to search on it.
    """

    @workflow.run
    async def run(self, category: str) -> str:
        # Attributes can be set at start time by the Client, or upserted from
        # inside the Workflow as it learns more -- both land in the history.
        workflow.upsert_search_attributes([CATEGORY_KEY.value_set(category)])
        workflow.upsert_memo({"note": f"categorised as {category}"})
        await workflow.sleep(timedelta(seconds=2))
        return f"categorised as {category}"


CONCEPT_WORKFLOWS = [
    CounterWorkflow,
    ParentWorkflow,
    GreetingChildWorkflow,
    DeterminismWorkflow,
    CancellationWorkflow,
    LocalActivityWorkflow,
    VersionedWorkflow,
    SearchableWorkflow,
]
