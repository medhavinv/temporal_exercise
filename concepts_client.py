"""Client-side driver for the Workflows in concepts.py.

Each subcommand exercises one concept and prints what to look for in the
resulting Event History.

    python concepts_client.py update
    python concepts_client.py child
    ...
"""

import argparse
import asyncio
import uuid

from temporalio.client import Client, WorkflowFailureError
from temporalio.common import (
    SearchAttributeKey,
    SearchAttributePair,
    TypedSearchAttributes,
)
from temporalio.service import RPCError

from concepts import (
    CancellationWorkflow,
    CounterWorkflow,
    DeterminismWorkflow,
    LocalActivityWorkflow,
    ParentWorkflow,
    SearchableWorkflow,
    VersionedWorkflow,
)
from shared import TASK_QUEUE

ADDRESS = "localhost:7233"


async def _client() -> Client:
    return await Client.connect(ADDRESS)


def _wid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def update() -> None:
    """Update with a validator, plus Signal and Query on the same Workflow."""
    client = await _client()
    handle = await client.start_workflow(
        CounterWorkflow.run, 0, id=_wid("counter"), task_queue=TASK_QUEUE
    )
    print(f"workflow: {handle.id}")

    print("  add(5)  ->", await handle.execute_update(CounterWorkflow.add, 5))
    print("  add(7)  ->", await handle.execute_update(CounterWorkflow.add, 7))

    # The validator rejects this. Nothing is written to the history and the
    # total is unchanged -- that is what makes Update different from Signal.
    try:
        await handle.execute_update(CounterWorkflow.add, -1)
        print("  add(-1) -> NOT REJECTED (unexpected)")
    except Exception as err:  # noqa: BLE001 - surface whatever the server said
        cause = getattr(err, "cause", None)
        print(f"  add(-1) -> rejected by validator: {cause or err}")

    print("  query   ->", await handle.query(CounterWorkflow.total))
    await handle.signal(CounterWorkflow.finish)
    print("  result  ->", await handle.result())
    print("\nlook for: WorkflowExecutionUpdateAccepted/Completed for the two "
          "accepted updates, and NOTHING for the rejected one")


async def continue_as_new() -> None:
    """Drive the counter past its history threshold to force a rollover."""
    client = await _client()
    wid = _wid("cont")
    handle = await client.start_workflow(
        CounterWorkflow.run, 0, id=wid, task_queue=TASK_QUEUE
    )
    first_run = handle.first_execution_run_id
    print(f"workflow: {wid}\nfirst run: {first_run}")

    for i in range(25):
        await handle.execute_update(CounterWorkflow.add, 1)

    await asyncio.sleep(2)
    desc = await client.get_workflow_handle(wid).describe()
    print(f"current run: {desc.run_id}")
    print(f"rolled over: {desc.run_id != first_run}")
    print(f"total preserved across the rollover: "
          f"{await client.get_workflow_handle(wid).query(CounterWorkflow.total)}")
    await client.get_workflow_handle(wid).signal(CounterWorkflow.finish)
    print("\nlook for: WorkflowExecutionContinuedAsNew ending the first run, "
          "and a second run with a fresh, short history")


async def child() -> None:
    client = await _client()
    result = await client.execute_workflow(
        ParentWorkflow.run,
        ["ada", "alan", "grace"],
        id=_wid("parent"),
        task_queue=TASK_QUEUE,
    )
    print("result:", result)
    print("\nlook for: three StartChildWorkflowExecutionInitiated / "
          "ChildWorkflowExecutionCompleted pairs in the PARENT's history, "
          "and three separate Workflows in `temporal workflow list`")


async def determinism() -> None:
    client = await _client()
    result = await client.execute_workflow(
        DeterminismWorkflow.run, id=_wid("determinism"), task_queue=TASK_QUEUE
    )
    for key, value in result.items():
        print(f"  {key:8} {value}")
    print("\nsave this history and replay it (replay_check.py) -- the uuid and "
          "random values come back identical, because they were recorded")


async def cancellation() -> None:
    """Real Temporal cancellation, not a hand-rolled Signal."""
    client = await _client()
    handle = await client.start_workflow(
        CancellationWorkflow.run, 60, id=_wid("cancel"), task_queue=TASK_QUEUE
    )
    print(f"workflow: {handle.id} -- running a 60s activity")
    await asyncio.sleep(3)

    print("sending cancellation...")
    await handle.cancel()

    try:
        await handle.result()
        print("completed (unexpected)")
    except WorkflowFailureError as err:
        print(f"ended as: {type(err.cause).__name__}")
    except Exception as err:  # noqa: BLE001
        print(f"ended as: {type(err).__name__}")

    print("\nlook for: WorkflowExecutionCancelRequested, the slow_task activity "
          "cancelling, then the SHIELDED cleanup activity still running to "
          "completion before WorkflowExecutionCanceled")


async def local_activity() -> None:
    client = await _client()
    result = await client.execute_workflow(
        LocalActivityWorkflow.run,
        args=[2, 40],
        id=_wid("local"),
        task_queue=TASK_QUEUE,
    )
    print("result:", result)
    print("\nlook for: MarkerRecorded (a LocalActivity marker) instead of the "
          "ActivityTaskScheduled/Started/Completed trio -- no Matching round trip")


async def versioning() -> None:
    client = await _client()
    result = await client.execute_workflow(
        VersionedWorkflow.run, "ada", id=_wid("versioned"), task_queue=TASK_QUEUE
    )
    print("result:", result, "(uppercased => the patched branch was taken)")
    print("\nlook for: MarkerRecorded with a 'core_patch' / patch id entry. "
          "A history recorded before the patch replays down the OLD branch.")


async def searchable() -> None:
    client = await _client()

    key = SearchAttributeKey.for_keyword("Category")
    wid = _wid("searchable")
    try:
        handle = await client.start_workflow(
            SearchableWorkflow.run,
            "urgent",
            id=wid,
            task_queue=TASK_QUEUE,
            # Set at start time; the Workflow also upserts from the inside.
            search_attributes=TypedSearchAttributes(
                [SearchAttributePair(key, "starting")]
            ),
            memo={"opened_by": "concepts_client"},
        )
    except RPCError as err:
        print(f"could not start: {err}")
        print("register the attribute first:\n"
              "  temporal operator search-attribute create "
              "--name Category --type Keyword")
        return

    print("result:", await handle.result())

    desc = await handle.describe()
    # memo() is async because the values go through the data converter.
    print("memo:              ", await desc.memo())
    print("search attributes: ", {
        pair.key.name: pair.value for pair in desc.typed_search_attributes
    })

    print("\nnow query the Visibility store by attribute rather than by ID:")
    found = [
        wf.id
        async for wf in client.list_workflows("Category = 'urgent'")
    ]
    print(f"  Category = 'urgent' -> {len(found)} workflows, e.g. {found[:3]}")


COMMANDS = {
    "update": update,
    "continue-as-new": continue_as_new,
    "child": child,
    "determinism": determinism,
    "cancellation": cancellation,
    "local-activity": local_activity,
    "versioning": versioning,
    "searchable": searchable,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=sorted(COMMANDS))
    asyncio.run(COMMANDS[parser.parse_args().command]())


if __name__ == "__main__":
    main()
