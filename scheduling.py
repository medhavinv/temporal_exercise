"""Temporal's scheduling features -- all three of them, which are distinct.

1. Start Delay      -- run this one Workflow once, later.
2. Cron Schedule    -- legacy repeated execution, configured on the Workflow.
3. Schedules        -- the real API: a first-class, pausable, backfillable
                       object you can describe, trigger, and update.

Prefer Schedules for anything recurring. Cron is kept for compatibility and
cannot be paused, backfilled, or inspected in the same way.

    python scheduling.py delay
    python scheduling.py cron
    python scheduling.py create
    python scheduling.py describe|trigger|pause|unpause|backfill|delete
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleBackfill,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
)

from concepts import DeterminismWorkflow
from shared import TASK_QUEUE

SCHEDULE_ID = "determinism-every-10s"
ADDRESS = "localhost:7233"


async def _client() -> Client:
    return await Client.connect(ADDRESS)


async def start_delay() -> None:
    """One-shot, deferred. The Workflow exists immediately but sits waiting."""
    client = await _client()
    handle = await client.start_workflow(
        DeterminismWorkflow.run,
        id=f"delayed-{datetime.now(timezone.utc).timestamp():.0f}",
        task_queue=TASK_QUEUE,
        start_delay=timedelta(seconds=15),
    )
    print(f"started {handle.id} -- it will not begin executing for 15 seconds")


async def cron() -> None:
    """The legacy path. Each run schedules the next; no pause, no backfill."""
    client = await _client()
    handle = await client.start_workflow(
        DeterminismWorkflow.run,
        id="cron-determinism",
        task_queue=TASK_QUEUE,
        cron_schedule="* * * * *",  # every minute
    )
    print(f"started cron workflow {handle.id} -- terminate it to stop the loop")


async def create() -> None:
    client = await _client()
    handle = await client.create_schedule(
        SCHEDULE_ID,
        Schedule(
            action=ScheduleActionStartWorkflow(
                DeterminismWorkflow.run,
                id="scheduled-determinism",
                task_queue=TASK_QUEUE,
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(seconds=10))]
            ),
            # What to do if a run is still going when the next one is due.
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            state=ScheduleState(note="created by scheduling.py"),
        ),
    )
    print(f"created schedule {handle.id}, firing every 10s")


async def describe() -> None:
    client = await _client()
    desc = await client.get_schedule_handle(SCHEDULE_ID).describe()
    print(f"id:        {desc.id}")
    print(f"paused:    {desc.schedule.state.paused}")
    print(f"note:      {desc.schedule.state.note}")
    print(f"num_taken: {desc.info.num_actions}")
    print(f"recent:    {[r.started_at for r in desc.info.recent_actions[-3:]]}")
    print(f"upcoming:  {desc.info.next_action_times[:3]}")


async def trigger() -> None:
    """Fire once right now, without disturbing the schedule."""
    await (await _client()).get_schedule_handle(SCHEDULE_ID).trigger()
    print("triggered an immediate run")


async def pause(paused: bool) -> None:
    handle = (await _client()).get_schedule_handle(SCHEDULE_ID)
    if paused:
        await handle.pause(note="paused from scheduling.py")
    else:
        await handle.unpause(note="resumed from scheduling.py")
    print("paused" if paused else "unpaused")


async def backfill() -> None:
    """Replay a past window as if the schedule had been running then.

    This is the feature cron cannot do, and the usual reason to migrate.
    """
    now = datetime.now(timezone.utc)
    await (await _client()).get_schedule_handle(SCHEDULE_ID).backfill(
        ScheduleBackfill(
            start_at=now - timedelta(minutes=2),
            end_at=now - timedelta(minutes=1),
            overlap=ScheduleOverlapPolicy.ALLOW_ALL,
        )
    )
    print("backfilled the window from 2 minutes ago to 1 minute ago")


async def delete() -> None:
    await (await _client()).get_schedule_handle(SCHEDULE_ID).delete()
    print(f"deleted schedule {SCHEDULE_ID}")


COMMANDS = {
    "delay": start_delay,
    "cron": cron,
    "create": create,
    "describe": describe,
    "trigger": trigger,
    "pause": lambda: pause(True),
    "unpause": lambda: pause(False),
    "backfill": backfill,
    "delete": delete,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=sorted(COMMANDS))
    asyncio.run(COMMANDS[parser.parse_args().command]())


if __name__ == "__main__":
    main()
