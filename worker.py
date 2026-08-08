"""The Worker: your process, running your code.

Temporal never executes your code. The Worker long-polls a Task Queue, receives
Workflow Tasks and Activity Tasks, runs them, and reports back. Kill this
process and the Workflow does not die -- it just stops progressing until a
Worker returns.

    python worker.py

    WORKFLOW_MODULE=workflows_experiment3 python worker.py  # run against a copy
"""

import asyncio
import importlib
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import ALL_ACTIVITIES
from shared import TASK_QUEUE

# Defaults to the repo's own workflows.py, but Experiment 3 asks you to edit
# Workflow code -- set WORKFLOW_MODULE to point the Worker at an isolated copy
# (e.g. `cp workflows.py workflows_experiment3.py`) instead of editing and
# reverting the original. See README, Experiment 3.
WORKFLOW_MODULE = os.environ.get("WORKFLOW_MODULE", "workflows")
OrderWorkflow = importlib.import_module(WORKFLOW_MODULE).OrderWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=ALL_ACTIVITIES,
    )
    logging.info(
        "Worker polling task queue %r with workflow code from %r -- ctrl-c to kill it",
        TASK_QUEUE,
        WORKFLOW_MODULE,
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
