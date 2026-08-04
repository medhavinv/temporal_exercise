"""The Worker: your process, running your code.

Temporal never executes your code. The Worker long-polls a Task Queue, receives
Workflow Tasks and Activity Tasks, runs them, and reports back. Kill this
process and the Workflow does not die -- it just stops progressing until a
Worker returns.

    python worker.py
"""

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from activities import ALL_ACTIVITIES
from shared import TASK_QUEUE
from workflows import OrderWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=ALL_ACTIVITIES,
    )
    logging.info("Worker polling task queue %r -- ctrl-c to kill it", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
