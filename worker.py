"""The Worker: your process, running your code.

Temporal never executes your code. The Worker long-polls a Task Queue, receives
Workflow Tasks and Activity Tasks, runs them, and reports back. Kill this
process and the Workflow does not die -- it just stops progressing until a
Worker returns.

    python worker.py                     # normal
    python worker.py --flaky             # every Activity fails its first 2 tries
    python worker.py --fail-shipping     # ship_order fails non-retryably
    python worker.py --non-idempotent    # charge_payment double-charges on retry

WHY THESE ARE WORKER FLAGS, NOT WORKFLOW VARIANTS
-------------------------------------------------
Workflow behaviour is selected per-run through Workflow input (see
variants.py), because Workflow code is replayed and must be deterministic.
Activity behaviour has no such constraint -- Activities run exactly once per
attempt and are never replayed -- so injecting failures via process-level flags
is fine here, and lets you change Activity behaviour without starting a new
Workflow.

The flags are translated into environment variables *before* the Activity
module is imported, which is why the import sits inside main() rather than at
the top of the file.
"""

import argparse
import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from shared import TASK_QUEUE


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flaky",
        action="store_true",
        help="every Activity fails its first two attempts, so retries are visible",
    )
    parser.add_argument(
        "--fail-shipping",
        action="store_true",
        help="ship_order raises a non-retryable error, triggering compensation",
    )
    parser.add_argument(
        "--non-idempotent",
        action="store_true",
        help="charge_payment charges again on every retry -- the at-least-once trap",
    )
    parser.add_argument(
        "--address", default="localhost:7233", help="Temporal frontend address"
    )
    parser.add_argument("--task-queue", default=TASK_QUEUE)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO)

    # Set these before importing the Activity module: it reads them at import
    # time to decide how to misbehave.
    if args.flaky:
        os.environ["FLAKY_ACTIVITIES"] = "1"
    if args.fail_shipping:
        os.environ["FAIL_SHIPPING"] = "1"
    if args.non_idempotent:
        os.environ["NON_IDEMPOTENT_PAYMENT"] = "1"

    from activities import ALL_ACTIVITIES
    from concepts import CONCEPT_ACTIVITIES, CONCEPT_WORKFLOWS
    from workflows import OrderWorkflow

    client = await Client.connect(args.address)

    # One Worker can serve many Workflow and Activity types. What it can *not*
    # do is serve a type it has not registered: if a Workflow is started for a
    # type missing here, its Workflow Task fails and the run stalls in RUNNING.
    worker = Worker(
        client,
        task_queue=args.task_queue,
        workflows=[OrderWorkflow, *CONCEPT_WORKFLOWS],
        activities=[*ALL_ACTIVITIES, *CONCEPT_ACTIVITIES],
    )

    injected = [
        name
        for name, on in [
            ("flaky", args.flaky),
            ("fail-shipping", args.fail_shipping),
            ("non-idempotent", args.non_idempotent),
        ]
        if on
    ]
    logging.info(
        "Worker polling task queue %r%s -- ctrl-c to kill it",
        args.task_queue,
        f" [injected: {', '.join(injected)}]" if injected else "",
    )

    # Blocks until interrupted. Everything above was setup; this is the loop
    # that polls, executes, and reports results back to the Service.
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
