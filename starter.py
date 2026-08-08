"""The Client: starts Workflows and talks to running ones.

    python starter.py start                          # wait for the result
    python starter.py start --no-wait                # return immediately
    python starter.py start --variant no-retries     # a different behaviour
    python starter.py start --list-variants          # what is available
    python starter.py status <workflow-id>           # Query a running Workflow
    python starter.py cancel <workflow-id>           # Signal a running Workflow

The Client is just a gRPC caller. It does not run any of your Workflow or
Activity code -- a Worker does that. You can start a Workflow with no Worker
running at all; it simply sits in RUNNING until one shows up.
"""

import argparse
import asyncio
import uuid

from temporalio.client import Client

import variants
from shared import TASK_QUEUE, OrderInput
from workflows import OrderWorkflow


async def start(wait: bool, variant: str) -> None:
    # Fail fast on a typo, before we create a Workflow that cannot run.
    config = variants.get(variant)
    client = await Client.connect("localhost:7233")

    order_id = f"order-{uuid.uuid4().hex[:8]}"
    handle = await client.start_workflow(
        OrderWorkflow.run,
        OrderInput(
            order_id=order_id,
            customer_id="cust-1",
            item="a very durable widget",
            amount_cents=4200,
            # Recorded in the history, and therefore replay-safe. See
            # variants.py for why this is input rather than an env var.
            variant=config.name,
        ),
        # The Workflow ID is yours to choose, and it is the deduplication key:
        # starting again with the same ID while one is running is rejected.
        # Use a real business identifier here, not a random one.
        id=order_id,
        task_queue=TASK_QUEUE,
    )
    print(f"started Workflow ID {handle.id} (Run ID {handle.result_run_id})")
    if config.name != variants.DEFAULT:
        print(f"variant: {config.name} -- {config.summary}")
    print(f"watch it at http://localhost:8233/namespaces/default/workflows/{handle.id}")

    if wait:
        print("result:", await handle.result())


async def status(workflow_id: str) -> None:
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)
    print(await handle.query(OrderWorkflow.status))


async def cancel(workflow_id: str) -> None:
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)
    await handle.signal(OrderWorkflow.cancel)
    print(f"cancel signal sent to {workflow_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_start = sub.add_parser("start")
    p_start.add_argument("--no-wait", action="store_true")
    p_start.add_argument(
        "--variant",
        default=variants.DEFAULT,
        choices=sorted(variants.VARIANTS),
        help="which behaviour of OrderWorkflow to run (see --list-variants)",
    )
    p_start.add_argument(
        "--list-variants",
        action="store_true",
        help="print the available variants and exit",
    )

    p_status = sub.add_parser("status")
    p_status.add_argument("workflow_id")

    p_cancel = sub.add_parser("cancel")
    p_cancel.add_argument("workflow_id")

    args = parser.parse_args()
    if args.command == "start":
        if args.list_variants:
            print("Available variants:\n")
            print(variants.describe_all())
            return
        asyncio.run(start(wait=not args.no_wait, variant=args.variant))
    elif args.command == "status":
        asyncio.run(status(args.workflow_id))
    elif args.command == "cancel":
        asyncio.run(cancel(args.workflow_id))


if __name__ == "__main__":
    main()
