"""The Client: starts Workflows and talks to running ones.

    python starter.py start                 # start one, wait for the result
    python starter.py start --no-wait       # start and return immediately
    python starter.py status <workflow-id>  # Query a running Workflow
    python starter.py cancel <workflow-id>  # Signal a running Workflow
"""

import argparse
import asyncio
import uuid

from temporalio.client import Client

from shared import TASK_QUEUE, OrderInput
from workflows import OrderWorkflow


async def start(wait: bool) -> None:
    client = await Client.connect("localhost:7233")

    order_id = f"order-{uuid.uuid4().hex[:8]}"
    handle = await client.start_workflow(
        OrderWorkflow.run,
        OrderInput(
            order_id=order_id,
            customer_id="cust-1",
            item="a very durable widget",
            amount_cents=4200,
        ),
        # The Workflow ID is yours to choose, and it is the deduplication key:
        # starting again with the same ID while one is running is rejected.
        # Use a real business identifier here, not a random one.
        id=order_id,
        task_queue=TASK_QUEUE,
    )
    print(f"started {handle.id} (run {handle.result_run_id})")
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

    p_status = sub.add_parser("status")
    p_status.add_argument("workflow_id")

    p_cancel = sub.add_parser("cancel")
    p_cancel.add_argument("workflow_id")

    args = parser.parse_args()
    if args.command == "start":
        asyncio.run(start(wait=not args.no_wait))
    elif args.command == "status":
        asyncio.run(status(args.workflow_id))
    elif args.command == "cancel":
        asyncio.run(cancel(args.workflow_id))


if __name__ == "__main__":
    main()
