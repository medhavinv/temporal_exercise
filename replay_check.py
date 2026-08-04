"""Replay real Event Histories against the current Workflow code.

This is the safety check you run in CI before deploying a change to Workflow
code while Workflows started by the old code are still in flight. It replays
recorded histories through the code in this repo; if your edit changed the
sequence of Commands the Workflow produces, replay fails here instead of
wedging production.

    # save the history of a finished run to disk
    python replay_check.py save order-abc123

    # replay every saved history against the current code
    python replay_check.py check
"""

import argparse
import asyncio
import json
import pathlib

from temporalio.client import Client, WorkflowHistory
from temporalio.worker import Replayer

from workflows import OrderWorkflow

HISTORY_DIR = pathlib.Path(__file__).parent / "histories"


async def save(workflow_id: str) -> None:
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)
    history = await handle.fetch_history()

    HISTORY_DIR.mkdir(exist_ok=True)
    path = HISTORY_DIR / f"{workflow_id}.json"
    path.write_text(json.dumps(history.to_json_dict(), indent=2))
    print(f"wrote {path}")


async def check() -> None:
    paths = sorted(HISTORY_DIR.glob("*.json"))
    if not paths:
        print(f"no histories in {HISTORY_DIR} -- run 'save <workflow-id>' first")
        return

    replayer = Replayer(workflows=[OrderWorkflow])
    for path in paths:
        history = WorkflowHistory.from_json(path.stem, json.loads(path.read_text()))
        await replayer.replay_workflow(history)
        print(f"OK   {path.name}")
    print(f"\nreplayed {len(paths)} histories against the current code")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p_save = sub.add_parser("save")
    p_save.add_argument("workflow_id")
    sub.add_parser("check")

    args = parser.parse_args()
    if args.command == "save":
        asyncio.run(save(args.workflow_id))
    else:
        asyncio.run(check())


if __name__ == "__main__":
    main()
