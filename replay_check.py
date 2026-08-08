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


async def save(workflow_id: str, directory: str | None = None) -> None:
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle(workflow_id)
    history = await handle.fetch_history()

    target_dir = pathlib.Path(directory) if directory else HISTORY_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{workflow_id}.json"
    path.write_text(json.dumps(history.to_json_dict(), indent=2))
    print(f"wrote {path}")


async def check(target: str | None = None) -> None:
    """Replay one history file, or every history in a directory.

    Note that histories accumulate across code changes. A history recorded by
    an older version of your Workflow *should* fail here -- that is the check
    doing its job, not a broken file.
    """
    location = pathlib.Path(target) if target else HISTORY_DIR
    if location.is_file():
        paths = [location]
    else:
        paths = sorted(location.glob("*.json"))

    if not paths:
        print(f"no histories in {location} -- run 'save <workflow-id>' first")
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
    p_save.add_argument("--dir", default=None, help="where to write the history")
    p_check = sub.add_parser("check")
    p_check.add_argument(
        "target", nargs="?", default=None, help="a history file or a directory"
    )

    args = parser.parse_args()
    if args.command == "save":
        asyncio.run(save(args.workflow_id, args.dir))
    else:
        asyncio.run(check(args.target))


if __name__ == "__main__":
    main()
