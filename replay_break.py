"""Demonstrate a non-determinism error without touching your source tree.

The naive way to show this is to edit workflows.py, replay, and edit it back --
which leaves the repo broken if anything interrupts you, and can leave a Worker
running code that no longer matches the file on disk.

Instead this loads a *modified copy* of the module under a different name, and
replays a recorded history against that. workflows.py is never written to.

    python replay_break.py <history.json>
"""

import argparse
import asyncio
import importlib.util
import json
import pathlib
import re
import sys
import tempfile

from temporalio.client import WorkflowHistory
from temporalio.runtime import LoggingConfig, Runtime, TelemetryConfig, TelemetryFilter
from temporalio.worker import Replayer

# The Rust core logs the non-determinism at WARN before the Python exception is
# raised, which buries the point of the demo. We want only our own output.
QUIET = Runtime(
    telemetry=TelemetryConfig(
        logging=LoggingConfig(filter=TelemetryFilter(core_level="ERROR", other_level="ERROR"))
    )
)

SOURCE = pathlib.Path(__file__).parent / "workflows.py"


def _statement(stage: str) -> str:
    """Regex for one `self._stage = ...` + execute_activity statement.

    Matched individually rather than as one contiguous block, so comments
    between the two steps do not break the swap.
    """
    return (
        r'        self\._stage = "' + stage + r'"\n'
        r"        self\._\w+ = await workflow\.execute_activity\(\n"
        r"(?:.*?\n)*?"
        r"        \)\n"
    )


def load_swapped_module():
    """Import a copy of workflows.py with two Activities in the opposite order."""
    original = SOURCE.read_text()

    charge = re.search(_statement("charging_payment"), original)
    reserve = re.search(_statement("reserving_inventory"), original)
    if not charge or not reserve or charge.start() > reserve.start():
        raise SystemExit(
            "could not locate the charge/reserve steps in workflows.py; "
            "if you have restructured that file, adjust replay_break.py to match"
        )

    # Swap the two statements where they stand, leaving everything between them
    # (including comments) untouched.
    swapped = (
        original[: charge.start()]
        + reserve.group(0)
        + original[charge.end() : reserve.start()]
        + charge.group(0)
        + original[reserve.end() :]
    )

    directory = pathlib.Path(tempfile.mkdtemp(prefix="replay-break-"))
    module_path = directory / "workflows_swapped.py"
    module_path.write_text(swapped)

    # The Workflow sandbox re-imports the module by name on every replay, so
    # the directory holding it has to be importable.
    sys.path.insert(0, str(directory))

    spec = importlib.util.spec_from_file_location("workflows_swapped", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["workflows_swapped"] = module
    spec.loader.exec_module(module)
    return module


async def main(history_path: str) -> None:
    path = pathlib.Path(history_path)
    history = WorkflowHistory.from_json(path.stem, json.loads(path.read_text()))

    from workflows import OrderWorkflow

    print("1. replaying against the real workflows.py")
    await Replayer(workflows=[OrderWorkflow], runtime=QUIET).replay_workflow(history)
    print("   OK -- the code still produces the recorded sequence of commands\n")

    print("2. replaying against a copy with two activities swapped")
    swapped = load_swapped_module()
    try:
        await Replayer(workflows=[swapped.OrderWorkflow], runtime=QUIET).replay_workflow(history)
    except Exception as err:  # noqa: BLE001 - the failure is the point
        message = str(err)
        marker = "Nondeterminism error:"
        detail = message[message.index(marker):].split('"')[0] if marker in message else message
        print(f"   FAILED as it should:\n   {detail}")
    else:
        print("   replayed without error (unexpected)")

    print(
        "\nThis is what would happen to in-flight Workflows if you deployed that "
        "change.\nRun `replay_check.py check` in CI against saved histories to "
        "catch it first."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("history", help="path to a saved history JSON file")
    asyncio.run(main(parser.parse_args().history))
