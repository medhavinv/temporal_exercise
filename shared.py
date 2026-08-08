"""Types and constants shared by the Workflow, the Activities, and the Client.

Anything that crosses the Worker/Client boundary gets serialized (JSON by
default), so keep these plain and stable.
"""

from dataclasses import dataclass

TASK_QUEUE = "orders"


@dataclass
class OrderInput:
    order_id: str
    customer_id: str
    item: str
    amount_cents: int

    # Which behaviour of OrderWorkflow to run -- see variants.py.
    #
    # This rides along as Workflow *input* precisely so it gets recorded in the
    # Event History at WorkflowExecutionStarted. Replay reads the same recorded
    # value forever, so branching on it stays deterministic. Reading the same
    # setting from an environment variable inside the Workflow would not be
    # safe: a Worker restarted with a different value would replay down a
    # different branch.
    #
    # Defaulted so histories recorded before this field existed still
    # deserialize -- adding an optional field is a backward-compatible change,
    # removing one or renaming one is not.
    variant: str = "default"


@dataclass
class OrderStatus:
    """Whatever the Workflow reports when you query it."""

    stage: str
    # Which variant this run is executing, echoed back so you can tell two
    # concurrent runs apart without digging into the history.
    variant: str = "default"
    payment_id: str | None = None
    reservation_id: str | None = None
    tracking_number: str | None = None
    cancelled: bool = False
    compensations_run: list[str] | None = None
