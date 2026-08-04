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


@dataclass
class OrderStatus:
    """Whatever the Workflow reports when you query it."""

    stage: str
    payment_id: str | None = None
    reservation_id: str | None = None
    tracking_number: str | None = None
    cancelled: bool = False
    compensations_run: list[str] | None = None
