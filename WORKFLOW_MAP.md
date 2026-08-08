# Workflow → Activity map

Which Workflow calls which Activity, and for which use case. There is exactly
one Workflow in this repo (`OrderWorkflow` in `workflows.py`), registered on the
`orders` Task Queue by `worker.py`. Every Activity below lives in
`activities.py` and is registered via `ALL_ACTIVITIES`.

## The table

| Use case | Workflow | Activities called, in order | Terminal state |
|---|---|---|---|
| **Happy path** — order goes out the door | `OrderWorkflow.run` | `charge_payment` → `reserve_inventory` → *(durable Timer, 10s)* → `ship_order` | Completed, returns `trk-<order_id>` |
| **Customer cancels** inside the window (`cancel` Signal) | `OrderWorkflow.run` → `_compensate` | `charge_payment` → `reserve_inventory` → `release_inventory` → `refund_payment` | Completed, returns `"order … cancelled by customer"` |
| **Shipping fails** (retries exhausted, or non-retryable `ShippingRejected`) | `OrderWorkflow.run` → `_compensate` | `charge_payment` → `reserve_inventory` → `ship_order` ✗ → `release_inventory` → `refund_payment` | Failed with `ApplicationError` |
| **Transient Activity failure** (`FLAKY_ACTIVITIES=1`) | `OrderWorkflow.run` | same as the case it occurs in; the failing Activity is retried by the Service (5 attempts, 1s initial, ×2 backoff) | unchanged — the Workflow never sees it |
| **Read current stage** (`status` Query) | `OrderWorkflow.status` | *none* — Queries must not call Activities | n/a, returns `OrderStatus` |
| **Request cancellation** (`cancel` Signal) | `OrderWorkflow.cancel` | *none* — sets a flag that `wait_condition` is watching | n/a |

Compensations always run newest-first: `release_inventory` before
`refund_payment`. `_compensate` is guarded — it only releases a reservation if
`reserve_inventory` succeeded, and only refunds if `charge_payment` succeeded.

## Per-Activity reference

| Activity | Called from | Timeouts / policy | Purpose |
|---|---|---|---|
| `charge_payment` | `run`, stage `charging_payment` | `start_to_close=30s`, retry ×5 | Take the money; returns `pay-<order_id>` |
| `reserve_inventory` | `run`, stage `reserving_inventory` | `start_to_close=30s`, retry ×5 | Hold the item; returns `resv-<order_id>` |
| `ship_order` | `run`, stage `shipping` | `start_to_close=60s`, `heartbeat=10s`, retry ×5 | Hand to carrier; long enough to heartbeat, and the designated failure point |
| `release_inventory` | `_compensate` | `start_to_close=30s`, default retry | Undo `reserve_inventory` |
| `refund_payment` | `_compensate` | `start_to_close=30s`, default retry | Undo `charge_payment` |

## The flow

```mermaid
flowchart TD
    START([Client: starter.py start]) --> CHARGE

    CHARGE["<b>charge_payment</b><br/>stage: charging_payment"] --> RESERVE
    RESERVE["<b>reserve_inventory</b><br/>stage: reserving_inventory"] --> WINDOW

    WINDOW{"durable Timer<br/>wait_condition, 10s<br/>stage: awaiting_cancellation_window"}
    SIG>"Signal: cancel()"] -.sets _cancelled.-> WINDOW

    WINDOW -->|"cancel Signal received"| COMP
    WINDOW -->|"window expired"| SHIP

    SHIP["<b>ship_order</b><br/>stage: shipping<br/>heartbeats every ~0.2s"]
    SHIP -->|success| DONE([Completed: tracking number])
    SHIP -->|"ActivityError<br/>retries exhausted or non-retryable"| COMP

    subgraph COMP["_compensate — saga rollback, newest first"]
        direction TB
        REL["<b>release_inventory</b><br/>if reservation_id"] --> REF["<b>refund_payment</b><br/>if payment_id"]
    end

    COMP --> CANCELLED([Completed: cancelled by customer])
    COMP --> FAILED([Failed: ApplicationError, rolled back])

    QUERY>"Query: status()"] -.reads state, calls no Activity.-> WINDOW

    classDef act fill:#1f6feb22,stroke:#1f6feb,stroke-width:2px
    class CHARGE,RESERVE,SHIP,REL,REF act
```

> The two `COMP` exits are mutually exclusive per run: the cancellation branch
> ends Completed, the shipping-failure branch ends Failed.

## Sequence, happy path

```mermaid
sequenceDiagram
    autonumber
    participant C as Client<br/>(starter.py)
    participant T as Temporal Service
    participant W as OrderWorkflow<br/>(workflows.py)
    participant A as Activities<br/>(activities.py)

    C->>T: start_workflow(OrderWorkflow.run, OrderInput)
    T->>W: Workflow Task
    W->>T: Command: schedule charge_payment
    T->>A: Activity Task
    A-->>T: "pay-<id>"
    W->>T: Command: schedule reserve_inventory
    T->>A: Activity Task
    A-->>T: "resv-<id>"
    W->>T: Command: start Timer (10s)
    C-->>T: Query status() (optional, no Activity)
    T-->>W: TimerFired
    W->>T: Command: schedule ship_order
    T->>A: Activity Task (heartbeats)
    A-->>T: "trk-<id>"
    W-->>T: WorkflowExecutionCompleted
    T-->>C: tracking number
```
