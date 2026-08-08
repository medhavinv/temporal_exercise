# Learning Temporal by breaking it

A small order-fulfillment saga in Python, built to be *interfered with*. Reading
about durable execution doesn't stick; killing a Worker mid-Workflow and
watching the Workflow resume does.

Work through the experiments in order. Budget about two hours.

---

## The one idea

Temporal is **durable execution**. Your Workflow function's progress is
persisted as an **Event History**, so it survives process crashes, deploys, and
machine loss. It resumes by **replaying** that history through your code to
rebuild in-memory state, then continues from where it stopped.

Two consequences explain nearly every rule you'll hit:

1. **Workflow code must be deterministic.** Same history in, same decisions out.
   No `datetime.now()`, no `random`, no I/O, no `asyncio.sleep`. The SDK
   provides deterministic replacements (`workflow.now()`, `workflow.sleep()`).
2. **Activities hold everything else.** They're retried automatically and are
   *at-least-once*, so they must be idempotent.

## The pieces

Your side:

| Component | What it is |
|---|---|
| **Client** (`starter.py`) | Starts Workflows, sends Signals, runs Queries. |
| **Worker** (`worker.py`) | *Your* process, running *your* code. Long-polls a Task Queue. Temporal never executes your code — it hands out tasks and stores results. |

Temporal Service side — four independently scalable roles:

| Service | Responsibility |
|---|---|
| **Frontend** | Stateless gRPC gateway; routing, rate limiting, auth. |
| **History** | The core. Owns Event Histories (sharded), drives each Workflow's state machine, Timers, and retries. |
| **Matching** | Owns Task Queues; holds tasks until a Worker polls. |
| **Worker** (internal) | System Workflows: archival, visibility indexing, scans. |
| **Persistence** | Cassandra / PostgreSQL / MySQL, plus an optional Elasticsearch visibility store. |

One round trip:

```
client → Frontend → History appends WorkflowExecutionStarted
       → Matching → your Worker polls a WORKFLOW TASK
       → SDK replays history → your code returns COMMANDS ("schedule activity X")
       → History persists them → Matching → Worker runs the ACTIVITY TASK
       → result appended to history → next Workflow Task → ...
```

## The files

| File | Read it for |
|---|---|
| `workflows.py` | Deterministic orchestration: the saga, a durable Timer, a Signal, a Query. **Start here.** |
| `activities.py` | The side-effecting half, plus injectable failures and compensations. |
| `worker.py` | Wiring code and Task Queue registration. |
| `starter.py` | Starting, querying, and signalling from outside. |
| `tests/test_order_workflow.py` | Time-skipping tests with mocked Activities. |
| `replay_check.py` | The pre-deploy determinism check. |
| `WORKFLOW_MAP.md` | Diagrams: which Workflow calls which Activity, for which use case. |

---

## Setup

```bash
# 1. Temporal CLI (server + Web UI in one binary)
curl -sSf https://temporal.download/cli.sh | sh    # or: brew install temporal

# 2. Python deps
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

Two terminals from here on:

```bash
# terminal 1 — the Temporal Service. --db-filename makes it survive restarts.
temporal server start-dev --db-filename temporal.db

# terminal 2 — your Worker
.venv/bin/python worker.py
```

Server on `localhost:7233`, Web UI on <http://localhost:8233>.

---

## Experiment 0 — a run, end to end

```bash
.venv/bin/python starter.py start
```

It prints a Workflow ID and a UI link, waits ~10 seconds in the cancellation
window, ships, and returns a tracking number.

## Experiment 1 — read the Event History

**This is the highest-value step in the whole exercise.** Open the run in the
Web UI and expand the history. Then map every event back to a line of
`workflows.py`:

- `WorkflowExecutionStarted` — your `start_workflow` call.
- `WorkflowTaskScheduled` / `Started` / `Completed` — one *slice* of your
  Workflow code executing. There are several; find where each one begins and
  ends in the source.
- `ActivityTaskScheduled` — the Command your code emitted at
  `execute_activity`. Note it's *scheduled*, not called: your Workflow only
  ever asks the Service to do things.
- `ActivityTaskStarted` / `Completed` — a Worker actually ran it. The result
  payload is right there in the event.
- `TimerStarted` / `TimerFired` — your `wait_condition` timeout.
- `WorkflowExecutionCompleted` — the return value.

Ask yourself: *if I deleted my Worker process and its memory entirely, could I
rebuild `self._payment_id` from this list alone?* That's replay.

## Experiment 2 — break things on purpose

**2a. Kill the Worker mid-flight.**

```bash
.venv/bin/python starter.py start --no-wait
```

Immediately `ctrl-c` the Worker in terminal 2. The Workflow is now mid-run with
nothing to execute it. Check it's still alive:

```bash
.venv/bin/python starter.py status <workflow-id>
```

The Query fails — Queries need a Worker. But the UI shows the Workflow is very
much *Running*. Now restart the Worker: it picks up where it left off, replays
the history to rebuild state, and finishes. **Nothing was lost and you wrote no
recovery code.** That's the entire product in one demo.

Now do it again during the 10-second Timer window, and leave the Worker down
for a minute. The Timer still fires on time — Timers live in the Service, not
in your process.

**2b. Watch retries.** Restart the Worker with flaky Activities:

```bash
FLAKY_ACTIVITIES=1 .venv/bin/python worker.py
```

Start a run and watch the history: `ActivityTaskFailed` followed by another
`ActivityTaskScheduled`, backing off per the `RetryPolicy` in `workflows.py`.
The *Workflow* history stays clean — retries are the Service's job. Note that
your Workflow code never saw the failure.

**2c. Watch the saga compensate.**

```bash
FAIL_SHIPPING=1 .venv/bin/python worker.py
```

`ship_order` raises a non-retryable `ApplicationError`, so no retries happen;
the Workflow catches it and runs `release_inventory` then `refund_payment` in
reverse order before failing. Compare the two failure modes in the UI: 2b
retried invisibly, 2c surfaced into your code.

**2d. Signals and Queries.** Start a run with `--no-wait`, then during the
10-second window:

```bash
.venv/bin/python starter.py status <workflow-id>   # Query — a synchronous read
.venv/bin/python starter.py cancel <workflow-id>   # Signal — a durable write
```

The Signal lands in the history as `WorkflowExecutionSignaled`; the Query does
not appear at all. That difference is the point: Queries are reads served from
replayed state, Signals are events that change what replay produces.

## Experiment 3 — break determinism

Uncomment the `datetime.now()` block marked `EXPERIMENT 3` in `workflows.py`.
Restart the Worker and start a run — the sandbox rejects it immediately. That
sandbox is a safety net for the common cases, not a proof.

The failure mode that actually bites in production is subtler: changing
Workflow code while old runs are still in flight. Simulate it:

```bash
# 1. record a completed run
.venv/bin/python replay_check.py save <workflow-id>

# 2. it replays cleanly against current code
.venv/bin/python replay_check.py check

# 3. now edit workflows.py — swap the order of charge_payment and
#    reserve_inventory — and re-run the check
.venv/bin/python replay_check.py check
```

Step 3 fails with a non-determinism error: the recorded history says payment
was scheduled first, the new code says inventory. **Run `replay_check.py` in
CI.** The real fixes for shipping such a change are `workflow.patched()` for
in-flight runs, or Worker Versioning to pin old runs to old Workers.

## Experiment 4 — tests that skip time

```bash
.venv/bin/python -m pytest -q
```

`tests/test_order_workflow.py` mocks Activities by name and runs against a
time-skipping test server. Change `CANCELLATION_WINDOW` in `workflows.py` to
`timedelta(days=30)` — the tests still pass in about a second. Timers are
fast-forwarded, which is what makes month-long Workflows testable at all.

> Heads up: the first run downloads a test-server binary from
> `temporal.download`. In a sandboxed or air-gapped environment that download
> is blocked and all three tests fail on `Failed starting test server` — that's
> the network, not the code.

## Experiment 5 — pick it apart yourself

- Make `charge_payment` non-idempotent (append to a module-level list, print
  the length). Run it flaky. Now you understand why at-least-once matters.
- Set `maximum_attempts=1` and compare the failure in the history.
- Add a `@workflow.update` handler with a validator that rejects an amount
  change after shipping — Updates are the read-write, rejectable sibling of
  Signals.
- Replace the Timer with a Child Workflow and see how the parent's history
  records it.
- Loop the Workflow a few thousand times and hit the history-size warning, then
  fix it with `workflow.continue_as_new()`.

---

## Where to read next

Now that you have hooks to hang it on:

- [How Temporal works](https://docs.temporal.io/encyclopedia/architecture/how-temporal-works) — the encyclopedia entry the rest of the docs assume.
- [Temporal Service architecture](https://docs.temporal.io/temporal-service) — the four services in depth.
- [learn.temporal.io](https://learn.temporal.io) — free 101/102 courses, Python track included.
- [Python SDK developer guide](https://docs.temporal.io/develop/python) — Signals, Queries, Updates, Child Workflows, versioning.

## The rough edges, honestly

- **Versioning long-running Workflows** across deploys is the hard part of
  operating Temporal. Nothing else comes close.
- **Payload and history limits** — 2 MB per payload, 50 MB / ~50k events per
  history. Pass references, not blobs; use `continue_as_new` for long loops.
- **Determinism rules are learned by violating them.** Which is why this repo
  is arranged the way it is.
