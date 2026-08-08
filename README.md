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

| File | Read it for | What it does |
|---|---|---|
| `workflows.py` | Deterministic orchestration: the saga, a durable Timer, a Signal, a Query. **Start here.** | Defines `OrderWorkflow`, the state machine driving the order saga: charge payment, reserve inventory, wait out a cancellable Timer window, ship, and compensate (release inventory, refund payment) on cancellation or shipping failure. Exposes a `cancel` Signal and a `status` Query. |
| `activities.py` | The side-effecting half, plus injectable failures and compensations. | Implements the Activities the Workflow calls: `charge_payment`, `reserve_inventory`, `ship_order` (with heartbeats), and their compensations `refund_payment` / `release_inventory`. Reads `FLAKY_ACTIVITIES` and `FAIL_SHIPPING` env vars to inject transient and non-retryable failures on demand. |
| `worker.py` | Wiring code and Task Queue registration. | Connects to the local Temporal Service, registers `OrderWorkflow` and `ALL_ACTIVITIES` on the `orders` Task Queue, and long-polls for work until killed. |
| `starter.py` | Starting, querying, and signalling from outside. | CLI (`start`, `status`, `cancel`) that talks to the Temporal Service as a Client: starts a new `OrderWorkflow` run, queries a running Workflow's status, or sends it a cancel Signal. |
| `shared.py` | Types and constants shared across the Worker/Client boundary. | Defines the `TASK_QUEUE` constant and the `OrderInput` / `OrderStatus` dataclasses used as Workflow input and Query output; kept plain since anything crossing the boundary gets serialized. |
| `replay_check.py` | The pre-deploy determinism check. | CLI (`save`, `check`) that fetches and saves a completed Workflow's Event History to `histories/`, then replays saved histories against the current `workflows.py` to catch non-deterministic code changes before deploy. |
| `tests/test_order_workflow.py` | Time-skipping tests with mocked Activities. | Pytest suite covering the happy path, shipping-failure compensation, and cancel-Signal compensation, run against a time-skipping test server with Activities mocked by name. |
| `pytest.ini` | Test runner configuration. | Points pytest at the `tests/` directory, adds the repo root to `pythonpath`, and enables `asyncio_mode = auto` for the async test functions. |
| `requirements.txt` | Python dependencies. | Pins `temporalio`, `pytest`, and `pytest-asyncio` versions needed to run the Worker, Client, and test suite. |
| `WORKFLOW_MAP.md` | Diagrams: which Workflow calls which Activity, for which use case. | Reference diagrams of the saga's call graph — useful alongside `workflows.py` when tracing which Activity a given step invokes. |

---

## Setup

### Option A — GitHub Codespaces

This repo has a `.devcontainer` config. Open it with **Code → Create codespace
on main** and wait for the postCreate script to finish — it installs the
Temporal CLI and Python deps for you, so there's nothing left to set up
manually. Then skip straight to the terminal steps below.

Notes specific to Codespaces:

- `temporal` is added to `PATH` via `.bashrc`, but that only takes effect in
  *new* terminals opened after setup finishes. If a terminal reports
  `command not found`, either open a fresh terminal or use the full path:
  `~/.temporalio/bin/temporal`.
- When you start the server, Codespaces will prompt to forward port 8233 —
  use that forwarded URL for the Web UI instead of `localhost:8233`.

Start terminals 1 and 2 (see below) with:

```bash
# terminal 1
~/.temporalio/bin/temporal server start-dev --db-filename temporal.db

# terminal 2
.venv/bin/python worker.py
```

### Option B — local machine

```bash
# 1. Temporal CLI (server + Web UI in one binary)
curl -sSf https://temporal.download/cli.sh | sh    # or: brew install temporal

# 2. Python deps
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
```

### Three terminals from here on — the experiments below refer to them by number

```bash
# terminal 1 — the Temporal Service. --db-filename makes it survive restarts.
temporal server start-dev --db-filename temporal.db

# terminal 2 — your Worker
.venv/bin/python worker.py

# terminal 3 — the Client, used to start/query/signal Workflows (kept idle for now)
```

Server on `localhost:7233`. Open the Web UI now, at <http://localhost:8233>
(in Codespaces, use the forwarded-port URL instead of `localhost`), and keep
the tab open — you'll come back to it throughout the experiments below to
watch runs and inspect Event Histories.

---

## Experiment 0 — a run, end to end

With the Worker still running in terminal 2, go to terminal 3 and run:

```bash
.venv/bin/python starter.py start
```

It prints a Workflow ID and a UI link like
`http://localhost:8233/namespaces/default/workflows/order-xxxxxxxx` — open
that link in your browser now. Watch it while the command runs: it waits ~10
seconds in the cancellation window, ships, and returns a tracking number in
the terminal at the same moment the UI marks the run **Completed**.

## Experiment 1 — read the Event History

**This is the highest-value step in the whole exercise.** In the browser tab
you opened above (or `http://localhost:8233`, click into your most recent
`OrderWorkflow` run), click the **"Event History"** / **"History"** tab (or
"Details" > "History", depending on CLI version) and expand it. You should
see a numbered list of events like `WorkflowExecutionStarted`,
`ActivityTaskScheduled`, `ActivityTaskCompleted`, etc. — click each one to
expand its payload. Then, with `workflows.py` open side by side, map every
event back to a line of code:

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

You'll be starting and stopping the Worker (terminal 2) a lot in this
section. "Restart the Worker" always means: go to terminal 2, `ctrl-c` to
stop it if it's running, then run `.venv/bin/python worker.py` again — it
reconnects to the same Task Queue (`orders`) and immediately starts picking
up any Workflow that's waiting for it.

**2a. Kill the Worker mid-flight.**

In terminal 3 (Client):

```bash
.venv/bin/python starter.py start --no-wait
```

Copy the Workflow ID it prints. **Immediately** switch to terminal 2 and hit
`ctrl-c` to kill the Worker — do this within a few seconds, before it gets to
the shipping step. The Workflow is now mid-run with nothing to execute it.
Confirm that in terminal 3:

```bash
.venv/bin/python starter.py status <workflow-id>
```

This command hangs or errors — Queries need a live Worker to answer them.
Now open the Workflow in the Web UI (`http://localhost:8233`, find it by
Workflow ID) and check the status badge: it still shows **Running**, even
though nothing is polling for it. That's the proof the state lives in the
Service, not in your process.

Now go back to terminal 2 and restart the Worker (`.venv/bin/python
worker.py`). Watch the UI: within a second or two the run's status flips to
**Completed**, and terminal 3 (if you re-run the `status` command) answers
again. **Nothing was lost and you wrote no recovery code.** That's the entire
product in one demo.

Now repeat it, but kill the Worker while the run is in the 10-second
cancellation window (start a fresh run in terminal 3, `ctrl-c` the Worker in
terminal 2 immediately after), and this time leave the Worker down for a
full minute before restarting it. Watch the UI's history tab for that run:
the `TimerFired` event's timestamp lands ~10 seconds after `TimerStarted`,
not a minute later — the Timer fired on schedule inside the Service even
though no Worker was around to see it.

**2b. Watch retries.** Restart the Worker (terminal 2, `ctrl-c` then), this
time setting the flaky-activities flag:

```bash
FLAKY_ACTIVITIES=1 .venv/bin/python worker.py
```

In terminal 3, start a new run (`.venv/bin/python starter.py start`) and
while it runs, refresh the Web UI's Event History for that run. Look for one
or more `ActivityTaskFailed` events immediately followed by another
`ActivityTaskScheduled` for the same Activity — that's the SDK retrying per
the `RetryPolicy` in `workflows.py`, with the delay between attempts growing
each time (the backoff). Note the *Workflow* history has no failure in it at
the top level; your Workflow code never saw the error, only the eventual
success.

**2c. Watch the saga compensate.** Restart the Worker again (terminal 2,
`ctrl-c` then):

```bash
FAIL_SHIPPING=1 .venv/bin/python worker.py
```

In terminal 3, start a new run. This time `ship_order` raises a
non-retryable `ApplicationError`, so there's no retry loop; instead the
Workflow catches it and runs `release_inventory` then `refund_payment`, in
that reverse order, before failing. Watch this happen in the UI's Event
History: you'll see `ActivityTaskScheduled`/`Completed` pairs for
`release_inventory` and `refund_payment` appear *after* the `ship_order`
failure, then a final `WorkflowExecutionFailed`. Open this run and the 2b run
side by side (two browser tabs) and compare: 2b's failure was invisible to
your code, 2c's surfaced and drove new Activity calls.

**2d. Signals and Queries.** Restart the Worker with no env vars
(`.venv/bin/python worker.py` in terminal 2), then in terminal 3:

```bash
.venv/bin/python starter.py start --no-wait
```

Copy the Workflow ID, and *within* the 10-second window run both of these in
terminal 3:

```bash
.venv/bin/python starter.py status <workflow-id>   # Query — a synchronous read
.venv/bin/python starter.py cancel <workflow-id>   # Signal — a durable write
```

After both commands finish, open the run's Event History in the UI and look
for `WorkflowExecutionSignaled` — it's there, timestamped right after your
`cancel` call. Search the same history for anything from your `status` call:
there is nothing, at any point. That difference is the point: Queries are
reads served from replayed state and leave no trace, Signals are events that
change what replay produces.

## Experiment 3 — break determinism

Open `workflows.py` in your editor, find the block commented
`EXPERIMENT 3` (inside `OrderWorkflow.run`, near the top), and uncomment the
three lines (`import datetime` and the `workflow.logger.info(...)` call).
Save the file, restart the Worker (terminal 2: `ctrl-c`, then
`.venv/bin/python worker.py`), and start a run from terminal 3
(`.venv/bin/python starter.py start`). It fails almost immediately — look at
terminal 2's Worker log for a sandbox violation error naming `datetime.now`.
That sandbox is a safety net for the common cases, not a proof. When you're
done, comment the block back out and restart the Worker again so later
experiments run against clean code.

The failure mode that actually bites in production is subtler: changing
Workflow code while old runs are still in flight. Simulate it from terminal
3 (Worker running normally, no env vars):

```bash
# 1. run one to completion, then record its history
.venv/bin/python starter.py start
.venv/bin/python replay_check.py save <workflow-id>

# 2. it replays cleanly against current code
.venv/bin/python replay_check.py check
```

You should see `OK   <workflow-id>.json` printed. Now open `workflows.py`
and swap the order of the `charge_payment` and `reserve_inventory` blocks
(the two `await workflow.execute_activity(...)` calls near the top of
`run`) — cut/paste one above the other so inventory is reserved before
payment is charged. Save the file, then re-run the check (no need to restart
anything — `replay_check.py` reads the file itself):

```bash
.venv/bin/python replay_check.py check
```

This time it fails with a non-determinism error printed in terminal 3: the
recorded history says payment was scheduled first, the new code says
inventory. Read the error message — it names the mismatched Command. **Run
`replay_check.py` in CI.** The real fixes for shipping such a change are
`workflow.patched()` for in-flight runs, or Worker Versioning to pin old runs
to old Workers. When you're done, revert the swap in `workflows.py` (put
`charge_payment` back first) so later experiments match the README.

## Experiment 4 — tests that skip time

No Worker or Temporal Service needed for this one — the test framework spins
up its own in-memory server. You can even stop terminal 1 and 2 if you want.
From the repo root:

```bash
.venv/bin/python -m pytest -q
```

You should see 3 tests pass in a couple seconds.
`tests/test_order_workflow.py` mocks Activities by name and runs against a
time-skipping test server. To see the time-skipping itself: open
`workflows.py`, find `CANCELLATION_WINDOW = timedelta(seconds=10)` near the
top, change it to `timedelta(days=30)`, save, and re-run `pytest -q` — the
tests still pass in about a second, even though the Workflow they're testing
now waits a full month before shipping. Timers are fast-forwarded, which is
what makes month-long Workflows testable at all. Revert the change back to
`timedelta(seconds=10)` afterward.

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
