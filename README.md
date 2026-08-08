# Learning Temporal by breaking it

A self-hosted Temporal lab you can run in a Codespace. An order-fulfillment
saga plus one small Workflow per concept, wired to a single driver script so
each idea is one command away.

Reading about durable execution doesn't stick. Killing a Worker mid-Workflow
and watching the Workflow resume does.

---

## Start here (GitHub Codespaces)

Open this repo in a Codespace. The container builds itself: Python deps, and a
Temporal CLI that embeds a complete server. Then:

```bash
./lab.sh up      # starts the Temporal server and a Worker
./lab.sh         # lists every experiment
```

Open the **PORTS** tab and click the forwarded port **8233** — that's the
Temporal Web UI in your browser. Keep it open; half the value of this repo is
reading Event Histories there while the experiments run.

<details>
<summary>Running locally instead</summary>

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./bootstrap_server.sh    # gets a Temporal CLI into ./bin
./lab.sh up
```

`bootstrap_server.sh` tries the official installer first and falls back to
building the CLI from the Go module proxy, which is the path that works in
sandboxes where `temporal.download` and `github.com` are blocked.
</details>

---

## The one idea

Temporal is **durable execution**. Your Workflow function's progress is
persisted as an **Event History**, so it survives crashes, deploys, and machine
loss. It resumes by **replaying** that history through your code to rebuild
in-memory state, then continues from where it stopped.

Two consequences explain nearly every rule you will hit:

1. **Workflow code must be deterministic.** Same history in, same decisions out.
   No `datetime.now()`, no `random`, no I/O, no `asyncio.sleep`. The SDK gives
   you deterministic replacements.
2. **Activities hold everything else.** They're retried automatically and are
   *at-least-once*, so they must be idempotent.

## The pieces

Your side:

| Component | What it is |
|---|---|
| **Client** (`starter.py`) | Starts Workflows, sends Signals, runs Queries, Updates. |
| **Worker** (`worker.py`) | *Your* process running *your* code, long-polling a Task Queue. Temporal never executes your code — it hands out tasks and stores results. |

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
       → result appended → next Workflow Task → ...
```

---

## Concept coverage

Every entry is runnable: `./lab.sh <command>`. The **Exp.** column is the
experiment that covers it, so you can jump straight to whichever concept you
came for — or find out which experiment you have already done.

| Concept | Exp. | Command | Where it lives |
|---|---|---|---|
| Workflows, Activities, Task Queues | [0](#0-a-run-end-to-end) | `order` | `workflows.py`, `activities.py` |
| **Event History** | [1](#1-read-the-event-history) | `history` | read it in the Web UI too |
| Crash durability | [2a](#2-break-things-on-purpose) | `crash` | the flagship demo |
| **Durable Timers** | [2a](#2-break-things-on-purpose) | `crash` | `wait_condition(timeout=…)` |
| Retries & backoff | [2b](#2-break-things-on-purpose) | `retries` | `RetryPolicy` |
| Saga / compensation | [2c](#2-break-things-on-purpose) | `compensation` | `OrderWorkflow._compensate` |
| **Signal** | [2d](#2-break-things-on-purpose) | `signal-query` | `@workflow.signal` |
| **Query** | [2d](#2-break-things-on-purpose) | `signal-query` | `@workflow.query` |
| **Replay** & non-determinism | [3](#3-break-determinism) | `replay-break` | `replay_break.py` |
| The Workflow sandbox | [3](#3-break-determinism) | `variant broken-determinism` | `variants.py` |
| Time-skipping tests, mocked Activities | [4](#4-tests-that-skip-time) | `test` | `tests/` |
| Replay tests for CI | [4](#4-tests-that-skip-time) | `replay-check` | `replay_check.py` |
| Idempotency / at-least-once | [5](#5-pick-it-apart-yourself) | `double-charge` | `charge_payment` |
| Retry policy tuning | [5](#5-pick-it-apart-yourself) | `variant no-retries` | `variants.py` |
| **Update** + validator | [5](#5-pick-it-apart-yourself) | `update` | `CounterWorkflow` |
| **Child Workflows** | [5](#5-pick-it-apart-yourself) | `child` | `ParentWorkflow` |
| **Continue-As-New** | [5](#5-pick-it-apart-yourself) | `continue-as-new` | `CounterWorkflow` |
| Deterministic `now`/`uuid4`/`random` | [6](#6-one-concept-at-a-time-new) | `determinism` | `DeterminismWorkflow` |
| **Cancellation** + cleanup | [6](#6-one-concept-at-a-time-new) | `cancellation` | `CancellationWorkflow` |
| Heartbeats | [6](#6-one-concept-at-a-time-new) | `cancellation` | `activity.heartbeat` |
| Local Activities | [6](#6-one-concept-at-a-time-new) | `local-activity` | `LocalActivityWorkflow` |
| **Versioning** (`patched`) | [6](#6-one-concept-at-a-time-new) | `versioning` | `VersionedWorkflow` |
| Search Attributes, Memos, Visibility | [6](#6-one-concept-at-a-time-new) | `searchable` | `SearchableWorkflow` |
| **Schedules** (pause, trigger, backfill) | [6](#6-one-concept-at-a-time-new) | `schedule` | `scheduling.py` |
| Start Delay | [6](#6-one-concept-at-a-time-new) | `schedule-delay` | `scheduling.py` |
| Cron (legacy) | [6](#6-one-concept-at-a-time-new) | `schedule-cron` | `scheduling.py` |

Not covered, deliberately: Nexus, multi-cluster replication, custom Data
Converters and payload encryption, interceptors, and Worker Versioning
(the heavier successor to `patched`). Each is a topic on its own once the
above is comfortable.

---

## You never have to edit code

Every variation is a flag. Nothing in this repo asks you to uncomment a line.

**Workflow behaviour — variants** (`./lab.sh variants` to list them):

| Variant | What changes | Command |
|---|---|---|
| `default` | 10s window, 5 attempts, compensation on | `./lab.sh variant default` |
| `long-window` | A **30-day** cancellation Timer | `./lab.sh variant long-window` |
| `no-retries` | `maximum_attempts=1` — first failure is final | `./lab.sh variant no-retries` |
| `no-compensation` | Skip the saga rollback; customer stays charged | `./lab.sh variant no-compensation` |
| `broken-determinism` | Calls `datetime.now()` in Workflow code | `./lab.sh variant broken-determinism` |

Or drive them directly: `python starter.py start --variant no-retries`.

**Activity behaviour — Worker flags** (`python worker.py --help`):

| Flag | What it injects |
|---|---|
| `--flaky` | Every Activity fails its first two attempts |
| `--fail-shipping` | `ship_order` fails non-retryably |
| `--non-idempotent` | `charge_payment` charges again on every retry |

`./lab.sh double-charge` combines the last two ideas: a non-idempotent Activity
meeting a retry, charging one customer three times for one order.

### Why the split? It's the determinism rule again

This isn't arbitrary tidiness — it's the same constraint the whole system runs on:

- **Workflow** variants travel as Workflow **input**, recorded in the Event
  History at `WorkflowExecutionStarted`. Replay reads the same recorded bytes
  forever, so branching on them is safe. Reading the same setting from an
  environment variable inside Workflow code would break replay the moment a
  Worker restarted with a different value.
- **Activity** behaviour can be a process flag, because Activities are never
  replayed. They run once per attempt, and nothing about them has to be
  reproducible.

`variants.py` spells this out at length; it's worth reading before the Workflow
itself.

---

## The experiments, in order

> **Returning from an earlier version of this repo?** The numbering is
> unchanged — Experiments 0–5 are the same exercises they always were, and
> only the commands got shorter. If you finished **Experiment 2**, pick up at
> [Experiment 3](#3-break-determinism). Experiment 6 is new material appended
> at the end, so it disturbs nothing before it.
>
> What changed: the old three-terminal dance
> (`temporal server start-dev` / `python worker.py` / `starter.py`) is now
> `./lab.sh up` once, then one command per experiment. Nothing asks you to
> edit a file any more.

### 0. A run, end to end

```bash
./lab.sh order
```

### 1. Read the Event History

**The highest-value step in the whole repo.** Open the run in the Web UI (port
8233), expand the history, and map every event to a line of `workflows.py`:

- `WorkflowExecutionStarted` — your `start_workflow` call.
- `WorkflowTaskScheduled/Started/Completed` — one *slice* of your Workflow code
  executing. There are several. Find where each begins and ends in the source.
- `ActivityTaskScheduled` — the Command your code emitted at
  `execute_activity`. Note *scheduled*, not called: your Workflow only ever
  asks the Service to do things.
- `ActivityTaskStarted/Completed` — a Worker actually ran it.
- `TimerStarted/TimerFired` — your `wait_condition` timeout.

Ask: *if I deleted my Worker and all its memory, could I rebuild
`self._payment_id` from this list alone?* That's replay.

### 2. Break things on purpose

Same four sub-experiments as before, one command each. Each one restarts the
Worker with the right flags and puts it back to normal afterwards.

**2a. Kill the Worker mid-flight.**

```bash
./lab.sh crash
```

The one that matters. The Workflow stays `RUNNING` with no Worker alive at
all, its 10-second Timer fires while nothing of yours is running, and when a
Worker returns it finishes. **You wrote no recovery code.**

**2b. Watch retries.**

```bash
./lab.sh retries
```

A genuine surprise, and a correction to what earlier versions of this README
said: **retries produce no events**. There is no `ActivityTaskFailed` for a
retried-then-successful Activity. Temporal records `ActivityTaskScheduled`
once, then only the *terminal* attempt — the one that finally succeeds, or the
last one if retries are exhausted — as a single `ActivityTaskStarted` /
`ActivityTaskCompleted` pair. Attempts in between live in the Service's
mutable state, not the history.

So the history of a flaky Activity is as compact as a clean one. The giveaway
is the `attempt` counter on `ActivityTaskStarted` reading `3` instead of `1`,
plus its `lastFailure` field. To *watch* retries happen rather than infer
them, tail the Worker log (`.run/worker.log`): each failed attempt is logged
there with its traceback and a growing backoff between them.

**2c. Watch the saga compensate.**

```bash
./lab.sh compensation
```

`ship_order` raises a non-retryable `ApplicationError`, so no retries happen;
the Workflow catches it and runs `release_inventory` then `refund_payment` in
reverse order before failing. Compare the two failure modes: 2b retried
invisibly, 2c surfaced into your code.

To see what the saga is actually buying you, run the same failure with the
rollback switched off:

```bash
./lab.sh variant no-compensation
```

`compensations_run` comes back empty — the customer is charged and the stock
is still held, for an order that will never ship.

**2d. Signals and Queries.**

```bash
./lab.sh signal-query
```

Starts a run, Queries it mid-flight, sends the `cancel` Signal, and Queries
again. The Signal lands in the history as `WorkflowExecutionSignaled`; the
Query does not appear at all. That difference is the point: Queries are reads
served from replayed state, Signals are events that change what replay
produces.

### 3. Break determinism

```bash
./lab.sh replay-break
```

Records a fresh run, replays it against the real code (passes), then against a
copy with two Activities swapped (fails with a non-determinism error). It works
on an in-memory copy, so `workflows.py` is never modified.

For the sandbox check, no editing either:

```bash
./lab.sh variant broken-determinism
```

That runs a Workflow that calls `datetime.now()` in Workflow code. The sandbox
rejects it, the Workflow Task fails on a loop, and the run sits in `RUNNING`
making no progress — what a bad deploy actually looks like. Note the contrast
with Activity retries: a failing *Workflow* Task **is** recorded, as
`WorkflowTaskFailed` events.

### 4. Tests that skip time

```bash
./lab.sh test
```

Eleven tests: Activities mocked by name, so orchestration is the unit under
test rather than the side effects. `pytest` uses a **time-skipping** test
server by default, which fast-forwards Timers.

To see the time-skipping itself — which used to mean editing
`CANCELLATION_WINDOW` — there is now a test that does it for you. The
`long-window` variant parks on a **30-day** Timer and the test still finishes
in milliseconds. It skips itself when no time-skipping server is available:

```
tests/test_order_workflow.py::test_long_window_variant_resolves_instantly_under_time_skipping
```

`./lab.sh test` points the suite at your already-running server instead of
downloading the test-server binary (see `tests/conftest.py`) — that's what to
do when `temporal.download` is blocked. The 30-day test is the one that skips
in that mode, for the obvious reason.

### 5. Pick it apart yourself

The old version of this experiment was a list of code edits. Each one is now a
command:

| Try this | Command | What you should notice |
|---|---|---|
| Make `charge_payment` non-idempotent and run it flaky | `./lab.sh double-charge` | One customer charged **three times** for one order. At-least-once is not exactly-once. |
| Set `maximum_attempts=1` and compare the failure | `./lab.sh variant no-retries` | The first injected failure is terminal. Same Activities, same failures, different `RetryPolicy`. |
| Add an Update handler with a validator | `./lab.sh update` | The rejected Update leaves **nothing** in the history and does not change state. |
| Replace the Timer with a Child Workflow | `./lab.sh child` | `StartChildWorkflowExecutionInitiated` pairs in the parent, plus separate Workflows in `workflow list`. |
| Grow the history until it needs `continue_as_new` | `./lab.sh continue-as-new` | Same Workflow ID, brand-new Run ID, state carried across, history reset to empty. |

### 6. One concept at a time (new)

Everything past this point is material the earlier version didn't have. Small,
single-purpose Workflows in `concepts.py` — read each one before running its
command; they're short and the comments carry the reasoning.

```bash
./lab.sh determinism       # workflow.now / uuid4 / random, stable across replay
./lab.sh cancellation      # real cancellation + shielded cleanup
./lab.sh local-activity    # a marker instead of the scheduled/started pair
./lab.sh versioning        # workflow.patched() for in-flight deploys
./lab.sh searchable        # Search Attributes, Memos, Visibility queries
./lab.sh schedule          # create, describe, trigger, pause, backfill, delete
./lab.sh schedule-delay    # one-shot deferred start
./lab.sh schedule-cron     # the legacy path, for comparison
```

(`update`, `child` and `continue-as-new` live in `concepts.py` too — they're
listed in Experiment 5 above because they were on the old "pick it apart"
list.)

---

## Three traps this repo hit for real

These aren't hypothetical; each one broke something here first.

**Initialise state in `__init__`, not in `run()`.** Signal and Update handlers
can execute *before* the first line of your `@workflow.run` method. A counter
that set `self._total = start` at the top of `run()` silently discarded an
Update that arrived in the first Workflow Task. `@workflow.init` is the fix —
see `CounterWorkflow`.

**Drain handlers before Continue-As-New.** Continue-As-New ends the run
immediately, so an Update still executing is lost and its caller sees a failure.
`await workflow.wait_condition(workflow.all_handlers_finished)` first.

**A Worker runs the code it imported, not the code on disk.** Edit a Workflow
and forget to restart the Worker and you get histories that don't match your
source — which then fail replay in confusing ways. Two Workers on one Task Queue
with different code is the same problem, and is exactly what Worker Versioning
exists to solve.

## The rough edges, honestly

- **Versioning long-running Workflows** across deploys is the hard part of
  operating Temporal. Nothing else is close.
- **Payload and history limits** — 2 MB per payload, ~50 MB / 50k events per
  history. Pass references, not blobs; `continue_as_new` for long loops.
- **Determinism rules are learned by violating them**, which is why this repo is
  arranged the way it is.

## Where to read next

- [How Temporal works](https://docs.temporal.io/encyclopedia/architecture/how-temporal-works)
- [Temporal Service architecture](https://docs.temporal.io/temporal-service)
- [Python SDK developer guide](https://docs.temporal.io/develop/python)
- [learn.temporal.io](https://learn.temporal.io) — free 101/102 courses

## Files

| File | Read it for | What it does |
|---|---|---|
| `workflows.py` | The saga. **Start here.** | Defines `OrderWorkflow`: charge payment, reserve inventory, wait out a cancellable Timer window, ship, and compensate (release inventory, refund payment) on cancellation or shipping failure. Exposes a `cancel` Signal and a `status` Query. |
| `activities.py` | The side-effecting half. | `charge_payment`, `reserve_inventory`, `ship_order` (with heartbeats), and the compensations `refund_payment` / `release_inventory`. Failures are injected with Worker flags (`--flaky`, `--fail-shipping`, `--non-idempotent`), never by editing this file. |
| `variants.py` | Selectable Workflow behaviours. | The named variants and, more usefully, a long explanation of why they travel as Workflow input rather than environment variables. Read this before `workflows.py`. |
| `concepts.py` | One tiny Workflow per concept. | Update + validator, Child Workflows, Continue-As-New, real cancellation with shielded cleanup, deterministic `now`/`uuid4`/`random`, Local Activities, `patched` versioning, Search Attributes and Memos. Heavily commented — read before running. |
| `worker.py` | Wiring and Task Queue registration. | Connects to the Service, registers every Workflow and Activity on the `orders` Task Queue, and long-polls until killed. |
| `starter.py` | The Client, for the saga. | `start`, `status`, `cancel` — start a run, Query a running one, or send it a cancel Signal. |
| `concepts_client.py` | The Client, for the concepts. | One subcommand per concept, each printing what to look for in the resulting history. |
| `scheduling.py` | All three scheduling mechanisms. | The Schedules API (create, describe, trigger, pause, backfill, delete), legacy cron, and one-shot start delay. |
| `shared.py` | Types crossing the Worker/Client boundary. | `TASK_QUEUE` plus the `OrderInput` / `OrderStatus` dataclasses; kept plain because everything here gets serialized. |
| `replay_check.py` | The pre-deploy determinism check. | `save` fetches a completed Workflow's history to disk; `check` replays saved histories against current code. This is the thing to run in CI. |
| `replay_break.py` | The non-determinism demonstration. | Replays a history against a modified *copy* of the module, so it can show the failure without ever writing to `workflows.py`. |
| `lab.sh` | Every experiment, one command each. | Starts/stops the server and Worker, and drives each experiment. Run it with no arguments for the list. |
| `bootstrap_server.sh` | Getting a Temporal CLI. | Tries the official installer, then falls back to building the CLI from the Go module proxy for locked-down networks. Installs into `./bin`. |
| `tests/` | Testing Workflows. | Nine tests with Activities mocked by name, against a time-skipping server (or a live one — see `tests/conftest.py`). |
| `WORKFLOW_MAP.md` | Call-graph diagrams for the saga. | Which Workflow calls which Activity, for which use case — useful alongside `workflows.py`. |
