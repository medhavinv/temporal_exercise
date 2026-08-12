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

| Concept | What it is | Exp. | Command | Code |
|---|---|---|---|---|
| Workflows, Activities, Task Queues | Orchestration code, its side-effecting steps, and the queue Workers poll for both | [0](#0-a-run-end-to-end) | `order` | `workflows.py`, `activities.py` |
| **Event History** | The append-only log of everything that happened to a Workflow, viewable in the Web UI | [1](#1-read-the-event-history) | `history` | read it in the Web UI too |
| Crash durability | Temporal resumes a Workflow after the Worker running it dies | [2a](#2-break-things-on-purpose) | `crash` | the flagship demo |
| **Durable Timers** | Sleeps that live server-side, so they outlast any Worker | [2a](#2-break-things-on-purpose) | `crash` | `wait_condition(timeout=…)` |
| Retries & backoff | Failed Activities are re-run automatically with growing delays | [2b](#2-break-things-on-purpose) | `retries` | `RetryPolicy` |
| Saga / compensation | Undo steps you write yourself to roll back a half-finished Workflow | [2c](#2-break-things-on-purpose) | `compensation` | `OrderWorkflow._compensate` |
| **Signal** | A fire-and-forget message that changes a running Workflow's state | [2d](#2-break-things-on-purpose) | `signal-query` | `@workflow.signal` |
| **Query** | A synchronous read of a running Workflow's state; never mutates it | [2d](#2-break-things-on-purpose) | `signal-query` | `@workflow.query` |
| **Replay** & non-determinism | Re-running your code against recorded history, which must produce identical decisions | [3](#3-break-determinism) | `replay-break` | `replay_break.py` |
| The Workflow sandbox | A runtime guard that blocks nondeterministic calls inside Workflow code | [3](#3-break-determinism) | `variant broken-determinism` | `variants.py` |
| Time-skipping tests | A test server that fast-forwards Timers, so a 30-day wait runs instantly | [4](#4-tests-that-skip-time) | `test` | `tests/` |
| Replay tests for CI | Replaying saved histories against new code to catch breaking changes before deploy | [4](#4-tests-that-skip-time) | `replay-check` | `replay_check.py` |
| Idempotency / at-least-once | Activities can run more than once, so repeating one must be harmless | [5](#5-pick-it-apart-yourself) | `double-charge` | `charge_payment` |
| Retry policy tuning | Attempt limits and backoff, configured per Activity | [5](#5-pick-it-apart-yourself) | `variant no-retries` | `variants.py` |
| **Update** + validator | A message that changes state *and* returns a value, and can be rejected before it lands | [5](#5-pick-it-apart-yourself) | `update` | `CounterWorkflow` |
| **Child Workflows** | A Workflow started and awaited by another, with its own ID and history | [5](#5-pick-it-apart-yourself) | `child` | `ParentWorkflow` |
| **Continue-As-New** | Restart a Workflow with an empty history, keeping its ID and carrying state forward | [5](#5-pick-it-apart-yourself) | `continue-as-new` | `CounterWorkflow` |
| Deterministic `now`/`uuid4`/`random` | SDK replacements for the clock and randomness that return the same values on replay | [6](#6-one-concept-at-a-time-new) | `determinism` | `DeterminismWorkflow` |
| **Cancellation** + cleanup | Stopping a Workflow while still letting its cleanup run | [6](#6-one-concept-at-a-time-new) | `cancellation` | `CancellationWorkflow` |
| Heartbeats | Progress pings that let Temporal notice a dead Activity quickly | [6](#6-one-concept-at-a-time-new) | `cancellation` | `activity.heartbeat` |
| Local Activities | Short Activities run inside the Worker, skipping the Task Queue round trip | [6](#6-one-concept-at-a-time-new) | `local-activity` | `LocalActivityWorkflow` |
| **Versioning** (`patched`) | Changing Workflow code without breaking runs already in flight | [6](#6-one-concept-at-a-time-new) | `versioning` | `VersionedWorkflow` |
| Search Attributes, Memos, Visibility | Indexed fields that let you find Workflows without knowing their IDs | [6](#6-one-concept-at-a-time-new) | `searchable` | `SearchableWorkflow` |
| **Schedules** | First-class recurring runs you can pause, trigger early, and backfill | [6](#6-one-concept-at-a-time-new) | `schedule` | `scheduling.py` |
| Start Delay | Run a Workflow once, later | [6](#6-one-concept-at-a-time-new) | `schedule-delay` | `scheduling.py` |
| Cron (legacy) | Older recurring execution set on the Workflow; cannot pause or backfill | [6](#6-one-concept-at-a-time-new) | `schedule-cron` | `scheduling.py` |

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

Each experiment below is written the same way: **the concept** it demonstrates,
**what actually runs**, and **where to look** — terminal, Web UI, or Worker log
— with what the thing you find there means. Read the "where to look" part
*before* running the command; most of these produce output that looks
unremarkable until you know which line is the point.

### 0. A run, end to end

```bash
./lab.sh order
```

**The concept — Workflow, Activity, Task Queue.** The Workflow is orchestration
code and never touches the outside world itself; it emits Commands, and a
Worker polling the `orders` Task Queue runs the Activities that do the real
work. Temporal stores and routes, it never executes your code.

**What runs.** One `OrderWorkflow`: charge payment → reserve inventory → wait
out a 10s cancellation Timer → ship. It completes.

**Where to look.** The terminal prints the **Workflow ID** (yours, chosen by
the Client) and the **Run ID** (Temporal's, for this one attempt at it) plus a
Web UI link. Open it — a `COMPLETED` execution. That run is the raw material
for Experiment 1, so leave the tab open.

### 1. Read the Event History

**The highest-value step in the whole repo.**

**The concept — the Event History.** This append-only log *is* the Workflow's
state. There is no other copy: your `self._payment_id` exists only because it
can be rebuilt from these events. Everything else in Temporal follows from
that.

**Where to look.** Open the run in the Web UI (port 8233), expand the history,
and map every event back to a line of `workflows.py`:

- `WorkflowExecutionStarted` — your `start_workflow` call. Its
  `input` field holds the variant, recorded permanently.
- `WorkflowTaskScheduled/Started/Completed` — one *slice* of your Workflow code
  executing. There are several. Find where each begins and ends in the source.
- `ActivityTaskScheduled` — the Command your code emitted at
  `execute_activity`. Note *scheduled*, not called: your Workflow only ever
  asks the Service to do things.
- `ActivityTaskStarted/Completed` — a Worker actually ran it.
- `TimerStarted/TimerFired` — your `wait_condition` timeout.

**What it means.** Ask: *if I deleted my Worker and all its memory, could I
rebuild `self._payment_id` from this list alone?* If yes, the Workflow is
durable. That reconstruction is replay, and Experiment 3 is what happens when
it goes wrong.

### 2. Break things on purpose

Four sub-experiments, one command each. Each one restarts the Worker with the
right flags and puts it back to normal afterwards.

**2a. Kill the Worker mid-flight.**

```bash
./lab.sh crash
```

**The concept — durable execution and durable Timers.** Workflow progress lives
server-side, not in your process. A Timer is a row in the History service, not
a `sleep` in your code, so it keeps counting while nothing of yours exists.

**What runs.** Starts a run, kills the Worker three seconds in, waits 15s with
*no Worker alive at all*, then restarts the Worker.

**Where to look.** The terminal status lines are the whole demo: `RUNNING` with
the Worker dead, still `RUNNING` after the 10s Timer fired with nothing of
yours executing, then `COMPLETED` once a Worker comes back. In the Web UI the
history has no gap and no error — `TimerFired` is recorded during the window
when you had no process at all.

**What it means.** You wrote no recovery code, no checkpointing, no retry loop.
The Timer kept counting because it lives in the History service, not in your
process.

**2b. Watch retries.**

```bash
./lab.sh retries
```

**The concept — automatic retries and `RetryPolicy`.** A failed Activity is
re-run by the Service with growing backoff, without the Workflow knowing.
Retries happen in the Service's *mutable state*, not in the history.

**What runs.** The Worker is restarted with `--flaky`, so every Activity fails
its first two attempts. The Workflow still completes.

**Where to look.** Three places:

- **Web UI** — **no `ActivityTaskFailed` events exist**. Temporal records
  `ActivityTaskScheduled` once, then only the *terminal* attempt (the one that
  finally succeeds, or the last one if retries run out) as a single
  `ActivityTaskStarted` / `ActivityTaskCompleted` pair.
- **Terminal** — the script prints the `attempt` counter and `lastFailure` for
  each `ActivityTaskStarted`. `attempt=3` is the only trace in the history that
  anything went wrong.
- **`.run/worker.log`** — to *watch* retries rather than infer them: every
  failed attempt with its traceback and a growing backoff between them.

**What it means.** A flaky Activity's history is as compact as a clean one, so
"no failure events" does not mean "no failures" — check `attempt`.

**2c. Watch the saga compensate.**

```bash
./lab.sh compensation
```

**The concept — the Saga pattern.** Temporal has no rollback. Undo is ordinary
code you write: compensating Activities, run in reverse order, in an exception
handler.

In short: Temporal's behavior doesn't change on error — it still just runs
your Workflow code to completion. What you control is what that code does
once a failure reaches it. In practice that's a `try`/`except ActivityError`
around the risky step, and compensation logic in the `except` block that
calls ordinary (compensating) Activities. It only triggers for failures that
actually reach your code — retries exhausted, or a non-retryable
`ApplicationError` — never for a retry Temporal is still working through.

**What runs.** The Worker is restarted with `--fail-shipping`, so `ship_order`
raises a **non-retryable** `ApplicationError` — no retries, the failure goes
straight into your code. `OrderWorkflow._compensate` runs `release_inventory`
then `refund_payment` before failing the Workflow.

**Where to look.** Terminal: `compensations_run` lists the two undo steps. Web
UI: `ActivityTaskScheduled` for the compensating Activities *after* the failed
`ship_order`, then `WorkflowExecutionFailed`.

**What it means.** Compare with 2b: same "failure", but a retryable one never
reaches your code, while a non-retryable one surfaces as a Python exception you
can catch. That flag is the switch between the two behaviours.

Then run the same failure with the rollback switched off:

```bash
./lab.sh variant no-compensation
```

`compensations_run` comes back empty — the customer is charged and the stock is
still held, for an order that will never ship.

**2d. Signals and Queries.**

```bash
./lab.sh signal-query
```

**The concept — Signal vs Query.** A Signal is a fire-and-forget *write* that
becomes part of the history. A Query is a synchronous *read* served from
replayed state and must never mutate anything.

**What runs.** Starts a run, Queries it mid-flight, sends the `cancel` Signal,
Queries again.

**Where to look.** Terminal shows the status changing between the two Queries.
In the Web UI, the Signal is there as `WorkflowExecutionSignaled` — and the two
Queries **do not appear at all**.

**What it means.** Anything that changes what replay produces must be recorded;
a Query changes nothing, so there is nothing to record. That's also why a Query
handler that mutates state is a bug — the mutation would only "happen" on the
machine that served it.

### 3. Break determinism

```bash
./lab.sh replay-break
```

**The concept — replay and non-determinism.** Temporal rebuilds Workflow state
by re-running your code against the recorded history. Your code must issue the
same Commands, in the same order, as the run that produced that history.

**What runs.** The Workflow runs successfully once — that's the completed run
you'll see in the dashboard — and its history is saved to JSON. Then the
`Replayer` runs twice over that same saved history: once against the real code
(right order — passes), once against an in-memory copy with two Activities
swapped (wrong order — fails).

**Where to look.** Terminal: stage 1 prints `OK`, stage 2 prints `FAILED as it
should:` with a `Nondeterminism error`. In the Web UI, open the recorded run
and find the two `ActivityTaskScheduled` events — `charge_payment` has the
lower **EventID**, `reserve_inventory` the higher. That order is baked in
permanently, and it's exactly what the swapped code violates. Note that neither
replay appears in the dashboard at all: replay is entirely offline and
client-side, so there is no second run to look for.

**What it means.** `Replayer` takes histories from real (or representative)
executions — often pulled from production — and replays them against your *new*
code offline, before you ship it. That's what `replay_check.py check` does, and
it belongs in CI. Skip it and a reordering like this hangs every Workflow
currently in flight.

For the sandbox check, no editing either:

```bash
./lab.sh variant broken-determinism
```

**The concept — the Workflow sandbox.** The SDK intercepts nondeterministic
calls inside Workflow code and refuses to run them, rather than letting you
record a history you can never replay.

**What runs.** A variant that calls `datetime.now()` in Workflow code. The
sandbox rejects it, so the Workflow Task fails and retries forever.

**Where to look.** Terminal: status stays `RUNNING` while the pending Workflow
Task's `attempt` counter climbs — running, but making no progress. The event
counts printed show `WorkflowTaskFailed` events piling up and **no Activity
ever scheduled**, because the Workflow never got past its first task.

**What it means.** Two things. First, this is what a bad deploy actually looks
like: not a crash or an alert, just Workflows that quietly stop advancing.
Second, note the contrast with 2b — a failing *Activity* Task leaves no event,
but a failing *Workflow* Task **is** recorded. Nothing is lost either: fix the
code, restart the Worker, and the next retry succeeds.

### 4. Tests that skip time

```bash
./lab.sh test
```

**The concept — time-skipping tests, and replay tests for CI.** Workflow tests
run against a test server that fast-forwards Timers instead of waiting them
out, so durable waits cost no wall-clock time. Activities are mocked by name,
which makes orchestration the unit under test rather than the side effects.

**What runs.** Eleven tests. The one to open is:

```
tests/test_order_workflow.py::test_long_window_variant_resolves_instantly_under_time_skipping
```

**Where to look.** That test uses the `long-window` variant, which parks on a
**30-day** Timer — and the suite still finishes in about a second. Check the
total runtime `pytest` prints against what those Timers "should" have cost.

**Which server you actually got.** This matters, because it decides whether the
30-day test runs at all. Time skipping comes from a dedicated test-server binary
the SDK downloads from `temporal.download` on first use; your own dev server
cannot skip time. So `./lab.sh test` tries that binary first and falls back to
the live server only if the download is unavailable:

| What happened | What you see | The 30-day test |
|---|---|---|
| Time-skipping server started | Whole suite in ~1s | **runs** — this is the point of the exercise |
| Download blocked, fell back | A `UserWarning` naming the failed URL, suite takes ~1 min | **skips itself**, with the reason printed |

`./lab.sh test` passes `-rs`, so pytest prints the reason for every skip rather
than a bare `s`. If you see the fallback warning, the 30-day test did not run —
you are reading a real-time suite, and the headline claim above is untested on
your machine. To force one mode or the other:

```bash
# never try time skipping; use a server you already have
TEMPORAL_TEST_ADDRESS=localhost:7233 .venv/bin/python -m pytest -q -rs

# no fallback: fail loudly instead of quietly running in real time
.venv/bin/python -m pytest -q -rs
```

**What it means.** Durable Timers are free to hold and free to skip, so waits of
days or months are testable like anything else — but only against a server built
to skip them. That asymmetry is worth internalising: the same test file is a
millisecond unit test or a minute-long integration test depending entirely on
which server the fixture in `tests/conftest.py` handed it.

The CI-facing half of this is `./lab.sh replay-check`, which replays every
history saved in `histories/` against the current code — Experiment 3's failure
mode, as a check you can gate a deploy on. Save one first with `./lab.sh
replay-save <workflow-id>`.

### 5. Pick it apart yourself

The old version of this experiment was a list of code edits. Each one is now a
command:

| Try this | Concept | Command | What to look for, and what it means |
|---|---|---|---|
| Make `charge_payment` non-idempotent and run it flaky | Idempotency / at-least-once | `./lab.sh double-charge` | The `payment_id` in the terminal output records **three** charges for one order. Temporal guarantees at-least-once, never exactly-once — the retry is correct, the Activity is the bug. Idempotency is your job. |
| Set `maximum_attempts=1` and compare the failure | Retry policy tuning | `./lab.sh variant no-retries` | The Workflow fails on the first injected failure instead of retrying past it. Same Activities, same failures, different `RetryPolicy` — durability is configuration, not magic. |
| Add an Update handler with a validator | Update + validator | `./lab.sh update` | Two accepted Updates appear as `WorkflowExecutionUpdateAccepted/Completed`; the rejected one leaves **nothing** in the history and doesn't change the total. A validator rejects before anything is recorded — unlike a Signal, which is already history by the time you see it. |
| Replace the Timer with a Child Workflow | Child Workflows | `./lab.sh child` | `StartChildWorkflowExecutionInitiated` / `ChildWorkflowExecutionCompleted` pairs in the *parent's* history, plus three separate executions in `./lab.sh list`. Children get their own ID, own history, own retries. |
| Grow the history until it needs `continue_as_new` | Continue-As-New | `./lab.sh continue-as-new` | Same Workflow ID, brand-new **Run ID**, state carried across, history reset to empty. Look for `WorkflowExecutionContinuedAsNew` ending the first run. This is the escape hatch for the ~50k event / 50 MB history limit. |

### 6. One concept at a time (new)

Everything past this point is material the earlier version didn't have. Small,
single-purpose Workflows in `concepts.py` — read each one before running its
command; they're short and the comments carry the reasoning. Each command also
prints its own "look for:" line when it finishes.

| Command | Concept | What to look for, and what it means |
|---|---|---|
| `./lab.sh determinism` | Deterministic `now` / `uuid4` / `random` | The printed uuid and random values are *recorded* in the history, so replaying gives back the identical values. That's why you use `workflow.uuid4()` and not `uuid.uuid4()` — the SDK's versions are replay-safe by construction. |
| `./lab.sh cancellation` | Cancellation + shielded cleanup | `WorkflowExecutionCancelRequested`, the 60s `slow_task` cancelling early, then the cleanup Activity **still running to completion** before `WorkflowExecutionCanceled`. Cancellation is cooperative: shielded cleanup is how you guarantee you still release what you acquired. |
| `./lab.sh local-activity` | Local Activities | A single `MarkerRecorded` event instead of the usual `ActivityTaskScheduled/Started/Completed` trio — the Activity ran inside the Worker with no Matching round trip. Cheap and fast, but no independent retry across Workers; use for short, safe calls. |
| `./lab.sh versioning` | `workflow.patched()` | `MarkerRecorded` carrying a patch id, and the result comes back uppercased (the new branch). A history recorded *before* the patch replays down the **old** branch. This is how you change Workflow code without breaking runs already in flight — the hardest part of operating Temporal. |
| `./lab.sh searchable` | Search Attributes, Memos, Visibility | The `Category` attribute and the memo on `describe`, then a `Category = 'urgent'` query returning Workflows found without knowing any IDs. Search Attributes are indexed and queryable; Memos are stored but not indexed. |
| `./lab.sh schedule` | Schedules | Create → describe → trigger early → pause → backfill → delete, all on a live Schedule. Schedules are first-class objects you can operate on; the runs they spawn are ordinary Workflows. |
| `./lab.sh schedule-delay` | Start Delay | One Workflow, started once, later. No Schedule object involved — the delay is a start option. |
| `./lab.sh schedule-cron` | Cron (legacy) | The same recurring idea attached to the Workflow itself. Compare with `schedule`: cron cannot be paused, triggered early, or backfilled, which is exactly why Schedules exist. |

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
| `tests/` | Testing Workflows. | Eleven tests with Activities mocked by name, against a time-skipping server (or a live one as fallback — see `tests/conftest.py`). |
| `WORKFLOW_MAP.md` | Call-graph diagrams for the saga. | Which Workflow calls which Activity, for which use case — useful alongside `workflows.py`. |
