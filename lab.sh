#!/usr/bin/env bash
# Single entry point for every experiment in this repo.
#
#   ./lab.sh up                 start the Temporal server and a Worker
#   ./lab.sh <experiment>       run one experiment
#   ./lab.sh down               stop everything
#
# Run ./lab.sh with no arguments for the full list.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
RUN=.run
mkdir -p "$RUN"

PY=${PY:-.venv/bin/python}
ADDRESS=${TEMPORAL_ADDRESS:-127.0.0.1:7233}

# The CLI: an explicit override, then ./bin, then whatever is on PATH.
if [ -n "${TEMPORAL_BIN:-}" ]; then TEMPORAL="$TEMPORAL_BIN"
elif [ -x ./bin/temporal ]; then TEMPORAL=./bin/temporal
else TEMPORAL=$(command -v temporal || echo ""); fi
TC="$TEMPORAL --address $ADDRESS"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
die()  { printf '\033[31m%s\033[0m\n' "$*" >&2; exit 1; }

require_server() {
  [ -n "$TEMPORAL" ] || die "no temporal CLI found. Run ./bootstrap_server.sh"
  $TC operator cluster health >/dev/null 2>&1 || die "server not reachable at $ADDRESS. Run ./lab.sh up"
}

# --- lifecycle ---------------------------------------------------------------

start_server() {
  [ -n "$TEMPORAL" ] || die "no temporal CLI found. Run ./bootstrap_server.sh"
  if $TC operator cluster health >/dev/null 2>&1; then echo "server already up"; return; fi
  nohup $TEMPORAL server start-dev \
    --db-filename "$RUN/temporal.db" --ip 127.0.0.1 --port 7233 --ui-port 8233 \
    --log-level warn > "$RUN/server.log" 2>&1 &
  echo $! > "$RUN/server.pid"
  disown 2>/dev/null || true
  for _ in $(seq 1 30); do
    $TC operator cluster health >/dev/null 2>&1 && break; sleep 1
  done
  $TC operator cluster health >/dev/null 2>&1 || { tail -20 "$RUN/server.log"; die "server failed to start"; }
  # Needed by the `searchable` experiment; harmless if it already exists.
  $TC operator search-attribute create --name Category --type Keyword >/dev/null 2>&1
  echo "server up on $ADDRESS (Web UI on port 8233)"
}

start_worker() {
  stop_worker
  env "$@" nohup $PY worker.py > "$RUN/worker.log" 2>&1 &
  echo $! > "$RUN/worker.pid"
  disown 2>/dev/null || true
  sleep 6
  grep -q "polling task queue" "$RUN/worker.log" || { tail -20 "$RUN/worker.log"; die "worker failed to start"; }
  echo "worker up${1:+ (env: $*)}"
}

stop_worker() {
  [ -f "$RUN/worker.pid" ] && kill -9 "$(cat "$RUN/worker.pid")" 2>/dev/null
  rm -f "$RUN/worker.pid"; sleep 1
}

stop_server() {
  [ -f "$RUN/server.pid" ] && kill "$(cat "$RUN/server.pid")" 2>/dev/null
  rm -f "$RUN/server.pid"
}

# Start a workflow and echo its ID. Parsed from the Web UI line rather than the
# human-readable first line, which is prose and liable to be reworded.
start_order() {
  $PY starter.py start --no-wait 2>&1 | sed -n 's|.*/workflows/\([^ ]*\)$|\1|p' | head -1
}

# Status of one Workflow, read from JSON so it does not depend on table layout.
wf_status() {
  $TC workflow describe -w "$1" -o json 2>/dev/null \
    | $PY -c 'import sys,json;print(json.load(sys.stdin)["workflowExecutionInfo"]["status"].replace("WORKFLOW_EXECUTION_STATUS_",""))' \
    2>/dev/null || echo "UNKNOWN"
}

# ID of the most recent Workflow, optionally filtered by status.
latest_workflow() {
  local query=${1:-}
  $TC workflow list --limit 1 ${query:+--query "$query"} -o json 2>/dev/null \
    | $PY -c 'import sys,json;d=json.load(sys.stdin);print(d[0]["execution"]["workflowId"] if d else "")'
}

# --- experiments -------------------------------------------------------------

exp_order() {
  bold "── happy path ──"
  $PY starter.py start
}

exp_history() {
  local wid=${1:-}
  [ -n "$wid" ] || wid=$(latest_workflow)
  bold "── event history for $wid ──"
  $TC workflow show -w "$wid"
}

exp_signal_query() {
  bold "── signal and query ──"
  local wid; wid=$(start_order); echo "started $wid"
  sleep 2
  echo "query  ->"; $PY starter.py status "$wid"
  echo "signal ->"; $PY starter.py cancel "$wid"
  sleep 3
  echo "query  ->"; $PY starter.py status "$wid"
  echo; echo "the Signal is in the history as WorkflowExecutionSignaled; the Query is not there at all"
}

exp_retries() {
  bold "── retries (FLAKY_ACTIVITIES=1: every activity fails its first 2 attempts) ──"
  start_worker FLAKY_ACTIVITIES=1
  local wid; wid=$(start_order); echo "started $wid"
  $TC workflow result -w "$wid" >/dev/null 2>&1

  echo
  echo "The surprise: there are NO ActivityTaskFailed events. Temporal retries"
  echo "in its own mutable state, so a retried Activity leaves the history as"
  echo "compact as one that succeeded first time. The evidence is the 'attempt'"
  echo "counter and 'lastFailure' on ActivityTaskStarted:"
  echo
  $TC workflow show -w "$wid" -o json 2>/dev/null | $PY -c '
import json, sys
events = json.load(sys.stdin).get("events", [])
print("  distinct event types:")
for name in sorted({e["eventType"].replace("EVENT_TYPE_", "") for e in events}):
    print(f"    {name}")
print()
for e in events:
    started = e.get("activityTaskStartedEventAttributes")
    if not started:
        continue
    failure = (started.get("lastFailure") or {}).get("message", "-")
    eid, attempt = e["eventId"], started.get("attempt")
    print("  eventId %3s  attempt=%s  lastFailure=%s" % (eid, attempt, failure))
'
  start_worker
}

exp_compensation() {
  bold "── saga compensation (FAIL_SHIPPING=1) ──"
  start_worker FAIL_SHIPPING=1
  local wid; wid=$(start_order); echo "started $wid"
  sleep 16
  $PY starter.py status "$wid"
  echo; echo "the two compensating activities ran, then the workflow failed:"
  $TC workflow show -w "$wid" 2>/dev/null | grep -iE "ActivityTaskScheduled|WorkflowExecutionFailed" | tail -6
  start_worker
}

exp_crash() {
  bold "── durability: kill the worker mid-run ──"
  local wid; wid=$(start_order); echo "started $wid"
  sleep 3
  echo ">>> killing the worker"; stop_worker
  echo "    status with no worker at all: $(wf_status "$wid")"
  echo ">>> waiting 15s with no worker (the 10s durable Timer fires anyway)"
  sleep 15
  echo "    still, with nothing running your code: $(wf_status "$wid")"
  echo ">>> restarting the worker"; start_worker
  sleep 8
  echo "    final status: $(wf_status "$wid")"
  $PY starter.py status "$wid"
  echo; echo "nothing was lost, and you wrote no recovery code"
}

exp_replay_break() {
  bold "── non-determinism caught by replay ──"
  # Run a fresh order so the history definitely matches the current code, and
  # save it to its own directory: histories/ accumulates runs from older code
  # versions, which would muddy the demonstration.
  local demo="$RUN/replay-demo"
  rm -rf "$demo"
  local wid; wid=$(start_order)
  echo "recording a fresh run: $wid"
  $TC workflow result -w "$wid" >/dev/null 2>&1
  $PY replay_check.py save "$wid" --dir "$demo"
  echo
  # replay_break.py works on an in-memory copy of the module, so workflows.py
  # is never modified and cannot be left in a broken state.
  $PY replay_break.py "$demo/$wid.json"
}

exp_schedule() {
  bold "── schedules ──"
  $PY scheduling.py create
  echo "waiting 25s for it to fire a few times..."; sleep 25
  $PY scheduling.py describe
  echo; $PY scheduling.py trigger
  echo; $PY scheduling.py pause
  echo; $PY scheduling.py backfill
  echo; echo "cleaning up:"; $PY scheduling.py delete
}

usage() {
  cat <<'USAGE'
lab.sh -- drive every experiment in this repo

  lifecycle
    up                    start the Temporal server and a Worker
    down                  stop both
    status                is anything running?
    test                  run the test suite against the live server

  the order saga (workflows.py)
    order                 happy path, start to finish
    signal-query          Signal vs Query, and how they differ in the history
    retries               injected transient failures and automatic retry
    compensation          non-retryable failure, saga rollback
    crash                 kill the Worker mid-run and watch it resume
    history [id]          print an Event History (defaults to the latest)
    list                  list recent Workflows

  durability and safe deploys
    replay-break          prove that reordering activities breaks replay
    replay-save <id>      save a history to histories/
    replay-check          replay all saved histories against current code

  one concept at a time (concepts.py)
    update                Update + validator vs Signal vs Query
    continue-as-new       roll over a long history, keeping the Workflow ID
    child                 Child Workflows in parallel
    determinism           workflow.now / uuid4 / random
    cancellation          real cancellation + shielded cleanup
    local-activity        Local Activities and their marker in the history
    versioning            workflow.patched() for in-flight deploys
    searchable            Search Attributes, Memos, and Visibility queries

  scheduling
    schedule              create, describe, trigger, pause, backfill, delete
    schedule-delay        one-shot deferred start
    schedule-cron         legacy cron (start it, then terminate it to stop)
USAGE
}

# --- dispatch ----------------------------------------------------------------

case "${1:-}" in
  up)            start_server; start_worker ;;
  down)          stop_worker; stop_server; echo "stopped" ;;
  status)
    if [ -n "$TEMPORAL" ] && $TC operator cluster health >/dev/null 2>&1; then
      echo "server: up on $ADDRESS"; else echo "server: down"; fi
    if [ -f "$RUN/worker.pid" ] && kill -0 "$(cat "$RUN/worker.pid")" 2>/dev/null; then
      echo "worker: up"; else echo "worker: down"; fi ;;
  test)          require_server; TEMPORAL_TEST_ADDRESS=$ADDRESS $PY -m pytest -q ;;

  order)         require_server; exp_order ;;
  signal-query)  require_server; exp_signal_query ;;
  retries)       require_server; exp_retries ;;
  compensation)  require_server; exp_compensation ;;
  crash)         require_server; exp_crash ;;
  history)       require_server; exp_history "${2:-}" ;;
  list)          require_server; $TC workflow list --limit 20 ;;

  replay-break)  require_server; exp_replay_break ;;
  replay-save)   require_server; $PY replay_check.py save "${2:?workflow id required}" ;;
  replay-check)  $PY replay_check.py check ;;

  update|continue-as-new|child|determinism|cancellation|local-activity|versioning|searchable)
                 require_server; bold "── $1 ──"; $PY concepts_client.py "$1" ;;

  schedule)       require_server; exp_schedule ;;
  schedule-delay) require_server; $PY scheduling.py delay ;;
  schedule-cron)  require_server; $PY scheduling.py cron ;;

  ""|-h|--help|help) usage ;;
  *)             echo "unknown experiment: $1"; echo; usage; exit 1 ;;
esac
