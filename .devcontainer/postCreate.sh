#!/usr/bin/env bash
# Runs once when the Codespace / dev container is created.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "== python environment =="
python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt
echo "   installed: $(.venv/bin/python -c 'import temporalio; print("temporalio", temporalio.__version__ if hasattr(temporalio,"__version__") else "")')"

echo
echo "== temporal cli =="
chmod +x lab.sh bootstrap_server.sh
./bootstrap_server.sh

cat <<'DONE'

════════════════════════════════════════════════════════════════════
  Ready. Two commands to start:

    ./lab.sh up          start the Temporal server and a Worker
    ./lab.sh             list every experiment

  Then open the forwarded port 8233 ("Temporal Web UI") from the
  PORTS tab. Reading the Event History there is the whole point --
  see Experiment 1 in the README.
════════════════════════════════════════════════════════════════════
DONE
