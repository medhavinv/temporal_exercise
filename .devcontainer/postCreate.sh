#!/usr/bin/env bash
set -euo pipefail

# Temporal CLI (bundles the dev server + Web UI)
curl -sSf https://temporal.download/cli.sh | sh
echo 'export PATH="$HOME/.temporalio/bin:$PATH"' >> "$HOME/.bashrc"

# Python deps
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

echo ""
echo "Setup complete. Two terminals from here:"
echo "  1) ~/.temporalio/bin/temporal server start-dev --db-filename temporal.db"
echo "  2) .venv/bin/python worker.py"
