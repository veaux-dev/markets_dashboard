#!/usr/bin/env bash
# Linux equivalent of RetoU2update.bat for cron usage.

set -euo pipefail

# Root of the project; adjust if you move the repo.
BASE_DIR="/mnt/markets_dashboard"

cd "$BASE_DIR"

# Activate virtual environment if present.
if [ -f ".venv/bin/activate" ]; then
  source ".venv/bin/activate"
fi

# Run the main script.
python RetoActinver_Stocks.py
