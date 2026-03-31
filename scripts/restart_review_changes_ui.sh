#!/bin/zsh
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT/.claude/review_changes_ui.pid"
LOG_FILE="$ROOT/.claude/review_changes_ui.log"

mkdir -p "$ROOT/.claude"

if [[ -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${OLD_PID}" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    kill "$OLD_PID" 2>/dev/null || true
    sleep 0.4
    if kill -0 "$OLD_PID" 2>/dev/null; then
      kill -9 "$OLD_PID" 2>/dev/null || true
    fi
  fi
fi

cd "$ROOT"
nohup python3 scripts/review_changes_ui.py >>"$LOG_FILE" 2>&1 </dev/null &
echo $! > "$PID_FILE"
