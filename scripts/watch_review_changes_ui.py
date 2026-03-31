#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WATCH_PATHS = [
    ROOT / "scripts" / "review_changes_ui.py",
    ROOT / "scripts" / "race_runner.py",
    ROOT / "scripts" / "race_runner.schema.json",
    ROOT / "scripts" / "apply_changes.py",
]
POLL_SECONDS = 0.6


def snapshot() -> dict[str, int]:
    state: dict[str, int] = {}
    for path in WATCH_PATHS:
        try:
            state[str(path)] = path.stat().st_mtime_ns
        except FileNotFoundError:
            continue
    return state


def start_server() -> subprocess.Popen[str]:
    print("Starting review_changes_ui.py...", flush=True)
    return subprocess.Popen([sys.executable, str(ROOT / "scripts" / "review_changes_ui.py")], cwd=ROOT, text=True)


def stop_server(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def main() -> None:
    print(f"Watching review UI files in {ROOT}", flush=True)
    print("Press Ctrl+C to stop.", flush=True)
    proc = start_server()
    previous = snapshot()
    try:
        while True:
            time.sleep(POLL_SECONDS)
            current = snapshot()
            if current != previous:
                previous = current
                print("Change detected. Restarting review UI server...", flush=True)
                stop_server(proc)
                proc = start_server()
            elif proc.poll() is not None:
                print("Review UI server exited. Restarting...", flush=True)
                proc = start_server()
    except KeyboardInterrupt:
        print("\nStopping watcher.", flush=True)
    finally:
        stop_server(proc)


if __name__ == "__main__":
    main()
