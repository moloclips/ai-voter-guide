#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WATCH_PATHS = [
    ROOT / "template.html",
    ROOT / "build.py",
    ROOT / "data",
]
POLL_SECONDS = 0.75


def iter_files():
    for path in WATCH_PATHS:
        if path.is_dir():
            for child in sorted(path.rglob("*")):
                if child.is_file() and child.suffix in {".csv"}:
                    yield child
        elif path.is_file():
            yield path


def snapshot():
    state = {}
    for path in iter_files():
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        state[str(path)] = stat.st_mtime_ns
    return state


def run_build():
    print("Change detected. Running build.py...", flush=True)
    result = subprocess.run(
        [sys.executable, str(ROOT / "build.py")],
        cwd=ROOT,
        text=True,
    )
    if result.returncode == 0:
        print("Watching for changes...", flush=True)
    else:
        print(f"build.py failed with exit code {result.returncode}. Watching for more changes...", flush=True)


def main():
    print(f"Watching {ROOT} for template/data changes.", flush=True)
    print("Press Ctrl+C to stop.", flush=True)
    previous = snapshot()

    try:
        while True:
            time.sleep(POLL_SECONDS)
            current = snapshot()
            if current != previous:
                previous = current
                run_build()
    except KeyboardInterrupt:
        print("\nStopped watcher.", flush=True)


if __name__ == "__main__":
    main()
