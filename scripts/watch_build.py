#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import subprocess
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parents[1]
WATCH_PATHS = [
    ROOT / "template.html",
    ROOT / "build.py",
    ROOT / "data",
]
POLL_SECONDS = 0.75
VERSION = {"value": 1}

RELOAD_SNIPPET = """
<script>
(function () {
  let current = null;
  async function poll() {
    try {
      const res = await fetch('/__watch_build_version', { cache: 'no-store' });
      const data = await res.json();
      if (current === null) current = data.version;
      else if (data.version !== current) location.reload();
    } catch (err) {}
  }
  setInterval(poll, 1000);
  poll();
})();
</script>
"""


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
        VERSION["value"] += 1
        print("Watching for changes...", flush=True)
    else:
        print(
            f"build.py failed with exit code {result.returncode}. Watching for more changes...",
            flush=True,
        )


class Handler(BaseHTTPRequestHandler):
    directory = ROOT

    def _send_bytes(self, data: bytes, content_type: str, status: int = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/__watch_build_version":
            payload = json.dumps({"version": VERSION["value"]}).encode("utf-8")
            self._send_bytes(payload, "application/json; charset=utf-8")
            return

        rel = unquote(parsed.path.lstrip("/")) or "guide.html"
        target = (self.directory / rel).resolve()
        if self.directory.resolve() not in target.parents and target != self.directory.resolve():
            self.send_error(HTTPStatus.FORBIDDEN)
            return
        if target.is_dir():
            target = target / "guide.html"
        if not target.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        mime, _ = mimetypes.guess_type(str(target))
        mime = mime or "application/octet-stream"
        data = target.read_bytes()
        if target.suffix.lower() in {".html", ".htm"}:
            text = data.decode("utf-8")
            if "</body>" in text:
                text = text.replace("</body>", RELOAD_SNIPPET + "</body>")
            else:
                text += RELOAD_SNIPPET
            data = text.encode("utf-8")
            mime = "text/html; charset=utf-8"
        self._send_bytes(data, mime)

    def log_message(self, fmt: str, *args) -> None:
        return


def watch_loop():
    previous = snapshot()
    while True:
        time.sleep(POLL_SECONDS)
        current = snapshot()
        if current != previous:
            previous = current
            run_build()


def serve(host: str, port: int, directory: Path) -> None:
    Handler.directory = directory
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Hot preview running at http://{host}:{port}/guide.html", flush=True)
    server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Watch voter-guide source files and rebuild on change.")
    parser.add_argument("--serve", action="store_true", help="Also serve guide.html with browser auto-reload.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5503)
    parser.add_argument("--directory", default=str(ROOT))
    args = parser.parse_args()

    print(f"Watching {ROOT} for template/data changes.", flush=True)
    print("Press Ctrl+C to stop.", flush=True)

    watcher = threading.Thread(target=watch_loop, daemon=True)
    watcher.start()

    try:
      if args.serve:
          serve(args.host, args.port, Path(args.directory).resolve())
      else:
          while True:
              time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped watcher.", flush=True)


if __name__ == "__main__":
    main()
