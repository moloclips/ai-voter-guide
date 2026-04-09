from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "reports" / "codex_model_probe.txt"
DEFAULT_MODELS = [
    "o3",
    "o4-mini",
    "gpt-5",
    "gpt-5-mini",
    "gpt-4.1",
    "gpt-4o",
]


def probe_model(model: str, timeout: int) -> str:
    cmd = [
        "codex",
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "-m",
        model,
        "Return exactly OK",
    ]
    started = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        elapsed = time.time() - started
        combined = (result.stdout or "") + (result.stderr or "")
        lines = [line for line in combined.strip().splitlines() if line.strip()]
        status = "accepted" if result.returncode == 0 else "rejected_or_failed"
        preview = "\n".join(lines[:12]) or "(no output)"
        return (
            f"MODEL: {model}\n"
            f"STATUS: {status}\n"
            f"RETURNCODE: {result.returncode}\n"
            f"ELAPSED_SECONDS: {elapsed:.1f}\n"
            f"OUTPUT:\n{preview}\n"
            f"{'-' * 60}\n"
        )
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - started
        stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        combined = stdout + stderr
        lines = [line for line in combined.strip().splitlines() if line.strip()]
        preview = "\n".join(lines[:12]) or "(no output before timeout)"
        return (
            f"MODEL: {model}\n"
            f"STATUS: timeout_probably_available\n"
            f"RETURNCODE: timeout\n"
            f"ELAPSED_SECONDS: {elapsed:.1f}\n"
            f"OUTPUT:\n{preview}\n"
            f"{'-' * 60}\n"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe which hosted models Codex accepts under the current ChatGPT login.")
    parser.add_argument("--timeout", type=int, default=20, help="Seconds to wait per model before treating it as a timeout")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Where to write the text report")
    parser.add_argument("models", nargs="*", help="Optional explicit model names to test")
    args = parser.parse_args()

    models = args.models or DEFAULT_MODELS
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sections = [
        f"Codex model probe\n",
        f"Working directory: {ROOT}\n",
        f"Per-model timeout: {args.timeout}s\n",
        f"Models tested: {', '.join(models)}\n",
        f"{'=' * 60}\n",
    ]
    for model in models:
        sections.append(probe_model(model, args.timeout))

    output_path.write_text("\n".join(sections), encoding="utf-8")
    print(f"Wrote model probe report to {output_path}")


if __name__ == "__main__":
    main()
