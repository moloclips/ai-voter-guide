#!/usr/bin/env python3
"""
verdict_review.py — Find candidates with pending proposed verdict changes, then
use codex exec or claude code to rate confidence in the new verdict vs the old one.

Usage:
  python scripts/verdict_review.py
  python scripts/verdict_review.py --provider codex --model gpt-5.4
  python scripts/verdict_review.py --provider claude
  python scripts/verdict_review.py --provider both
  python scripts/verdict_review.py --candidate "Arizona|Governor|Andy Biggs"
  python scripts/verdict_review.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.changes_file import resolve_changes_csv
from scripts.verdict_legend import load_verdict_labels, render_verdict_legend

FATAL_STDERR_PATTERNS = [
    "rate limit",
    "too many requests",
    "quota exceeded",
    "insufficient credits",
    "overloaded",
    "capacity",
]


class ProviderFatalError(RuntimeError):
    """Raised when a provider signals a fatal error (rate limit, quota, etc.)."""


CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
SHOW_CONTEXT = ROOT / "scripts" / "show_change_context.py"
DEFAULT_OUTPUT_CSV = ROOT / "reports" / "verdict_review_confidence.csv"

VERDICT_LABELS = load_verdict_labels()

CHANGE_FIELDNAMES = [
    "change_id", "table", "key", "action", "reasoning", "Model",
    "field", "value", "D", "Reasoning D", "I", "Reasoning I",
]

CSV_FIELDNAMES = [
    "change_id",
    "candidate_key",
    "old_verdict",
    "new_verdict",
    "confidence_codex",
    "reasoning_codex",
    "confidence_claude",
    "reasoning_claude",
]


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_changes(path: Path) -> list[dict[str, str]]:
    rows = load_csv(path)
    return [{f: row.get(f, "").strip() for f in CHANGE_FIELDNAMES} for row in rows]


def candidate_key(row: dict[str, str]) -> str:
    existing = row.get("Candidate_Key", "").strip()
    if existing:
        return existing
    c = row.get("Candidate", "").strip()
    s = row.get("State", "").strip()
    o = row.get("Office", "").strip()
    return f"{s}|{o}|{c}" if (c and s and o) else c


# ---------------------------------------------------------------------------
# Build the list of candidates to review
# ---------------------------------------------------------------------------

def pending_verdict_changes(
    changes_path: Path,
    only_candidate: str | None = None,
) -> list[dict[str, str]]:
    """
    Return one row per candidate that has a pending (not approved/denied)
    proposed Verdict change that differs from the current verdict.
    When multiple pending proposals exist for the same candidate, use the
    one with the highest change_id.
    """
    candidates = load_csv(CANDIDATES_CSV)
    current: dict[str, str] = {candidate_key(r): r.get("Verdict", "").strip() for r in candidates}

    changes = load_changes(changes_path)

    # Collect pending verdict proposals per candidate key
    proposals: dict[str, list[dict[str, str]]] = {}
    for row in changes:
        if row.get("table") != "candidates":
            continue
        if row.get("field") != "Verdict":
            continue
        d_val = row.get("D", "").strip().lower()
        i_val = row.get("I", "").strip().lower()
        if d_val in ("denied", "applied", "conflict"):
            continue
        if i_val in ("denied", "applied", "conflict"):
            continue
        if d_val == "approved" and i_val == "approved":
            continue
        ckey = row.get("key", "").strip()
        if not ckey:
            continue
        if only_candidate and ckey != only_candidate:
            continue
        proposals.setdefault(ckey, []).append(row)

    results: list[dict[str, str]] = []
    for ckey, rows in sorted(proposals.items()):
        # Pick highest change_id among pending proposals
        latest = max(
            rows,
            key=lambda r: int(r["change_id"]) if r.get("change_id", "").isdigit() else 0,
        )
        proposed = latest.get("value", "").strip()
        old = current.get(ckey, "")
        if proposed and proposed != old:
            results.append({
                "candidate_key": ckey,
                "change_id":     latest["change_id"],
                "old_verdict":   old,
                "new_verdict":   proposed,
                "model":         latest.get("Model", ""),
                "reasoning":     latest.get("reasoning", ""),
            })

    return results


# ---------------------------------------------------------------------------
# Context + prompt
# ---------------------------------------------------------------------------

def get_context(ckey: str, changes_filename: str) -> str:
    result = subprocess.run(
        [sys.executable, str(SHOW_CONTEXT), ckey, "--changes-file", changes_filename],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def build_prompt(ckey: str, old: str, new: str, context: str) -> str:
    old_label = VERDICT_LABELS.get(old, old)
    new_label = VERDICT_LABELS.get(new, new)
    verdict_legend = render_verdict_legend(prefix="  ", align=True)
    return f"""You are auditing a proposed verdict change for the AI Voter Guide project.

Verdict legend:
{verdict_legend}

Proposed change:
  Candidate : {ckey}
  Old verdict: {old} — {old_label}
  New verdict: {new} — {new_label}

Full candidate context (current evidence rows, prior changes, and model reasoning):
{context}

Your task:
Based solely on the direct AI-related evidence listed above, assess how confident you
are that the NEW verdict ({new}) is correct compared to the OLD verdict ({old}).

Respond with a JSON object only — no markdown, no preamble:
{{
  "confidence": <integer 0–100>,
  "reasoning": "<one or two sentences>"
}}

Confidence guide:
  100 = new verdict is definitely correct; old was wrong
   75 = new verdict is probably correct
   50 = genuinely uncertain; either could be right
   25 = old verdict is probably still correct
    0 = old verdict is definitely correct; new should be rejected"""


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def provider_env() -> dict[str, str]:
    env = dict(os.environ)
    node20 = Path("/opt/homebrew/opt/node@20/bin/node")
    if node20.exists():
        env["PATH"] = f"{node20.parent}:{env.get('PATH', '')}"
    return env


def resolve_cli(name: str) -> str | None:
    path = shutil.which(name)
    if path:
        return path
    candidate = Path.home() / ".npm-global" / "bin" / name
    if candidate.exists():
        return str(candidate)
    return None


def extract_json_result(raw: str) -> dict:
    """Parse a confidence/reasoning JSON object from raw text."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r'\{[^{}]*"confidence"[^{}]*\}', raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        return {"confidence": None, "reasoning": f"parse error — raw: {raw[:300]}"}


def proposal_resume_key(item: dict[str, str]) -> str:
    return "||".join([
        item.get("candidate_key", "").strip(),
        item.get("change_id", "").strip(),
        item.get("old_verdict", "").strip(),
        item.get("new_verdict", "").strip(),
    ])


# ---------------------------------------------------------------------------
# Codex provider
# ---------------------------------------------------------------------------

def render_codex_event(obj: dict, start: float) -> None:
    elapsed = time.time() - start
    prefix = f"  [codex {elapsed:>4.0f}s]"
    msg_type = obj.get("type", "")
    if msg_type == "message" and obj.get("role") == "assistant":
        for block in obj.get("content", []):
            if block.get("type") == "text":
                snippet = block["text"][:160].replace("\n", " ")
                print(f"{prefix} assistant: {snippet}", file=sys.stderr, flush=True)
    elif msg_type == "tool_call":
        name = obj.get("name", "")
        print(f"{prefix} tool_call: {name}", file=sys.stderr, flush=True)
    elif msg_type == "item.started":
        item = obj.get("item", {}) or {}
        item_type = item.get("type", "")
        label = item.get("command") or item.get("query") or item.get("id") or ""
        print(f"{prefix} started: {item_type} {label}".rstrip(), file=sys.stderr, flush=True)
    elif msg_type == "item.completed":
        item = obj.get("item", {}) or {}
        item_type = item.get("type", "")
        if item_type == "agent_message":
            text = (item.get("text") or "")[:160].replace("\n", " ")
            print(f"{prefix} completed: agent_message {text}", file=sys.stderr, flush=True)
        else:
            label = item.get("command") or item.get("query") or item.get("url") or item.get("id") or ""
            print(f"{prefix} completed: {item_type} {label}".rstrip(), file=sys.stderr, flush=True)
    elif msg_type == "item.updated":
        item = obj.get("item", {}) or {}
        if item.get("type") == "todo_list":
            remaining = sum(0 if part.get("completed") else 1 for part in item.get("items", []))
            print(f"{prefix} todo_list updated: {remaining} remaining", file=sys.stderr, flush=True)
    elif msg_type == "turn.completed":
        usage = obj.get("usage", {}) or {}
        print(
            f"{prefix} turn completed | in={usage.get('input_tokens', '?')} cached={usage.get('cached_input_tokens', '?')} out={usage.get('output_tokens', '?')}",
            file=sys.stderr,
            flush=True,
        )


def run_codex(prompt: str, model: str | None, timeout: int) -> dict:
    cli = resolve_cli("codex") or "codex"
    fd, tmp = tempfile.mkstemp(prefix="verdict-review-", suffix=".txt")
    os.close(fd)
    output_file = Path(tmp)

    cmd = [
        cli,
        "--search",
        "exec",
        "--skip-git-repo-check",
        "--sandbox", "read-only",
        "--json",
        "--output-last-message", str(output_file),
    ]
    if model:
        cmd.extend(["--model", model])
    cmd.append(prompt)

    start = time.time()
    print(f"  [codex    0s] starting codex exec", file=sys.stderr, flush=True)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=provider_env(),
            bufsize=1,
        )

        stderr_buf: list[str] = []

        def _drain() -> None:
            assert proc.stderr is not None
            for line in proc.stderr:
                if line:
                    stderr_buf.append(line)
                    print(f"  [codex stderr] {line.rstrip()}", file=sys.stderr, flush=True)

        stderr_thread = threading.Thread(target=_drain, daemon=True)
        stderr_thread.start()

        assert proc.stdout is not None
        for line in proc.stdout:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
                render_codex_event(obj, start)
            except json.JSONDecodeError:
                snippet = stripped[:160].replace("\n", " ")
                print(f"  [codex {time.time() - start:>4.0f}s] raw: {snippet}", file=sys.stderr, flush=True)

        stderr_thread.join(timeout=5)
        returncode = proc.wait(timeout=timeout)

        raw = output_file.read_text(encoding="utf-8").strip() if output_file.exists() else ""
        if returncode != 0 and not raw:
            stderr_text = "".join(stderr_buf).strip()
            if any(p in stderr_text.lower() for p in FATAL_STDERR_PATTERNS):
                raise ProviderFatalError(f"codex fatal: {stderr_text[:300]}")
            reason = f"codex exited {returncode}"
            if stderr_text:
                reason = f"codex exited {returncode} | {stderr_text[:300]}"
            return {"confidence": None, "reasoning": reason}
        return extract_json_result(raw)

    except subprocess.TimeoutExpired:
        proc.kill()
        stderr_thread.join(timeout=2)
        stderr_text = "".join(stderr_buf).strip()
        reason = "timeout"
        if stderr_text:
            reason = f"timeout | stderr: {stderr_text[:300]}"
        return {"confidence": None, "reasoning": reason}
    except ProviderFatalError:
        raise
    except Exception as exc:
        return {"confidence": None, "reasoning": f"error: {exc}"}
    finally:
        output_file.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Claude provider
# ---------------------------------------------------------------------------

def render_claude_event(obj: dict, start: float) -> None:
    elapsed = time.time() - start
    prefix = f"  [claude {elapsed:>4.0f}s]"
    event_type = obj.get("type", "")
    if event_type == "assistant":
        message = obj.get("message", {}) or {}
        for block in message.get("content", []):
            if block.get("type") == "text":
                snippet = block["text"][:160].replace("\n", " ")
                print(f"{prefix} assistant: {snippet}", file=sys.stderr, flush=True)
    elif event_type == "result":
        subtype = obj.get("subtype", "")
        usage = obj.get("usage", {}) or {}
        cost = obj.get("cost_usd")
        cost_str = f" cost=${cost:.4f}" if cost is not None else ""
        print(
            f"{prefix} result:{subtype} | in={usage.get('input_tokens', '?')} out={usage.get('output_tokens', '?')}{cost_str}",
            file=sys.stderr,
            flush=True,
        )
    elif event_type == "system":
        subtype = obj.get("subtype", "")
        print(f"{prefix} system:{subtype}", file=sys.stderr, flush=True)


def run_claude(prompt: str, model: str | None, timeout: int) -> dict:
    cli = resolve_cli("claude") or "claude"
    cmd = [
        cli,
        "-p",
        "--verbose",
        "--output-format", "stream-json",
        "--include-partial-messages",
    ]
    if model:
        cmd.extend(["--model", model])
    # Prompt must come before variadic --allowedTools / --disallowedTools flags
    cmd.append(prompt)
    cmd.extend([
        "--allowedTools", "Read,Grep,Glob",
        "--disallowedTools", "WebFetch,WebSearch,Edit,Write,Bash,NotebookEdit,TodoWrite",
    ])

    start = time.time()
    print(f"  [claude   0s] starting claude -p", file=sys.stderr, flush=True)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=provider_env(),
            bufsize=1,
        )

        stderr_buf: list[str] = []

        def _drain() -> None:
            assert proc.stderr is not None
            for line in proc.stderr:
                if line:
                    stderr_buf.append(line)
                    print(f"  [claude stderr] {line.rstrip()}", file=sys.stderr, flush=True)

        stderr_thread = threading.Thread(target=_drain, daemon=True)
        stderr_thread.start()

        assistant_texts: list[str] = []
        assert proc.stdout is not None
        for line in proc.stdout:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                obj = json.loads(stripped)
                render_claude_event(obj, start)
                # Collect assistant text blocks for result extraction
                if obj.get("type") == "assistant":
                    message = obj.get("message", {}) or {}
                    for block in message.get("content", []):
                        if block.get("type") == "text":
                            assistant_texts.append(block["text"])
            except json.JSONDecodeError:
                snippet = stripped[:160].replace("\n", " ")
                print(f"  [claude {time.time() - start:>4.0f}s] raw: {snippet}", file=sys.stderr, flush=True)

        stderr_thread.join(timeout=5)
        returncode = proc.wait(timeout=timeout)

        # Use the last assistant text block as the result
        raw = assistant_texts[-1].strip() if assistant_texts else ""
        if returncode != 0 and not raw:
            stderr_text = "".join(stderr_buf).strip()
            if any(p in stderr_text.lower() for p in FATAL_STDERR_PATTERNS):
                raise ProviderFatalError(f"claude fatal: {stderr_text[:300]}")
            reason = f"claude exited {returncode}"
            if stderr_text:
                reason = f"claude exited {returncode} | {stderr_text[:300]}"
            return {"confidence": None, "reasoning": reason}
        return extract_json_result(raw)

    except subprocess.TimeoutExpired:
        proc.kill()
        stderr_thread.join(timeout=2)
        stderr_text = "".join(stderr_buf).strip()
        reason = "timeout"
        if stderr_text:
            reason = f"timeout | stderr: {stderr_text[:300]}"
        return {"confidence": None, "reasoning": reason}
    except ProviderFatalError:
        raise
    except Exception as exc:
        return {"confidence": None, "reasoning": f"error: {exc}"}


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def load_existing_results(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return load_csv(path)


def append_result_csv(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        if not exists:
            writer.writeheader()
        writer.writerow({name: row.get(name, "") for name in CSV_FIELDNAMES})


def update_result_csv(path: Path, rows: list[dict]) -> None:
    """Rewrite the full CSV (used when updating existing rows with new provider columns)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in CSV_FIELDNAMES})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--provider", choices=["codex", "claude", "both"], default="codex",
                        help="Which provider to use (default: codex)")
    parser.add_argument("--model", help="Model override (e.g. gpt-5.4 for codex, claude-opus-4-5 for claude)")
    parser.add_argument("--changes-file", default="changes.csv", help="Changes CSV filename to review")
    parser.add_argument("--candidate", metavar="KEY", help="Only review one candidate key")
    parser.add_argument(
        "--old-verdict",
        action="append",
        choices=["no_record", "nuanced", "naughty", "nice"],
        dest="old_verdicts",
        help="Only review changes whose current verdict matches this value; can be repeated",
    )
    parser.add_argument(
        "--new-verdict",
        action="append",
        choices=["no_record", "nuanced", "naughty", "nice"],
        dest="new_verdicts",
        help="Only review changes whose proposed verdict matches this value; can be repeated",
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_CSV), help="CSV path for confidence results")
    parser.add_argument("--no-resume", action="store_true", help="Do not skip candidates already present in the output CSV")
    parser.add_argument("--timeout", type=int, default=300, help="Seconds to wait for each provider call before timing out")
    parser.add_argument("--dry-run", action="store_true", help="List candidates without calling any provider")
    args = parser.parse_args()

    providers: list[str] = ["codex", "claude"] if args.provider == "both" else [args.provider]

    # Validate CLIs exist before doing any work
    cli_names = {"codex": "codex", "claude": "claude"}
    for provider in providers:
        if not args.dry_run and resolve_cli(cli_names[provider]) is None:
            print(f"ERROR: '{cli_names[provider]}' CLI not found in PATH or ~/.npm-global/bin. "
                  f"Install it or use --provider to choose a different provider.", file=sys.stderr)
            sys.exit(1)

    output_path = Path(args.output)
    changes_path = resolve_changes_csv(args.changes_file)

    items = pending_verdict_changes(changes_path=changes_path, only_candidate=args.candidate)
    for item in items:
        item["resume_key"] = proposal_resume_key(item)

    if args.old_verdicts:
        allowed_old = set(args.old_verdicts)
        items = [it for it in items if it["old_verdict"] in allowed_old]

    if args.new_verdicts:
        allowed_new = set(args.new_verdicts)
        items = [it for it in items if it["new_verdict"] in allowed_new]

    # Load existing results and build a lookup by proposal identity so newer
    # verdict changes for the same candidate do not reuse stale scores.
    existing_rows = load_existing_results(output_path) if not args.no_resume else []
    results_by_key: dict[str, dict] = {}
    for row in existing_rows:
        key = proposal_resume_key(row)
        if not key.strip("|"):
            continue
        results_by_key[key] = dict(row)

    # Filter items to those that still need work from at least one requested provider
    def needs_provider(resume_key: str, provider: str) -> bool:
        col = f"confidence_{provider}"
        existing = results_by_key.get(resume_key, {})
        return existing.get(col, "").strip() == ""

    if not args.no_resume:
        items = [it for it in items if any(needs_provider(it["resume_key"], p) for p in providers)]

    if not items:
        if not args.no_resume:
            print(f"No pending verdict changes left after resuming from {output_path}.")
        else:
            print("No pending verdict changes found that differ from current verdicts.")
        return

    print(f"Pending verdict changes to review: {len(items)}\n")
    for it in items:
        print(f"  [{it['change_id']:>4}] {it['candidate_key']}")
        print(f"         {it['old_verdict']} → {it['new_verdict']}  (proposed by {it['model']})")
    print()

    if args.dry_run:
        return

    for idx, it in enumerate(items, 1):
        ckey = it["candidate_key"]
        old  = it["old_verdict"]
        new  = it["new_verdict"]
        print(f"[{idx}/{len(items)}] {ckey}")
        print(f"  {old} → {new}", flush=True)

        # Ensure we have a result row for this candidate
        resume_key = it["resume_key"]
        if resume_key not in results_by_key:
            results_by_key[resume_key] = {
                "change_id":    it["change_id"],
                "candidate_key": ckey,
                "old_verdict":  old,
                "new_verdict":  new,
                "confidence_codex":   "",
                "reasoning_codex":    "",
                "confidence_claude":  "",
                "reasoning_claude":   "",
            }

        row = results_by_key[resume_key]

        context = get_context(ckey, changes_path.name)
        if not context:
            print("  WARNING: no context returned from show_change_context.py", file=sys.stderr)

        prompt = build_prompt(ckey, old, new, context)

        for provider in providers:
            if not needs_provider(resume_key, provider):
                conf_str = row.get(f"confidence_{provider}", "")
                print(f"  [{provider}] skipping — already have confidence={conf_str}")
                continue

            try:
                if provider == "codex":
                    result = run_codex(prompt, model=args.model, timeout=args.timeout)
                else:
                    result = run_claude(prompt, model=args.model, timeout=args.timeout)
            except ProviderFatalError as exc:
                print(f"\nFATAL: {exc}", file=sys.stderr)
                print("Aborting — re-run to resume from this candidate.", file=sys.stderr)
                update_result_csv(output_path, list(results_by_key.values()))
                sys.exit(1)

            confidence = result.get("confidence")
            reasoning  = result.get("reasoning", "")
            conf_str   = f"{confidence}%" if confidence is not None else "N/A"

            print(f"  [{provider}] confidence : {conf_str}")
            print(f"  [{provider}] reasoning  : {reasoning}")

            row[f"confidence_{provider}"] = confidence if confidence is not None else ""
            row[f"reasoning_{provider}"]  = reasoning

        print()

        # Rewrite full CSV after each candidate so progress is saved incrementally
        update_result_csv(output_path, list(results_by_key.values()))

    # ---- Summary sorted by average confidence ----
    all_results = list(results_by_key.values())
    print("=" * 70)
    print("SUMMARY  (sorted by confidence, high = new verdict probably correct)")
    print("=" * 70)

    def sort_key(r: dict) -> float:
        vals = [r.get(f"confidence_{p}") for p in ["codex", "claude"]]
        nums = [v for v in vals if v != "" and v is not None]
        try:
            nums = [float(v) for v in nums]
        except (TypeError, ValueError):
            return -1.0
        return sum(nums) / len(nums) if nums else -1.0

    for r in sorted(all_results, key=sort_key, reverse=True):
        cid  = r.get("change_id", "")
        ckey = r.get("candidate_key", "")
        old  = r.get("old_verdict", "")
        new  = r.get("new_verdict", "")
        parts = []
        for p in ["codex", "claude"]:
            c = r.get(f"confidence_{p}")
            if c != "" and c is not None:
                try:
                    parts.append(f"{p}={int(float(c))}%")
                except (TypeError, ValueError):
                    parts.append(f"{p}=?")
        conf_str = " ".join(parts) if parts else "N/A"
        print(f"  {conf_str:<22}  [{cid:>4}]  {ckey}: {old} → {new}")

    update_result_csv(output_path, all_results)
    print()
    print(f"Wrote CSV: {output_path}")


if __name__ == "__main__":
    main()
