from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
import shutil
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
README_PATH = ROOT / "README.md"
CHANGES_CSV = ROOT / "changes.csv"
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
RACES_CSV = ROOT / "data" / "races.csv"
LOG_DIR = ROOT / ".claude" / "race_runner_logs"
SCHEMA_PATH = ROOT / "scripts" / "race_runner.schema.json"
PROMPT_TEMPLATE_PATH = ROOT / "scripts" / "race_runner_prompt.txt"

CHANGE_FIELDNAMES = ["change_id", "table", "key", "action", "reasoning", "field", "value", "status"]
ACTIVE_CHANGE_STATUSES = {"pending", "approved", "applied"}
DEFAULT_ALLOWED_TOOLS = "Read,Grep,Glob,WebFetch,WebSearch"
DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_DISALLOWED_TOOLS = "Edit,Write,NotebookEdit,MultiEdit,Bash,TodoWrite"
REVIEWER_COLUMNS = {"claude": "Claude", "codex": "Codex", "gemini": "Gemini"}
VALID_CANDIDATE_FIELDS = {"Verdict", "Status"}
VALID_EVIDENCE_FIELDS = {"Source_Description", "URL"}
VALID_VERDICTS = {"nice", "nuanced", "no_record", "naughty"}
VALID_STATUS_VALUES = {"Out"}


def load_prompt_template() -> str:
    return PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")


DEFAULT_PROMPT_TEMPLATE = load_prompt_template()


class ClaudeRunError(RuntimeError):
    def __init__(self, message: str, stdout: str = "", stderr: str = "") -> None:
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr


def load_schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def resolve_cli(name: str) -> str | None:
    path = shutil.which(name)
    if path:
        return path
    candidate = Path.home() / ".npm-global" / "bin" / name
    if candidate.exists():
        return str(candidate)
    return None


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_changes(rows: list[dict[str, str]]) -> None:
    with CHANGES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CHANGE_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def ensure_changes_csv() -> None:
    if CHANGES_CSV.exists():
        return
    write_changes([])


def next_change_id(changes: list[dict[str, str]]) -> int:
    ids = [int(row["change_id"]) for row in changes if row.get("change_id", "").strip().isdigit()]
    return (max(ids) if ids else 0) + 1


def race_key(row: dict[str, str]) -> str:
    return f'{row.get("State", "").strip()}|{row.get("Office", "").strip()}'


def parse_int(value: str, default: int = 0) -> int:
    try:
        return int((value or "").strip())
    except ValueError:
        return default


def select_races(rows: list[dict[str, str]], requested_race: str | None, max_races: int) -> list[dict[str, str]]:
    if requested_race:
        for row in rows:
            if race_key(row) == requested_race:
                return [row]
        raise SystemExit(f"Race not found: {requested_race}")

    ordered = sorted(
        rows,
        key=lambda row: (
            parse_int(row.get("Claude", "0")),
            parse_int(row.get("Priority", "999999"), default=999999),
            row.get("State", ""),
            row.get("Office", ""),
        ),
    )
    if max_races > 0:
        return ordered[:max_races]
    return ordered


def candidates_for_race(
    candidate_rows: list[dict[str, str]],
    race_row: dict[str, str],
    requested_candidate: str | None,
    requested_verdict: str | None,
) -> list[dict[str, str]]:
    rows = [
        row
        for row in candidate_rows
        if row.get("State", "").strip() == race_row.get("State", "").strip()
        and row.get("Office", "").strip() == race_row.get("Office", "").strip()
    ]
    if requested_candidate:
        rows = [row for row in rows if row.get("Candidate", "").strip() == requested_candidate]
    if requested_verdict:
        rows = [row for row in rows if row.get("Verdict", "").strip() == requested_verdict]
    return rows


def existing_change_groups(changes: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in changes:
        change_id = row.get("change_id", "").strip()
        if not change_id:
            continue
        grouped[change_id].append(row)
    return grouped


def group_signature(rows: list[dict[str, str]]) -> tuple[Any, ...]:
    first = rows[0]
    atoms = tuple(sorted((row.get("field", "").strip(), row.get("value", "")) for row in rows))
    return (
        first.get("table", "").strip(),
        first.get("key", "").strip(),
        first.get("action", "").strip(),
        atoms,
    )


def active_group_signatures(changes: list[dict[str, str]]) -> set[tuple[Any, ...]]:
    signatures: set[tuple[Any, ...]] = set()
    for group in existing_change_groups(changes).values():
        if not any(row.get("status", "").strip() in ACTIVE_CHANGE_STATUSES for row in group):
            continue
        signatures.add(group_signature(group))
    return signatures


def relevant_existing_changes(
    changes: list[dict[str, str]],
    candidate_row: dict[str, str],
    evidence_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    evidence_ids = {row.get("Evidence_ID", "").strip() for row in evidence_rows}
    candidate = candidate_row.get("Candidate", "").strip()
    relevant: list[dict[str, str]] = []
    for row in changes:
        table = row.get("table", "").strip()
        key = row.get("key", "").strip()
        if table == "candidates" and key == candidate:
            relevant.append(row)
        elif table == "evidence" and (key in evidence_ids or (not key and row.get("field", "").strip() == "Candidate" and row.get("value", "").strip() == candidate)):
            relevant.append(row)
    return relevant


def candidate_has_existing_changes(
    changes: list[dict[str, str]],
    candidate_row: dict[str, str],
    evidence_rows: list[dict[str, str]],
) -> bool:
    return bool(relevant_existing_changes(changes, candidate_row, evidence_rows))


def stop_requested(stop_file: str) -> bool:
    return bool(stop_file) and Path(stop_file).exists()


def build_prompt(
    candidate_row: dict[str, str],
    evidence_rows: list[dict[str, str]],
    change_rows: list[dict[str, str]],
    prompt_template: str,
) -> str:
    candidate_json = json.dumps(candidate_row, indent=2, ensure_ascii=True)
    evidence_json = json.dumps(evidence_rows, indent=2, ensure_ascii=True)
    changes_json = json.dumps(change_rows, indent=2, ensure_ascii=True)
    today = datetime.now().date().isoformat()
    return prompt_template.format(
        today=today,
        candidate_json=candidate_json,
        evidence_json=evidence_json,
        changes_json=changes_json,
    )


def extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    if not text:
        raise ValueError("Claude returned empty output")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        return json.loads(text[start : end + 1])


def extract_claude_payload(result_obj: dict[str, Any]) -> dict[str, Any]:
    if result_obj.get("is_error"):
        raise RuntimeError(result_obj.get("result", "Claude returned an error result"))
    # --json-schema puts structured output here; plain -p puts it in result
    structured = result_obj.get("structured_output")
    if isinstance(structured, dict):
        return structured
    result = result_obj.get("result")
    if isinstance(result, dict):
        return result
    if isinstance(result, str) and result.strip():
        return extract_json(result)
    raise ValueError(f"Claude result envelope had no usable payload (result={result!r}, structured_output={structured!r})")


def extract_generic_payload(text: str) -> dict[str, Any]:
    return extract_json(text)


def render_stream_event(obj: dict[str, Any], start_time: float) -> None:
    event_type = str(obj.get("type", "")).strip()
    subtype = str(obj.get("subtype", "")).strip()
    elapsed = int(time.time() - start_time)
    prefix = f"[claude {elapsed:>4}s]"

    if event_type == "init" or (event_type == "system" and subtype == "init"):
        model = obj.get("model", "")
        print(f"{prefix} session started | model={model}" if model else f"{prefix} session started", file=sys.stderr, flush=True)
        return

    if event_type == "result":
        cost = obj.get("total_cost_usd")
        turns = obj.get("num_turns")
        fragments = [f"{prefix} done | subtype={subtype}"]
        if turns not in (None, ""):
            fragments.append(f"turns={turns}")
        if cost not in (None, ""):
            fragments.append(f"cost=${float(cost):.4f}")
        print(" | ".join(fragments), file=sys.stderr, flush=True)
        return

    if event_type == "assistant":
        content = (obj.get("message") or {}).get("content") or []
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type", "")
            if btype == "text":
                text = " ".join((block.get("text") or "").split())
                if text:
                    short = text[:120] + "…" if len(text) > 120 else text
                    print(f"{prefix} {short}", file=sys.stderr, flush=True)
            elif btype == "tool_use":
                name = block.get("name", "tool")
                inp = block.get("input") or {}
                detail = (
                    inp.get("query")
                    or inp.get("url")
                    or inp.get("file_path")
                    or inp.get("pattern")
                    or inp.get("path")
                    or inp.get("command")
                    or ""
                )
                if detail:
                    short = detail[:80] + "…" if len(detail) > 80 else detail
                    print(f"{prefix} {name}({short})", file=sys.stderr, flush=True)
                else:
                    print(f"{prefix} {name}()", file=sys.stderr, flush=True)
        return

    if event_type == "rate_limit_event":
        print(f"{prefix} rate limited…", file=sys.stderr, flush=True)
        return

    # swallow stream_event, user, and other low-value events


def render_codex_event(obj: dict[str, Any], start_time: float) -> None:
    elapsed = int(time.time() - start_time)
    prefix = f"[codex  {elapsed:>4}s]"
    event_type = str(obj.get("type", "")).strip()
    if event_type in {"reasoning", "message"}:
        text = " ".join(str(obj.get("text", "")).split())
        if text:
            short = text[:120] + "…" if len(text) > 120 else text
            print(f"{prefix} {short}", file=sys.stderr, flush=True)
        return
    if event_type in {"tool_call", "tool_result"}:
        name = obj.get("name") or obj.get("tool") or "tool"
        print(f"{prefix} {name}", file=sys.stderr, flush=True)
        return
    if event_type == "thread.started":
        thread_id = str(obj.get("thread_id", "")).strip()
        tail = thread_id[-8:] if thread_id else ""
        print(f"{prefix} session started{f' | thread={tail}' if tail else ''}", file=sys.stderr, flush=True)
        return
    if event_type == "turn.started":
        print(f"{prefix} turn started", file=sys.stderr, flush=True)
        return
    if event_type == "item.started":
        item = obj.get("item") or {}
        item_type = str(item.get("type", "")).strip() or "item"
        detail = (
            item.get("name")
            or item.get("tool")
            or item.get("command")
            or item.get("path")
            or ""
        )
        if detail:
            short = str(detail)
            short = short[:80] + "…" if len(short) > 80 else short
            print(f"{prefix} {item_type} started | {short}", file=sys.stderr, flush=True)
        else:
            print(f"{prefix} {item_type} started", file=sys.stderr, flush=True)
        return
    if event_type == "item.completed":
        item = obj.get("item") or {}
        item_type = str(item.get("type", "")).strip() or "item"
        if item_type == "agent_message":
            text = " ".join(str(item.get("text", "")).split())
            if text:
                short = text[:160] + "…" if len(text) > 160 else text
                print(f"{prefix} agent: {short}", file=sys.stderr, flush=True)
                return
        print(f"{prefix} {item_type} completed", file=sys.stderr, flush=True)
        return
    if event_type == "turn.completed":
        usage = obj.get("usage") or {}
        input_tokens = usage.get("input_tokens")
        cached_tokens = usage.get("cached_input_tokens")
        output_tokens = usage.get("output_tokens")
        fragments = [f"{prefix} turn completed"]
        if input_tokens not in (None, ""):
            fragments.append(f"in={input_tokens}")
        if cached_tokens not in (None, ""):
            fragments.append(f"cached={cached_tokens}")
        if output_tokens not in (None, ""):
            fragments.append(f"out={output_tokens}")
        print(" | ".join(fragments), file=sys.stderr, flush=True)
        return
    if event_type == "turn.failed":
        error = obj.get("error") or {}
        message = str(error.get("message", "")).strip() or "turn failed"
        short = message[:160] + "…" if len(message) > 160 else message
        print(f"{prefix} failed | {short}", file=sys.stderr, flush=True)
        return
    if event_type == "error":
        message = str(obj.get("message", "")).strip() or "error"
        short = message[:160] + "…" if len(message) > 160 else message
        print(f"{prefix} error | {short}", file=sys.stderr, flush=True)
        return


def stream_subprocess_lines(proc: subprocess.Popen[str], renderer, start_time: float) -> tuple[str, str, list[dict[str, Any]]]:
    stdout_lines: list[str] = []
    stderr_lines: list[str] = []
    parsed_events: list[dict[str, Any]] = []

    assert proc.stderr is not None

    def _drain_stderr() -> None:
        text = proc.stderr.read()
        if text:
            stderr_lines.append(text)

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    assert proc.stdout is not None
    for line in proc.stdout:
        stdout_lines.append(line)
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            print(stripped, file=sys.stderr, flush=True)
            continue
        parsed_events.append(obj)
        renderer(obj, start_time)

    stderr_thread.join(timeout=5)
    return "".join(stdout_lines), "".join(stderr_lines), parsed_events


def validate_proposal(candidate_name: str, proposal: dict[str, Any]) -> None:
    table = proposal.get("table", "")
    action = proposal.get("action", "")
    key = proposal.get("key", "")
    fields = proposal.get("fields", [])
    if table not in {"candidates", "evidence"}:
        raise ValueError(f"Unsupported table: {table}")
    if action not in {"add", "mod", "del"}:
        raise ValueError(f"Unsupported action: {action}")
    if not isinstance(fields, list):
        raise ValueError("Proposal fields must be a list")
    if table == "candidates" and key != candidate_name:
        raise ValueError(f"Candidate proposal key must match candidate name: {key}")
    if table == "candidates" and action == "add":
        raise ValueError("Candidate add changes are not supported")
    if table == "evidence" and action in {"mod", "del"} and not key:
        raise ValueError("Evidence mod/del changes must include an Evidence_ID key")
    if table == "evidence" and action == "add" and key:
        raise ValueError("Evidence add changes must use a blank key")
    if action in {"add", "mod"} and not fields:
        raise ValueError("Add/mod changes must include fields")
    if action == "del" and fields:
        raise ValueError("Delete changes must not include fields")
    if table == "candidates" and action in {"mod"}:
        invalid = [item.get("field", "").strip() for item in fields if item.get("field", "").strip() not in VALID_CANDIDATE_FIELDS]
        if invalid:
            raise ValueError(f"Invalid candidate fields: {invalid}")
        for item in fields:
            field = item.get("field", "").strip()
            value = item.get("value", "").strip()
            if field == "Verdict" and value not in VALID_VERDICTS:
                raise ValueError(f"Invalid Verdict value: {value}")
            if field == "Status" and value not in VALID_STATUS_VALUES:
                raise ValueError(f"Invalid Status value: {value}")
    if table == "evidence" and action in {"add", "mod"}:
        # Candidate is auto-injected by append_changes; ignore if model includes it
        non_auto_fields = [item for item in fields if item.get("field", "").strip() != "Candidate"]
        invalid = [item.get("field", "").strip() for item in non_auto_fields if item.get("field", "").strip() not in VALID_EVIDENCE_FIELDS]
        if invalid:
            raise ValueError(f"Invalid evidence fields: {invalid}")


def validate_proposals_for_candidate(
    candidate_row: dict[str, str],
    evidence_rows: list[dict[str, str]],
    proposed_changes: list[dict[str, Any]],
) -> None:
    candidate_name = candidate_row.get("Candidate", "").strip()
    evidence_lookup = {row.get("Evidence_ID", "").strip(): row for row in evidence_rows}
    deleted_evidence_ids: set[str] = set()
    added_evidence_count = 0

    for proposal in proposed_changes:
        validate_proposal(candidate_name, proposal)
        table = proposal.get("table", "").strip()
        action = proposal.get("action", "").strip()
        key = proposal.get("key", "").strip()
        fields = proposal.get("fields", [])

        if table == "candidates" and action == "mod":
            changed = False
            for item in fields:
                field = item.get("field", "").strip()
                new_value = item.get("value", "").strip()
                old_value = candidate_row.get(field, "").strip()
                if new_value != old_value:
                    changed = True
            if not changed:
                raise ValueError("Candidate mod proposal does not change any values")

        if table == "evidence" and action in {"mod", "del"}:
            if key not in evidence_lookup:
                raise ValueError(f"Evidence proposal key not found for candidate {candidate_name}: {key}")

        if table == "evidence" and action == "mod":
            current_row = evidence_lookup[key]
            changed = False
            for item in fields:
                field = item.get("field", "").strip()
                new_value = item.get("value", "").strip()
                old_value = current_row.get(field, "").strip()
                if new_value != old_value:
                    changed = True
            if not changed:
                raise ValueError(f"Evidence mod proposal does not change any values: {key}")

        if table == "evidence" and action == "del":
            deleted_evidence_ids.add(key)

        if table == "evidence" and action == "add":
            added_evidence_count += 1

    remaining_evidence_count = len(evidence_lookup) - len(deleted_evidence_ids) + added_evidence_count
    if evidence_lookup and deleted_evidence_ids and remaining_evidence_count <= 0:
        raise ValueError(
            f"Proposed evidence changes would leave {candidate_name} with zero evidence rows and no replacement source"
        )


def proposal_signature(proposal: dict[str, Any]) -> tuple[Any, ...]:
    atoms = tuple(sorted((item.get("field", "").strip(), item.get("value", "")) for item in proposal.get("fields", [])))
    return (
        proposal.get("table", "").strip(),
        proposal.get("key", "").strip(),
        proposal.get("action", "").strip(),
        atoms,
    )


def append_changes(changes: list[dict[str, str]], proposed_changes: list[dict[str, Any]], candidate_name: str) -> int:
    signatures = active_group_signatures(changes)
    appended = 0
    current_change_id = next_change_id(changes)
    for proposal in proposed_changes:
        signature = proposal_signature(proposal)
        if signature in signatures:
            continue
        reasoning = proposal.get("reasoning", "").strip()
        if proposal["action"] == "del":
            changes.append(
                {
                    "change_id": str(current_change_id),
                    "table": proposal["table"],
                    "key": proposal["key"],
                    "action": proposal["action"],
                    "reasoning": reasoning,
                    "field": "",
                    "value": "",
                    "status": "pending",
                }
            )
        else:
            # Strip any Candidate field the model may have included; it is re-added below
            field_changes = [f for f in proposal["fields"] if f.get("field", "").strip() != "Candidate"]
            if proposal["table"] == "evidence" and proposal["action"] == "add":
                field_changes = [{"field": "Candidate", "value": candidate_name}] + field_changes
            for field_change in field_changes:
                changes.append(
                    {
                        "change_id": str(current_change_id),
                        "table": proposal["table"],
                        "key": proposal["key"],
                        "action": proposal["action"],
                        "reasoning": reasoning,
                        "field": field_change["field"],
                        "value": field_change["value"],
                        "status": "pending",
                    }
                )
        signatures.add(signature)
        current_change_id += 1
        appended += 1
    return appended


def has_pending_race_increment(changes: list[dict[str, str]], key: str, reviewer_column: str) -> bool:
    for row in changes:
        if (
            row.get("table", "").strip() == "races"
            and row.get("key", "").strip() == key
            and row.get("action", "").strip() == "check"
            and row.get("field", "").strip() == reviewer_column
            and row.get("value", "").strip() == "+1"
            and row.get("status", "").strip() in {"pending", "approved"}
        ):
            return True
    return False


def append_race_increment(changes: list[dict[str, str]], race_row: dict[str, str], reviewer_column: str) -> bool:
    key = race_key(race_row)
    if has_pending_race_increment(changes, key, reviewer_column):
        return False
    change_id = next_change_id(changes)
    changes.append(
        {
            "change_id": str(change_id),
            "table": "races",
            "key": key,
            "action": "check",
            "reasoning": f"Completed {reviewer_column} review pass via race_runner.py",
            "field": reviewer_column,
            "value": "+1",
            "status": "pending",
        }
    )
    return True


def ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def log_response(race_row: dict[str, str], candidate_row: dict[str, str], prompt: str, stdout: str, stderr: str) -> None:
    ensure_log_dir()
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    race_slug = f'{race_row.get("State", "").strip()}-{race_row.get("Office", "").strip()}'.replace(" ", "_")
    candidate_slug = candidate_row.get("Candidate", "").strip().replace(" ", "_").replace("/", "_")
    base = LOG_DIR / f"{stamp}-{race_slug}-{candidate_slug}"
    (base.with_suffix(".prompt.txt")).write_text(prompt, encoding="utf-8")
    (base.with_suffix(".stdout.txt")).write_text(stdout, encoding="utf-8")
    (base.with_suffix(".stderr.txt")).write_text(stderr, encoding="utf-8")


def provider_env() -> dict[str, str]:
    env = dict(os.environ)
    node20 = Path("/opt/homebrew/opt/node@20/bin/node")
    if node20.exists():
        env["PATH"] = f"{node20.parent}:{env.get('PATH', '')}"
    return env


def run_claude(prompt: str, args: argparse.Namespace) -> dict[str, Any]:
    cli = resolve_cli("claude") or "claude"
    cmd = [
        cli,
        "-p",
        "--verbose",
        "--output-format",
        "stream-json",
        "--include-partial-messages",
        "--permission-mode",
        args.permission_mode,
        "--allowedTools",
        args.allowed_tools,
        "--disallowedTools",
        args.disallowed_tools,
        "--json-schema",
        json.dumps(load_schema()),
    ]
    if args.model:
        cmd.extend(["--model", args.model])
    if args.effort:
        cmd.extend(["--effort", args.effort])
    if args.dangerously_skip_permissions:
        cmd.append("--dangerously-skip-permissions")
    for extra_arg in args.claude_arg:
        cmd.append(extra_arg)
    cmd.append(prompt)
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=provider_env(),
        bufsize=1,
    )

    result_obj: dict[str, Any] | None = None
    start_time = time.time()
    stdout_text, stderr_joined, parsed_events = stream_subprocess_lines(proc, render_stream_event, start_time)
    for obj in parsed_events:
        if obj.get("type") == "result":
            result_obj = obj
    try:
        return_code = proc.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise ClaudeRunError(
            f"Claude timed out after {args.timeout}s",
            stdout=stdout_text,
            stderr=stderr_joined,
        )

    if return_code != 0:
        error_text = ""
        if result_obj is not None:
            result_message = result_obj.get("result")
            if isinstance(result_message, str):
                error_text = result_message.strip()
        if not error_text:
            error_text = stderr_joined.strip() or stdout_text.strip() or f"Claude exited with code {return_code}"
        raise ClaudeRunError(error_text, stdout=stdout_text, stderr=stderr_joined)
    if result_obj is None:
        raise ClaudeRunError("Claude stream ended without a result event", stdout=stdout_text, stderr=stderr_joined)
    try:
        data = extract_claude_payload(result_obj)
    except Exception as exc:  # noqa: BLE001
        raise ClaudeRunError(str(exc), stdout=stdout_text, stderr=stderr_joined) from exc
    return {
        "stdout": stdout_text,
        "stderr": stderr_joined,
        "data": data,
    }


def run_codex(prompt: str, args: argparse.Namespace) -> dict[str, Any]:
    cli = resolve_cli("codex") or "codex"
    fd, temp_path = tempfile.mkstemp(prefix="race-runner-codex-", suffix=".json")
    os.close(fd)
    output_file = Path(temp_path)
    cmd = [
        cli,
        "--search",
        "exec",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--json",
        "--output-schema",
        str(SCHEMA_PATH),
        "--output-last-message",
        str(output_file),
    ]
    if args.model:
        cmd.extend(["--model", args.model])
    cmd.append(prompt)
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=provider_env(),
        bufsize=1,
    )
    stdout_text, stderr_joined, _ = stream_subprocess_lines(proc, render_codex_event, time.time())
    try:
        return_code = proc.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        output_file.unlink(missing_ok=True)
        raise ClaudeRunError(f"Codex timed out after {args.timeout}s", stdout=stdout_text, stderr=stderr_joined)
    if return_code != 0:
        output_file.unlink(missing_ok=True)
        raise ClaudeRunError(stderr_joined.strip() or stdout_text.strip() or f"Codex exited with code {return_code}", stdout=stdout_text, stderr=stderr_joined)
    output = output_file.read_text(encoding="utf-8", errors="ignore").strip()
    output_file.unlink(missing_ok=True)
    if not output:
        raise ClaudeRunError("Codex produced empty structured output", stdout=stdout_text, stderr=stderr_joined)
    return {"stdout": stdout_text, "stderr": stderr_joined, "data": extract_generic_payload(output)}


def run_gemini(prompt: str, args: argparse.Namespace) -> dict[str, Any]:
    cli = resolve_cli("gemini") or "gemini"
    cmd = [cli, "-p"]
    if args.model:
        cmd.extend(["-m", args.model])
    cmd.append(prompt)
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=provider_env(),
        timeout=args.timeout,
    )
    if proc.returncode != 0:
        raise ClaudeRunError(proc.stderr.strip() or proc.stdout.strip() or f"Gemini exited with code {proc.returncode}", stdout=proc.stdout, stderr=proc.stderr)
    print(proc.stdout.strip(), file=sys.stderr, flush=True)
    return {"stdout": proc.stdout, "stderr": proc.stderr, "data": extract_generic_payload(proc.stdout)}


def run_provider(prompt: str, args: argparse.Namespace) -> dict[str, Any]:
    if args.provider == "claude":
        return run_claude(prompt, args)
    if args.provider == "codex":
        return run_codex(prompt, args)
    if args.provider == "gemini":
        return run_gemini(prompt, args)
    raise ValueError(f"Unsupported provider: {args.provider}")


def process_candidate(
    args: argparse.Namespace,
    changes: list[dict[str, str]],
    race_row: dict[str, str],
    candidate_row: dict[str, str],
    evidence_rows: list[dict[str, str]],
) -> tuple[bool, int]:
    prompt = build_prompt(
        candidate_row=candidate_row,
        evidence_rows=evidence_rows,
        change_rows=relevant_existing_changes(changes, candidate_row, evidence_rows),
        prompt_template=args.prompt_template,
    )
    if args.dry_run:
        print(f'DRY RUN: would process {candidate_row.get("Candidate", "").strip()} in {race_key(race_row)}', flush=True)
        return True, 0

    raw_stdout = ""
    raw_stderr = ""
    try:
        result = run_provider(prompt, args)
        raw_stdout = result["stdout"]
        raw_stderr = result["stderr"]
    except Exception as exc:
        if isinstance(exc, ClaudeRunError):
            raw_stdout = exc.stdout
            raw_stderr = exc.stderr
        elif isinstance(exc, RuntimeError):
            raw_stdout = str(exc)
        log_response(race_row, candidate_row, prompt, raw_stdout, raw_stderr)
        raise

    log_response(race_row, candidate_row, prompt, raw_stdout, raw_stderr)
    data = result["data"]
    proposed_changes = list(data.get("changes", []))
    validate_proposals_for_candidate(candidate_row, evidence_rows, proposed_changes)
    appended = append_changes(changes, proposed_changes, candidate_row.get("Candidate", "").strip())
    write_changes(changes)
    print(f'{candidate_row.get("Candidate", "").strip()}: proposed groups appended: {appended}', flush=True)
    return True, appended


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sequentially run one candidate at a time through a research model.")
    parser.add_argument("--provider", default="claude", choices=["claude", "codex", "gemini"], help="LLM provider to run")
    parser.add_argument("--race", help='Specific race key, e.g. "Arizona|Governor"')
    parser.add_argument("--candidate", help="Limit to a single candidate name within the selected race")
    parser.add_argument("--candidate-verdict", choices=sorted(VALID_VERDICTS), help="Only review candidates whose current Verdict matches this value")
    parser.add_argument("--skip-candidates-with-changes", action="store_true", help="Skip candidates that already have candidate/evidence rows in changes.csv")
    parser.add_argument("--max-races", type=int, default=1, help="How many races to process; 0 means all races")
    parser.add_argument(
        "--permission-mode",
        default="bypassPermissions",
        choices=["acceptEdits", "bypassPermissions", "default", "dontAsk", "plan", "auto"],
        help="Claude permission mode. Ignored by non-Claude providers.",
    )
    parser.add_argument("--dangerously-skip-permissions", action="store_true", help="Pass through Claude's dangerous skip flag")
    parser.add_argument("--model", default="", help="Provider-specific model alias or full model name")
    parser.add_argument("--effort", default="high", choices=["low", "medium", "high", "max"], help="Claude effort level")
    parser.add_argument(
        "--allowed-tools",
        default=DEFAULT_ALLOWED_TOOLS,
        help="Comma-separated Claude tools to allow",
    )
    parser.add_argument(
        "--disallowed-tools",
        default=DEFAULT_DISALLOWED_TOOLS,
        help="Comma-separated Claude tools to explicitly deny",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="Seconds before killing a Claude subprocess (default: 600)")
    parser.add_argument("--claude-arg", action="append", default=[], help="Extra raw argument to pass to the Claude CLI")
    parser.add_argument("--prompt-template", default=DEFAULT_PROMPT_TEMPLATE, help="Full prompt template with {today}, {candidate_json}, {evidence_json}, and {changes_json} placeholders")
    parser.add_argument("--stop-file", default="", help="Internal marker file used to request a graceful stop after the current candidate finishes")
    parser.add_argument("--dry-run", action="store_true", help="Select work and print what would run without calling Claude")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    reviewer_column = REVIEWER_COLUMNS.get(args.provider, "Claude")
    ensure_changes_csv()
    candidate_rows = read_csv(CANDIDATES_CSV)
    evidence_rows = read_csv(EVIDENCE_CSV)
    race_rows = read_csv(RACES_CSV)
    changes = read_csv(CHANGES_CSV)

    races = select_races(race_rows, args.race, args.max_races)
    if not races:
        raise SystemExit("No races selected")

    should_stop = stop_requested(args.stop_file)
    for race_row in races:
        if should_stop:
            print("Graceful stop requested; exiting before next race.", flush=True)
            break
        selected_candidates = candidates_for_race(candidate_rows, race_row, args.candidate, args.candidate_verdict)
        if args.skip_candidates_with_changes:
            selected_candidates = [
                row
                for row in selected_candidates
                if not candidate_has_existing_changes(
                    changes,
                    row,
                    [ev for ev in evidence_rows if ev.get("Candidate", "").strip() == row.get("Candidate", "").strip()],
                )
            ]
        if not selected_candidates:
            print(f'No candidates found for {race_key(race_row)}', flush=True)
            continue

        print(f'Processing race {race_key(race_row)} with {len(selected_candidates)} candidates', flush=True)
        race_ok = True
        race_total = 0

        for candidate_row in selected_candidates:
            if stop_requested(args.stop_file):
                should_stop = True
                print("Graceful stop requested; exiting before next candidate.", flush=True)
                break
            candidate_name = candidate_row.get("Candidate", "").strip()
            candidate_evidence = [row for row in evidence_rows if row.get("Candidate", "").strip() == candidate_name]
            try:
                ok, appended = process_candidate(
                    args=args,
                    changes=changes,
                    race_row=race_row,
                    candidate_row=candidate_row,
                    evidence_rows=candidate_evidence,
                )
                race_total += appended
                if not ok:
                    race_ok = False
                    break
            except Exception as exc:  # noqa: BLE001
                race_ok = False
                print(f"{candidate_name}: failed: {exc}", file=sys.stderr, flush=True)
                break
            if stop_requested(args.stop_file):
                should_stop = True
                print(f"Graceful stop requested; stopping after {candidate_name}.", flush=True)
                break

        if race_ok and args.dry_run:
            print(f'Dry run complete for {race_key(race_row)} | no changes were written', flush=True)
            if should_stop:
                break
            continue

        if race_ok:
            full_race = not args.candidate
            added_increment = append_race_increment(changes, race_row, reviewer_column) if full_race and not should_stop else False
            write_changes(changes)
            print(
                f'Completed race {race_key(race_row)} | change groups appended: {race_total} | '
                f'{reviewer_column} increment queued: {"yes" if added_increment else ("already pending" if (full_race and not should_stop) else ("skipped (partial run)" if not full_race else "skipped (graceful stop)"))}',
                flush=True,
            )
        else:
            write_changes(changes)
            print(f'Stopped race {race_key(race_row)} before queueing {reviewer_column} increment', file=sys.stderr, flush=True)
            break
        if should_stop:
            break


if __name__ == "__main__":
    main()
