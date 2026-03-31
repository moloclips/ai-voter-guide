from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT / ".claude" / "race_runner_logs"
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
DEFAULT_OUTPUT = ROOT / "data" / "race_runner_log_summary.csv"
TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"
STATE_ABBREVIATIONS = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
    "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
    "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT",
    "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}


@dataclass
class RunRecord:
    timestamp: datetime
    candidate: str
    state: str
    race: str
    provider: str
    status: str
    model: str
    web_search_count: int
    web_fetch_count: int
    command_execution_count: int
    todo_list_count: int
    rate_limit_count: int
    claude_turns: int
    claude_input_tokens: int
    claude_cached_input_tokens: int
    claude_output_tokens: int
    claude_cost_usd: float
    codex_input_tokens: int
    codex_output_tokens: int


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def candidate_slug(name: str) -> str:
    return name.strip().replace(" ", "_").replace("/", "_")


def race_slug(state: str, office: str) -> str:
    return f"{state.strip()}-{office.strip()}".replace(" ", "_")


def parse_log_identity(base_name: str, race_map: dict[str, tuple[str, str]], candidate_map: dict[str, str]) -> tuple[datetime, str, str, str] | None:
    if len(base_name) < 17 or base_name[8] != "-" or "-" not in base_name[15:]:
        return None
    stamp = base_name[:15]
    try:
        timestamp = datetime.strptime(stamp, TIMESTAMP_FORMAT)
    except ValueError:
        return None
    remainder = base_name[16:]
    for cand_slug, candidate in sorted(candidate_map.items(), key=lambda item: len(item[0]), reverse=True):
        suffix = f"-{cand_slug}"
        if not remainder.endswith(suffix):
            continue
        race_part = remainder[: -len(suffix)]
        if race_part in race_map:
            state, race = race_map[race_part]
            return timestamp, candidate, state, race
    return None


def detect_provider(events: list[dict[str, Any]]) -> str:
    if not events:
        return "unknown"
    first_type = str(events[0].get("type", "")).strip()
    if first_type == "system":
        return "claude"
    if first_type == "thread.started":
        return "codex"
    return "unknown"


def parse_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            events.append(obj)
    return events


def claude_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = {
        "status": "unknown",
        "model": "",
        "web_search_count": 0,
        "web_fetch_count": 0,
        "command_execution_count": 0,
        "todo_list_count": 0,
        "rate_limit_count": 0,
        "claude_turns": 0,
        "claude_input_tokens": 0,
        "claude_cached_input_tokens": 0,
        "claude_output_tokens": 0,
        "claude_cost_usd": 0.0,
        "codex_input_tokens": 0,
        "codex_cached_input_tokens": 0,
        "codex_output_tokens": 0,
    }
    for event in events:
        event_type = str(event.get("type", "")).strip()
        if event_type == "system" and str(event.get("subtype", "")).strip() == "init":
            metrics["model"] = str(event.get("model", "")).strip()
        elif event_type == "assistant":
            content = (event.get("message") or {}).get("content") or []
            for block in content:
                if not isinstance(block, dict):
                    continue
                if str(block.get("type", "")).strip() != "tool_use":
                    continue
                name = str(block.get("name", "")).strip()
                if name == "WebSearch":
                    metrics["web_search_count"] += 1
                elif name == "WebFetch":
                    metrics["web_fetch_count"] += 1
        elif event_type == "rate_limit_event":
            metrics["rate_limit_count"] += 1
        elif event_type == "result":
            metrics["status"] = str(event.get("subtype", "")).strip() or ("failed" if event.get("is_error") else "success")
            metrics["claude_turns"] = int(event.get("num_turns") or 0)
            usage = event.get("usage") or {}
            metrics["claude_input_tokens"] = int(usage.get("input_tokens") or 0)
            metrics["claude_cached_input_tokens"] = int(usage.get("cache_creation_input_tokens") or usage.get("cache_read_input_tokens") or 0)
            metrics["claude_output_tokens"] = int(usage.get("output_tokens") or 0)
            metrics["claude_cost_usd"] = float(event.get("total_cost_usd") or 0.0)
    return metrics


def codex_metrics(events: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = {
        "status": "unknown",
        "model": "",
        "web_search_count": 0,
        "web_fetch_count": 0,
        "command_execution_count": 0,
        "todo_list_count": 0,
        "rate_limit_count": 0,
        "claude_turns": 0,
        "claude_input_tokens": 0,
        "claude_cached_input_tokens": 0,
        "claude_output_tokens": 0,
        "claude_cost_usd": 0.0,
        "codex_input_tokens": 0,
        "codex_cached_input_tokens": 0,
        "codex_output_tokens": 0,
    }
    for event in events:
        event_type = str(event.get("type", "")).strip()
        if event_type == "item.started":
            item = event.get("item") or {}
            item_type = str(item.get("type", "")).strip()
            if item_type == "web_search":
                metrics["web_search_count"] += 1
            elif item_type == "command_execution":
                metrics["command_execution_count"] += 1
            elif item_type == "todo_list":
                metrics["todo_list_count"] += 1
        elif event_type == "turn.completed":
            metrics["status"] = "success"
            usage = event.get("usage") or {}
            metrics["codex_input_tokens"] = int(usage.get("input_tokens") or 0)
            metrics["codex_cached_input_tokens"] = int(usage.get("cached_input_tokens") or 0)
            metrics["codex_output_tokens"] = int(usage.get("output_tokens") or 0)
        elif event_type in {"turn.failed", "error"}:
            metrics["status"] = "failed"
    return metrics


def candidate_label(candidate: str, state: str, race: str) -> str:
    if race.startswith("House "):
        return f"{candidate} ({race})"
    state_abbrev = STATE_ABBREVIATIONS.get(state, state[:2].upper())
    return f"{candidate} ({state_abbrev} - {race})"


def collect_runs() -> list[RunRecord]:
    candidates = read_csv(CANDIDATES_CSV)
    candidate_map = {candidate_slug(row.get("Candidate", "")): row.get("Candidate", "").strip() for row in candidates}
    race_map = {
        race_slug(row.get("State", ""), row.get("Office", "")): (row.get("State", "").strip(), row.get("Office", "").strip())
        for row in candidates
    }
    runs: list[RunRecord] = []
    for stdout_path in sorted(LOG_DIR.glob("*.stdout.txt")):
        base_name = stdout_path.name[: -len(".stdout.txt")]
        parsed = parse_log_identity(base_name, race_map, candidate_map)
        if parsed is None:
            continue
        started_at, candidate, state, race = parsed
        events = parse_events(stdout_path)
        provider = detect_provider(events)
        metrics = claude_metrics(events) if provider == "claude" else codex_metrics(events) if provider == "codex" else {
            "status": "unknown",
            "model": "",
            "web_search_count": 0,
            "web_fetch_count": 0,
            "command_execution_count": 0,
            "todo_list_count": 0,
            "rate_limit_count": 0,
            "claude_turns": 0,
            "claude_input_tokens": 0,
            "claude_cached_input_tokens": 0,
            "claude_output_tokens": 0,
            "claude_cost_usd": 0.0,
            "codex_input_tokens": 0,
            "codex_cached_input_tokens": 0,
            "codex_output_tokens": 0,
        }
        runs.append(
            RunRecord(
                timestamp=started_at,
                candidate=candidate,
                state=state,
                race=race,
                provider=provider,
                status=str(metrics["status"]),
                model=str(metrics["model"]),
                web_search_count=int(metrics["web_search_count"]),
                web_fetch_count=int(metrics["web_fetch_count"]),
                command_execution_count=int(metrics["command_execution_count"]),
                todo_list_count=int(metrics["todo_list_count"]),
                rate_limit_count=int(metrics["rate_limit_count"]),
                claude_turns=int(metrics["claude_turns"]),
                claude_input_tokens=int(metrics["claude_input_tokens"]),
                claude_cached_input_tokens=int(metrics["claude_cached_input_tokens"]),
                claude_output_tokens=int(metrics["claude_output_tokens"]),
                claude_cost_usd=float(metrics["claude_cost_usd"]),
                codex_input_tokens=int(metrics["codex_input_tokens"]),
                codex_output_tokens=int(metrics["codex_output_tokens"]),
            )
        )
    return runs


def summarize_runs(runs: list[RunRecord]) -> list[dict[str, str]]:
    grouped: dict[str, list[RunRecord]] = defaultdict(list)
    for run in runs:
        grouped[run.candidate].append(run)

    rows: list[dict[str, str]] = []
    for candidate, candidate_runs in sorted(grouped.items()):
        candidate_runs.sort(key=lambda run: run.timestamp)
        latest = candidate_runs[-1]
        model_value = latest.model or latest.provider
        row = {
            "Candidate": candidate_label(candidate, latest.state, latest.race),
            "Succeeded/Total": f"{sum(1 for run in candidate_runs if run.status == 'success')}/{len(candidate_runs)}",
            "Latest_Run": latest.timestamp.isoformat(sep=" "),
            "Model": model_value,
            "Total_Web_Lookups": str(sum(run.web_search_count + run.web_fetch_count for run in candidate_runs)),
            "Total_Command_Executions": str(sum(run.command_execution_count for run in candidate_runs)),
            "Total_Todo_Lists": str(sum(run.todo_list_count for run in candidate_runs)),
            "Total_Rate_Limits": str(sum(run.rate_limit_count for run in candidate_runs)),
            "Total_Turns": str(sum(run.claude_turns for run in candidate_runs)),
            "Input_Tokens": str(sum(run.claude_input_tokens + run.codex_input_tokens for run in candidate_runs)),
            "Output_Tokens": str(sum(run.claude_output_tokens + run.codex_output_tokens for run in candidate_runs)),
            "Claude_Total_Cost_USD": f"{sum(run.claude_cost_usd for run in candidate_runs):.6f}",
        }
        rows.append(row)
    return rows


def write_summary(rows: list[dict[str, str]], output_path: Path) -> None:
    fieldnames = [
        "Candidate",
        "Succeeded/Total",
        "Latest_Run",
        "Model",
        "Total_Web_Lookups",
        "Total_Command_Executions",
        "Total_Todo_Lists",
        "Total_Rate_Limits",
        "Total_Turns",
        "Input_Tokens",
        "Output_Tokens",
        "Claude_Total_Cost_USD",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    runs = collect_runs()
    rows = summarize_runs(runs)
    write_summary(rows, DEFAULT_OUTPUT)
    print(f"Wrote {len(rows)} candidate log summaries to {DEFAULT_OUTPUT}")


if __name__ == "__main__":
    main()
