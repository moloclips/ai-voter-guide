from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.changes_file import resolve_changes_csv

RACES_CSV = ROOT / "data" / "races.csv"
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
CHANGE_FIELDNAMES = [
    "change_id",
    "table",
    "key",
    "action",
    "reasoning",
    "Model",
    "field",
    "value",
    "D",
    "Reasoning D",
    "I",
    "Reasoning I",
]


def candidate_key(row: dict[str, str]) -> str:
    existing = row.get("Candidate_Key", "").strip()
    if existing:
        return existing
    candidate = row.get("Candidate", "").strip()
    state = row.get("State", "").strip()
    office = row.get("Office", "").strip()
    if candidate and state and office:
        return f"{state}|{office}|{candidate}"
    return candidate


def race_key(row: dict[str, str]) -> str:
    state = row.get("State", "").strip()
    office = row.get("Office", "").strip()
    if state and office:
        return f"{state}|{office}"
    return ""


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_changes(changes_path: Path) -> list[dict[str, str]]:
    rows = load_csv(changes_path)
    return [{field: row.get(field, "").strip() for field in CHANGE_FIELDNAMES} for row in rows]


def format_row(row: dict[str, str], indent: str = "  ") -> str:
    if not row:
        return f"{indent}(none)"
    return "\n".join(f"{indent}{key}: {value}" for key, value in row.items())


def format_change_group(group: list[dict[str, str]], indent: str = "  ") -> str:
    first = group[0]
    lines = [
        f"{indent}change_id: {first.get('change_id', '')}",
        f"{indent}table: {first.get('table', '')}",
        f"{indent}key: {first.get('key', '') or '(blank)'}",
        f"{indent}action: {first.get('action', '')}",
        f"{indent}model: {first.get('Model', '')}",
        f"{indent}D: {first.get('D', '') or 'pending'}",
        f"{indent}Reasoning D: {first.get('Reasoning D', '')}",
        f"{indent}I: {first.get('I', '') or 'pending'}",
        f"{indent}Reasoning I: {first.get('Reasoning I', '')}",
        f"{indent}reasoning: {first.get('reasoning', '')}",
    ]
    field_rows = [row for row in group if row.get("field", "").strip()]
    if field_rows:
        lines.append(f"{indent}fields:")
        for row in field_rows:
            lines.append(f"{indent}  {row['field']}: {row.get('value', '')}")
    return "\n".join(lines)


def find_candidate_context(candidate_id: str, changes_path: Path) -> str:
    races = load_csv(RACES_CSV)
    candidates = load_csv(CANDIDATES_CSV)
    evidence = load_csv(EVIDENCE_CSV)
    changes = load_changes(changes_path)

    candidate_row = next((row for row in candidates if candidate_key(row) == candidate_id), {})
    if not candidate_row:
        raise SystemExit(f"Candidate key not found: {candidate_id}")

    office = candidate_row.get("Office", "").strip()
    state = candidate_row.get("State", "").strip()
    race_row = next(
        (
            row
            for row in races
            if row.get("State", "").strip() == state and row.get("Office", "").strip() == office
        ),
        {},
    )

    evidence_rows = [row for row in evidence if row.get("Candidate_Key", "").strip() == candidate_id]
    evidence_ids = {row.get("Evidence_ID", "").strip() for row in evidence_rows if row.get("Evidence_ID", "").strip()}

    grouped_changes: dict[str, list[dict[str, str]]] = {}
    for row in changes:
        grouped_changes.setdefault(row.get("change_id", "").strip(), []).append(row)

    related_groups: dict[str, list[dict[str, str]]] = {}
    for change_id, group in grouped_changes.items():
        if not change_id:
            continue
        is_related = False
        for row in group:
            table = row.get("table", "")
            key = row.get("key", "").strip()
            value = row.get("value", "").strip()
            field = row.get("field", "").strip()

            if table == "candidates" and key == candidate_id:
                is_related = True
                break
            if table == "evidence":
                if key and key in evidence_ids:
                    is_related = True
                    break
                if field == "Candidate_Key" and value == candidate_id:
                    is_related = True
                    break
            if table == "races" and key == race_key(race_row):
                is_related = True
                break

        if is_related:
            related_groups[change_id] = group

    lines = [
        f"Candidate key: {candidate_id}",
        "",
        "Race row:",
        format_row(race_row),
        "",
        "Candidate row:",
        format_row(candidate_row),
        "",
        f"Evidence rows ({len(evidence_rows)}):",
    ]

    if evidence_rows:
        for idx, row in enumerate(evidence_rows, start=1):
            lines.extend(["", f"Evidence {idx}:", format_row(row)])
    else:
        lines.append("  (none)")

    lines.extend(["", f"Related change groups ({len(related_groups)}):"])
    if related_groups:
        for change_id in sorted(related_groups, key=lambda value: int(value) if value.isdigit() else value):
            lines.extend(["", format_change_group(related_groups[change_id])])
    else:
        lines.append("  (none)")

    return "\n".join(lines) + "\n"


def find_change_context(change_id: str, changes_path: Path) -> str:
    changes = load_changes(changes_path)
    candidates = load_csv(CANDIDATES_CSV)
    evidence = load_csv(EVIDENCE_CSV)

    group = [row for row in changes if row.get("change_id", "").strip() == change_id]
    if not group:
        raise SystemExit(f"Change id not found: {change_id}")

    first = group[0]
    table = first["table"]
    key = first["key"].strip()

    if table == "candidates":
        candidate_id = key
    elif table == "evidence":
        if key:
            evidence_row = next((row for row in evidence if row.get("Evidence_ID", "").strip() == key), {})
            candidate_id = evidence_row.get("Candidate_Key", "").strip()
        else:
            candidate_id = next(
                (
                    row.get("value", "").strip()
                    for row in group
                    if row.get("field", "").strip() == "Candidate_Key" and row.get("value", "").strip()
                ),
                "",
            )
    else:
        candidate_id = ""

    lines = [
        f"Change ID: {change_id}",
        f"Table: {first.get('table', '')}",
        f"Key: {key or '(blank)'}",
        f"Action: {first.get('action', '')}",
        "",
        "Change group:",
        format_change_group(group),
    ]

    if candidate_id and any(candidate_key(row) == candidate_id for row in candidates):
        lines.extend(
            [
                "",
                "Candidate context:",
                find_candidate_context(candidate_id, changes_path).rstrip(),
            ]
        )

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Show either full context for a change_id or full dataset context for a candidate key."
    )
    parser.add_argument("value", help="Change id or candidate key")
    parser.add_argument(
        "--mode",
        choices=["auto", "change", "candidate"],
        default="auto",
        help="Interpret the value as a change id or candidate key. Defaults to auto.",
    )
    parser.add_argument("--changes-file", default="changes.csv", help="Changes CSV filename to inspect")
    args = parser.parse_args()
    changes_path = resolve_changes_csv(args.changes_file)

    if args.mode == "change":
        print(find_change_context(args.value, changes_path), end="")
        return

    if args.mode == "candidate":
        print(find_candidate_context(args.value, changes_path), end="")
        return

    if "|" in args.value:
        print(find_candidate_context(args.value, changes_path), end="")
    else:
        print(find_change_context(args.value, changes_path), end="")


if __name__ == "__main__":
    main()
