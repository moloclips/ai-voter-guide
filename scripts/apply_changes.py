from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.changes_file import DEFAULT_CHANGES_FILENAME, resolve_changes_csv

CHANGES_CSV = resolve_changes_csv()
DATA_DIR = ROOT / "data"

TABLE_PATHS = {
    "candidates": DATA_DIR / "candidates.csv",
    "evidence": DATA_DIR / "evidence.csv",
    "races": DATA_DIR / "races.csv",
}

REVIEW_COLUMNS = ("D", "I")
VALID_STATUSES = {"pending", "approved", "denied", "applied", "conflict"}
MODEL_TO_REVIEW_COLUMN = {
    "claude": "Claude",
    "codex": "Codex",
    "gemini": "Gemini",
}


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_changes() -> list[dict[str, str]]:
    with CHANGES_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    for row in rows:
        for column in REVIEW_COLUMNS:
            if row.get(column, "").strip() and row.get(column, "").strip() not in VALID_STATUSES:
                row[column] = ""
    return rows


def write_changes(rows: list[dict[str, str]]) -> None:
    fieldnames = ["change_id", "table", "key", "action", "reasoning", "Model", "field", "value", "D", "Reasoning D", "I", "Reasoning I"]
    with CHANGES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def group_approved_changes(rows: list[dict[str, str]]) -> list[list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    order: list[str] = []
    for row in rows:
        if not all(row.get(column, "").strip() == "approved" for column in REVIEW_COLUMNS):
            continue
        change_id = row.get("change_id", "").strip()
        if not change_id:
            continue
        if change_id not in grouped:
            order.append(change_id)
        grouped[change_id].append(row)
    return [grouped[cid] for cid in order]


def race_key_parts(key: str) -> tuple[str, str]:
    state, sep, office = key.partition("|")
    if not sep or not state.strip() or not office.strip():
        raise ValueError(f"Invalid race key: {key}")
    return state.strip(), office.strip()


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


def find_row(rows: list[dict[str, str]], table: str, key: str) -> dict[str, str] | None:
    key = key.strip()
    if table == "candidates":
        return next((row for row in rows if candidate_key(row) == key), None)
    if table == "evidence":
        return next((row for row in rows if row.get("Evidence_ID", "").strip() == key), None)
    if table == "races":
        state, office = race_key_parts(key)
        return next(
            (row for row in rows if row.get("State", "").strip() == state and row.get("Office", "").strip() == office),
            None,
        )
    raise ValueError(f"Unknown table: {table}")


def next_evidence_id(rows: list[dict[str, str]]) -> str:
    nums = []
    for row in rows:
        value = str(row.get("Evidence_ID", "")).strip()
        if value.isdigit():
            nums.append(int(value))
    return str((max(nums) if nums else 0) + 1)


def infer_review_column(model_value: str) -> str | None:
    model = model_value.strip().lower()
    if not model:
        return None
    for needle, column in MODEL_TO_REVIEW_COLUMN.items():
        if needle in model:
            return column
    return None


def affected_candidate_key(group: list[dict[str, str]], tables: dict[str, tuple[list[str], list[dict[str, str]]]]) -> str:
    first = group[0]
    table = first["table"].strip()
    key = first["key"].strip()
    if table == "candidates":
        return key
    if table != "evidence":
        return ""
    if key:
        evidence_rows = tables["evidence"][1]
        target = find_row(evidence_rows, "evidence", key)
        if target is None:
            return ""
        return target.get("Candidate_Key", "").strip()
    for row in group:
        if row.get("field", "").strip() == "Candidate_Key":
            return row.get("value", "").strip()
    return ""


def increment_candidate_review_counts(
    pairs: set[tuple[str, str]],
    tables: dict[str, tuple[list[str], list[dict[str, str]]]],
) -> None:
    if not pairs:
        return
    candidate_rows = tables["candidates"][1]
    for candidate_id, column in pairs:
        target = find_row(candidate_rows, "candidates", candidate_id)
        if target is None or column not in target:
            continue
        current = int((target.get(column, "0") or "0").strip() or "0")
        target[column] = str(current + 1)


def apply_group(group: list[dict[str, str]], tables: dict[str, tuple[list[str], list[dict[str, str]]]]) -> None:
    first = group[0]
    table = first["table"].strip()
    action = first["action"].strip()
    key = first["key"].strip()
    fieldnames, rows = tables[table]

    for row in group[1:]:
        if row["table"].strip() != table or row["action"].strip() != action or row["key"].strip() != key:
            raise ValueError("Inconsistent rows within change_id group")

    if action == "mod":
        target = find_row(rows, table, key)
        if target is None:
            raise KeyError(f"Missing target row for {table}:{key}")
        for row in group:
            field = row["field"].strip()
            if field not in fieldnames:
                raise KeyError(f"Unknown field {field} for table {table}")
            target[field] = row["value"]
        return

    if action == "del":
        target = find_row(rows, table, key)
        if target is None:
            raise KeyError(f"Missing target row for {table}:{key}")
        rows.remove(target)
        return

    if action == "add":
        new_row = {field: "" for field in fieldnames}
        for row in group:
            field = row["field"].strip()
            if field not in fieldnames:
                raise KeyError(f"Unknown field {field} for table {table}")
            new_row[field] = row["value"]
        if table == "evidence":
            new_row["Evidence_ID"] = next_evidence_id(rows)
            if not new_row.get("Candidate", "").strip():
                candidate_id = new_row.get("Candidate_Key", "").strip()
                if candidate_id:
                    new_row["Candidate"] = candidate_id.split("|", 2)[-1]
        rows.append(new_row)
        return

    raise ValueError(f"Unsupported action: {action}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Apply approved changes from a selected changes CSV.")
    parser.add_argument("--changes-file", default=DEFAULT_CHANGES_FILENAME, help="Changes CSV filename in the repo root")
    args = parser.parse_args()
    global CHANGES_CSV
    CHANGES_CSV = resolve_changes_csv(args.changes_file)
    changes = load_changes()
    tables = {name: load_csv(path) for name, path in TABLE_PATHS.items()}
    approved_groups = group_approved_changes(changes)

    change_rows_by_id: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in changes:
        change_rows_by_id[row.get("change_id", "").strip()].append(row)

    review_increments: set[tuple[str, str]] = set()
    for group in approved_groups:
        change_id = group[0]["change_id"].strip()
        try:
            candidate_id = affected_candidate_key(group, tables)
            apply_group(group, tables)
        except Exception:
            for row in change_rows_by_id[change_id]:
                if all(row.get(column, "").strip() == "approved" for column in REVIEW_COLUMNS):
                    for column in REVIEW_COLUMNS:
                        row[column] = "conflict"
            continue
        review_column = infer_review_column(group[0].get("Model", ""))
        if candidate_id and review_column:
            review_increments.add((candidate_id, review_column))
        for row in change_rows_by_id[change_id]:
            if all(row.get(column, "").strip() == "approved" for column in REVIEW_COLUMNS):
                for column in REVIEW_COLUMNS:
                    row[column] = "applied"

    increment_candidate_review_counts(review_increments, tables)
    for table, (fieldnames, rows) in tables.items():
        write_csv(TABLE_PATHS[table], fieldnames, rows)
    write_changes(changes)
    print(f"Applied approved changes. Groups processed: {len(approved_groups)}")


if __name__ == "__main__":
    main()
