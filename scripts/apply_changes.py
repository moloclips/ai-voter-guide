from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CHANGES_CSV = ROOT / "changes.csv"
DATA_DIR = ROOT / "data"

TABLE_PATHS = {
    "candidates": DATA_DIR / "candidates.csv",
    "evidence": DATA_DIR / "evidence.csv",
    "races": DATA_DIR / "races.csv",
}

VALID_STATUSES = {"pending", "approved", "denied", "applied", "conflict"}


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
        if row.get("status", "").strip() not in VALID_STATUSES:
            row["status"] = "pending"
    return rows


def write_changes(rows: list[dict[str, str]]) -> None:
    fieldnames = ["change_id", "table", "key", "action", "reasoning", "field", "value", "status"]
    with CHANGES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def group_approved_changes(rows: list[dict[str, str]]) -> list[list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    order: list[str] = []
    for row in rows:
        if row.get("status", "").strip() != "approved":
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


def find_row(rows: list[dict[str, str]], table: str, key: str) -> dict[str, str] | None:
    key = key.strip()
    if table == "candidates":
        return next((row for row in rows if row.get("Candidate", "").strip() == key), None)
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

    if action == "check":
        if table != "races":
            raise ValueError("check action is only supported for races")
        target = find_row(rows, table, key)
        if target is None:
            raise KeyError(f"Missing target row for {table}:{key}")
        for row in group:
            field = row["field"].strip()
            if field not in fieldnames:
                raise KeyError(f"Unknown race field {field}")
            delta = int((row["value"] or "0").strip())
            current = int((target.get(field, "0") or "0").strip())
            target[field] = str(current + delta)
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
        rows.append(new_row)
        return

    raise ValueError(f"Unsupported action: {action}")


def main() -> None:
    changes = load_changes()
    tables = {name: load_csv(path) for name, path in TABLE_PATHS.items()}
    approved_groups = group_approved_changes(changes)

    change_rows_by_id: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in changes:
        change_rows_by_id[row.get("change_id", "").strip()].append(row)

    for group in approved_groups:
        change_id = group[0]["change_id"].strip()
        try:
            apply_group(group, tables)
        except Exception:
            for row in change_rows_by_id[change_id]:
                if row.get("status", "").strip() == "approved":
                    row["status"] = "conflict"
            continue
        for row in change_rows_by_id[change_id]:
            if row.get("status", "").strip() == "approved":
                row["status"] = "applied"

    for table, (fieldnames, rows) in tables.items():
        write_csv(TABLE_PATHS[table], fieldnames, rows)
    write_changes(changes)
    print(f"Applied approved changes. Groups processed: {len(approved_groups)}")


if __name__ == "__main__":
    main()
