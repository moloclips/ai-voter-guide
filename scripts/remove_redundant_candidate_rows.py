from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CHANGES_CSV = ROOT / "changes.csv"
FIELDNAMES = ["change_id", "table", "key", "action", "reasoning", "field", "value", "D", "Reasoning D", "I", "Reasoning I"]


def load_rows() -> list[dict[str, str]]:
    with CHANGES_CSV.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_rows(rows: list[dict[str, str]]) -> None:
    with CHANGES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = load_rows()
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("change_id", "").strip()].append(row)

    removed = 0
    kept_rows: list[dict[str, str]] = []
    for row in rows:
        change_id = row.get("change_id", "").strip()
        group = grouped.get(change_id, [])
        is_evidence_add = any(
            item.get("table", "").strip() == "evidence" and item.get("action", "").strip() == "add"
            for item in group
        )
        has_candidate_key = any(
            item.get("field", "").strip() == "Candidate_Key" and item.get("value", "").strip()
            for item in group
        )
        if is_evidence_add and has_candidate_key and row.get("field", "").strip() == "Candidate":
            removed += 1
            continue
        kept_rows.append(row)

    write_rows(kept_rows)
    print(f"Removed redundant Candidate rows from evidence add groups: {removed}")


if __name__ == "__main__":
    main()
