from __future__ import annotations

import argparse
import csv
import shutil
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
CHANGES_CSV = ROOT / "changes.csv"
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
REPORTS_DIR = ROOT / "reports"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def model_to_column(model: str) -> str:
    low = model.strip().lower()
    if "claude" in low:
        return "Claude"
    if "gemini" in low:
        return "Gemini"
    if "codex" in low or low.startswith("gpt-"):
        return "Codex"
    return ""


def candidate_key_from_group(rows: list[dict[str, str]], evidence_by_id: dict[str, dict[str, str]]) -> str:
    first = rows[0]
    table = first.get("table", "").strip()
    key = first.get("key", "").strip()
    if table == "candidates":
        return key
    for row in rows:
        if row.get("field", "").strip() == "Candidate_Key" and row.get("value", "").strip():
            return row.get("value", "").strip()
    if key and key in evidence_by_id:
        return evidence_by_id[key].get("Candidate_Key", "").strip()
    return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill candidate review counters from existing queued changes.")
    parser.add_argument("--backup", action="store_true", help="Write a timestamp-free backup copy to reports/ before modifying candidates.csv")
    args = parser.parse_args()

    candidate_rows = read_csv(CANDIDATES_CSV)
    change_rows = read_csv(CHANGES_CSV)
    evidence_rows = read_csv(EVIDENCE_CSV)
    evidence_by_id = {row.get("Evidence_ID", "").strip(): row for row in evidence_rows if row.get("Evidence_ID", "").strip()}
    by_group: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in change_rows:
        change_id = row.get("change_id", "").strip()
        if change_id:
            by_group[change_id].append(row)

    needed: set[tuple[str, str]] = set()
    for rows in by_group.values():
        model = rows[0].get("Model", "").strip()
        review_column = model_to_column(model)
        if not review_column:
            continue
        candidate_key = candidate_key_from_group(rows, evidence_by_id)
        if candidate_key:
            needed.add((candidate_key, review_column))

    if args.backup:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        backup_path = REPORTS_DIR / "candidates.pre_review_count_backfill.csv"
        shutil.copy2(CANDIDATES_CSV, backup_path)
        print(f"Backup written to {backup_path}")

    updated = 0
    for row in candidate_rows:
        candidate_key = row.get("Candidate_Key", "").strip()
        if not candidate_key:
            continue
        for needed_key, review_column in needed:
            if candidate_key != needed_key:
                continue
            current = (row.get(review_column, "") or "0").strip()
            if not current or current == "0":
                row[review_column] = "1"
                updated += 1
            break

    write_csv(CANDIDATES_CSV, candidate_rows)
    print(f"Candidate/provider pairs found in changes.csv: {len(needed)}")
    print(f"Candidate rows updated in candidates.csv: {updated}")


if __name__ == "__main__":
    main()
