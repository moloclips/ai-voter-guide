#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
DEFAULT_OUTPUT_CSV = ROOT / "reports" / "active_review.csv"

CSV_FIELDNAMES = [
    "candidate_key",
    "candidate",
    "state",
    "office",
    "status",
    "current_active",
    "proposed_active",
    "reason",
]

OUT_SUBSTRINGS = (
    "removed from ballot",
    "failed to qualify",
    "exploratory",
    "potential candidate",
)


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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


def infer_active(status: str) -> tuple[str, str]:
    normalized = status.strip()
    lowered = normalized.lower()
    if not normalized:
        return "Out", "blank status"
    if normalized == "Out":
        return "Out", "explicit Out status"
    for term in OUT_SUBSTRINGS:
        if term in lowered:
            return "Out", f"status contains '{term}'"
    return "In", "status indicates candidate is still active/in the field"


def review_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    reviewed: list[dict[str, str]] = []
    for row in rows:
        status = row.get("Status", "").strip()
        current_active = row.get("Active", "").strip()
        proposed_active, reason = infer_active(status)
        reviewed.append({
            "candidate_key": candidate_key(row),
            "candidate": row.get("Candidate", "").strip(),
            "state": row.get("State", "").strip(),
            "office": row.get("Office", "").strip(),
            "status": status,
            "current_active": current_active,
            "proposed_active": proposed_active,
            "reason": reason,
        })
    return reviewed


def apply_active(rows: list[dict[str, str]], fieldnames: list[str]) -> tuple[list[str], int]:
    updated = 0
    if "Active" not in fieldnames:
        insert_at = fieldnames.index("Status") + 1 if "Status" in fieldnames else len(fieldnames)
        fieldnames = fieldnames[:insert_at] + ["Active"] + fieldnames[insert_at:]
    for row in rows:
        proposed, _ = infer_active(row.get("Status", "").strip())
        if row.get("Active", "").strip() != proposed:
            row["Active"] = proposed
            updated += 1
    for row in rows:
        for field in fieldnames:
            row.setdefault(field, "")
    return fieldnames, updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Review and optionally populate candidates.csv Active values from Status.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_CSV), help="CSV report path")
    parser.add_argument("--apply", action="store_true", help="Write proposed Active values into data/candidates.csv")
    args = parser.parse_args()

    fieldnames, candidate_rows = load_csv(CANDIDATES_CSV)
    reviewed = review_rows(candidate_rows)
    write_csv(Path(args.output), CSV_FIELDNAMES, reviewed)
    print(f"Wrote review CSV: {args.output}")

    if args.apply:
        updated_fieldnames, updated = apply_active(candidate_rows, fieldnames)
        write_csv(CANDIDATES_CSV, updated_fieldnames, candidate_rows)
        print(f"Updated Active values in {CANDIDATES_CSV} ({updated} row(s) changed)")


if __name__ == "__main__":
    main()
