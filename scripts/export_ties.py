from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
CANDIDATES_CSV = DATA_DIR / "candidates.csv"
EVIDENCE_CSV = DATA_DIR / "evidence.csv"
OUTPUT_JSON = DATA_DIR / "ties.json"

EXCLUDED_STATUS_TERMS = (
    "removed from ballot",
    "failed to qualify",
    "exploratory",
    "potential candidate",
)

SAFE_VERDICT = "nice"


def load_race_ties() -> dict[str, list[str]]:
    races: dict[tuple[str, str], list[tuple[int, str]]] = {}

    with CANDIDATES_CSV.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            candidate = row["Candidate"].strip()
            state = row["State"].strip()
            office = row["Office"].strip()
            party = row["Party"].strip()
            status = row["Status"].strip()
            verdict = row["Verdict"].strip()
            preference = row.get("Preference", "").strip()
            if not all((candidate, state, office, party, status)):
                continue
            if any(term in status.lower() for term in EXCLUDED_STATUS_TERMS):
                continue
            if verdict != SAFE_VERDICT:
                continue

            pref_value = int(preference) if preference.isdigit() else 999
            races.setdefault((state, office), []).append((pref_value, candidate))

    ties: dict[str, list[str]] = {}
    for (state, office), candidates in sorted(races.items()):
        if len(candidates) > 1:
            ties[f"{state} | {office}"] = [
                candidate
                for _, candidate in sorted(candidates, key=lambda item: (item[0], item[1]))
            ]
    return ties


def main() -> None:
    ties = load_race_ties()
    OUTPUT_JSON.write_text(json.dumps(ties, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {OUTPUT_JSON}")
    print(f"Races with >1 {SAFE_VERDICT} candidate: {len(ties)}")


if __name__ == "__main__":
    main()
