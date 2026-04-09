from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
CHANGES_CSV = ROOT / "changes.csv"
REPORT_PATH = ROOT / "reports" / "candidate_key_migration_report.txt"
CHANGE_FIELDNAMES = ["change_id", "table", "key", "action", "reasoning", "field", "value", "D", "Reasoning D", "I", "Reasoning I"]


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def candidate_key(row: dict[str, str]) -> str:
    existing = row.get("Candidate_Key", "").strip()
    if existing:
        return existing
    return f'{row.get("State", "").strip()}|{row.get("Office", "").strip()}|{row.get("Candidate", "").strip()}'


def main() -> None:
    candidates = load_csv(CANDIDATES_CSV)
    evidence = load_csv(EVIDENCE_CSV)
    changes = load_csv(CHANGES_CSV)

    by_name: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidates:
        row["Candidate_Key"] = candidate_key(row)
        by_name[row.get("Candidate", "").strip()].append(row)

    evidence_fieldnames = list(evidence[0].keys()) if evidence else ["Evidence_ID", "Candidate", "Source_Description", "URL"]
    if "Candidate_Key" not in evidence_fieldnames:
        evidence_fieldnames.insert(1, "Candidate_Key")
    unresolved_evidence: list[str] = []
    for row in evidence:
        name = row.get("Candidate", "").strip()
        matches = by_name.get(name, [])
        if len(matches) == 1:
            row["Candidate_Key"] = matches[0]["Candidate_Key"]
        else:
            row["Candidate_Key"] = row.get("Candidate_Key", "").strip()
            if not row["Candidate_Key"]:
                unresolved_evidence.append(
                    f"Evidence_ID {row.get('Evidence_ID','').strip()}: {name} -> {[candidate_key(m) for m in matches]}"
                )

    ambiguous_candidate_changes: list[str] = []
    ambiguous_evidence_adds: list[str] = []
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    order: list[str] = []
    for row in changes:
        cid = row.get("change_id", "").strip()
        if cid not in grouped:
            order.append(cid)
        grouped[cid].append(row)

    new_changes: list[dict[str, str]] = []
    for cid in order:
        group = grouped[cid]
        first = group[0]
        table = first.get("table", "").strip()
        if table == "candidates":
            key = first.get("key", "").strip()
            matches = by_name.get(key, [])
            if "|" in key:
                for row in group:
                    new_changes.append(row)
            elif len(matches) == 1:
                candidate_id = matches[0]["Candidate_Key"]
                for row in group:
                    row["key"] = candidate_id
                    new_changes.append(row)
            else:
                ambiguous_candidate_changes.append(f"change_id {cid}: {key} -> {[candidate_key(m) for m in matches]}")
                for row in group:
                    new_changes.append(row)
            continue

        if table == "evidence" and not first.get("key", "").strip():
            has_candidate_key = any(r.get("field", "").strip() == "Candidate_Key" for r in group)
            candidate_name = next((r.get("value", "").strip() for r in group if r.get("field", "").strip() == "Candidate"), "")
            matches = by_name.get(candidate_name, [])
            if not has_candidate_key and candidate_name:
                if len(matches) == 1:
                    insert_after_candidate = False
                    for row in group:
                        new_changes.append(row)
                        if row.get("field", "").strip() == "Candidate" and not insert_after_candidate:
                            insert_after_candidate = True
                            new_changes.append(
                                {
                                    "change_id": cid,
                                    "table": "evidence",
                                    "key": "",
                                    "action": first.get("action", "").strip(),
                                    "reasoning": first.get("reasoning", "").strip(),
                                    "field": "Candidate_Key",
                                    "value": matches[0]["Candidate_Key"],
                                    "D": first.get("D", "").strip(),
                                    "Reasoning D": first.get("Reasoning D", "").strip(),
                                    "I": first.get("I", "").strip(),
                                    "Reasoning I": first.get("Reasoning I", "").strip(),
                                }
                            )
                    continue
                elif len(matches) > 1:
                    ambiguous_evidence_adds.append(f"change_id {cid}: {candidate_name} -> {[candidate_key(m) for m in matches]}")
            for row in group:
                new_changes.append(row)
            continue

        for row in group:
            new_changes.append(row)

    candidate_fieldnames = list(candidates[0].keys()) if candidates else ["Candidate", "State", "Office", "Party", "Status", "Active", "Verdict", "Preference", "Candidate_Key"]
    if "Candidate_Key" not in candidate_fieldnames:
        candidate_fieldnames.insert(1, "Candidate_Key")
    for row in candidates:
        for field in candidate_fieldnames:
            row.setdefault(field, "")
    for row in evidence:
        for field in evidence_fieldnames:
            row.setdefault(field, "")
    for row in new_changes:
        for field in CHANGE_FIELDNAMES:
            row.setdefault(field, "")

    write_csv(CANDIDATES_CSV, candidates, candidate_fieldnames)
    write_csv(EVIDENCE_CSV, evidence, evidence_fieldnames)
    write_csv(CHANGES_CSV, new_changes, CHANGE_FIELDNAMES)

    duplicate_names = {name: rows for name, rows in by_name.items() if len(rows) > 1}
    report_lines = [
        "Candidate key migration report",
        "",
        "Duplicate candidate names:",
    ]
    if duplicate_names:
        for name, rows in sorted(duplicate_names.items()):
            report_lines.append(f"- {name}: {', '.join(candidate_key(r) for r in rows)}")
    else:
        report_lines.append("- none")
    report_lines.extend(["", "Ambiguous existing evidence rows without Candidate_Key:"])
    report_lines.extend([f"- {line}" for line in unresolved_evidence] or ["- none"])
    report_lines.extend(["", "Ambiguous candidate-table change keys left unchanged:"])
    report_lines.extend([f"- {line}" for line in ambiguous_candidate_changes] or ["- none"])
    report_lines.extend(["", "Ambiguous evidence-add change groups left without Candidate_Key row:"])
    report_lines.extend([f"- {line}" for line in ambiguous_evidence_adds] or ["- none"])
    REPORT_PATH.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(f"Updated {CANDIDATES_CSV}")
    print(f"Updated {EVIDENCE_CSV}")
    print(f"Updated {CHANGES_CSV}")
    print(f"Wrote report to {REPORT_PATH}")


if __name__ == "__main__":
    main()
