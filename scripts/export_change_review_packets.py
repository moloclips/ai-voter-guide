from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
CHANGES_CSV = ROOT / "changes.csv"
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
DEFAULT_OUTPUT_DIR = ROOT / ".claude" / "review_packets"
CHANGE_FIELDNAMES = ["change_id", "table", "key", "action", "reasoning", "field", "value", "D", "Reasoning D", "I", "Reasoning I"]


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


def ensure_changes_csv() -> None:
    if CHANGES_CSV.exists():
        return
    with CHANGES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CHANGE_FIELDNAMES)
        writer.writeheader()


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_changes() -> list[dict[str, str]]:
    ensure_changes_csv()
    rows = load_csv(CHANGES_CSV)
    out = []
    for row in rows:
        normalized = {field: row.get(field, "").strip() for field in CHANGE_FIELDNAMES}
        out.append(normalized)
    return out


def parse_race_key(key: str) -> tuple[str, str]:
    state, sep, office = key.partition("|")
    if not sep:
        return "", ""
    return state.strip(), office.strip()


def build_review_groups(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    candidate_rows = load_csv(CANDIDATES_CSV)
    evidence_rows = load_csv(EVIDENCE_CSV)
    counts: dict[str, int] = {}
    for row in candidate_rows:
        name = row.get("Candidate", "").strip()
        counts[name] = counts.get(name, 0) + 1
    candidate_lookup = {
        candidate_key(row): {
            "state": row.get("State", "").strip(),
            "office": row.get("Office", "").strip(),
            "candidate": row.get("Candidate", "").strip(),
            "row": dict(row),
        }
        for row in candidate_rows
    }
    unique_name_lookup = {
        row.get("Candidate", "").strip(): candidate_lookup[candidate_key(row)]
        for row in candidate_rows
        if counts.get(row.get("Candidate", "").strip(), 0) == 1
    }
    evidence_lookup = {
        row.get("Evidence_ID", "").strip(): {
            "candidate": row.get("Candidate", "").strip(),
            "candidate_key": row.get("Candidate_Key", "").strip(),
            "row": dict(row),
        }
        for row in evidence_rows
    }

    grouped: dict[str, list[dict[str, str]]] = {}
    order: list[str] = []
    for row in rows:
        change_id = row.get("change_id", "").strip()
        if change_id not in grouped:
            order.append(change_id)
            grouped[change_id] = []
        grouped[change_id].append(row)

    out: list[dict[str, object]] = []
    for change_id in order:
        group = grouped[change_id]
        first = group[0]
        table = first.get("table", "").strip()
        if table not in {"candidates", "evidence"}:
            continue
        key = first.get("key", "").strip()
        state = ""
        office = ""
        candidate = ""
        current_row: dict[str, str] = {}

        if table == "candidates":
            candidate_meta = candidate_lookup.get(key, {})
            candidate = str(candidate_meta.get("candidate", "")).strip() or key
            state = str(candidate_meta.get("state", "")).strip()
            office = str(candidate_meta.get("office", "")).strip()
            current_row = dict(candidate_meta.get("row", {}))
        elif table == "evidence":
            if key and key in evidence_lookup:
                evidence_meta = evidence_lookup[key]
                candidate = str(evidence_meta.get("candidate", "")).strip()
                candidate_meta = candidate_lookup.get(str(evidence_meta.get("candidate_key", "")).strip(), {})
                if not candidate_meta:
                    candidate_meta = unique_name_lookup.get(candidate, {})
                current_row = dict(evidence_meta.get("row", {}))
            else:
                candidate = next(
                    (
                        row.get("value", "").strip()
                        for row in group
                        if row.get("field", "").strip() == "Candidate" and row.get("value", "").strip()
                    ),
                    "",
                )
                candidate_key_value = next(
                    (
                        row.get("value", "").strip()
                        for row in group
                        if row.get("field", "").strip() == "Candidate_Key" and row.get("value", "").strip()
                    ),
                    "",
                )
                candidate_meta = candidate_lookup.get(candidate_key_value, {})
                if not candidate_meta:
                    candidate_meta = unique_name_lookup.get(candidate, {})
                if not candidate:
                    candidate = str(candidate_meta.get("candidate", "")).strip()
            state = str(candidate_meta.get("state", "")).strip()
            office = str(candidate_meta.get("office", "")).strip()

        if not state:
            state = "(unknown state)"
        if not office:
            office = "(unknown race)"
        if not candidate:
            candidate = "(no candidate)"

        out.append(
            {
                "change_id": change_id,
                "table": table,
                "key": key,
                "D": first.get("D", "").strip(),
                "Reasoning D": first.get("Reasoning D", "").strip(),
                "I": first.get("I", "").strip(),
                "Reasoning I": first.get("Reasoning I", "").strip(),
                "reasoning": first.get("reasoning", "").strip(),
                "state": state,
                "race": office,
                "candidate": candidate,
                "current_row": current_row,
                "rows": group,
            }
        )
    return out


def build_hierarchy(groups: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    out: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for group in groups:
        key = (str(group["state"]), str(group["race"]), str(group["candidate"]))
        out.setdefault(key, []).append(group)
    return out


def proposed_row(group: dict[str, Any]) -> dict[str, str]:
    table = str(group["table"])
    action = str(group["rows"][0]["action"])
    current = dict(group.get("current_row") or {})
    row = dict(current)
    if action == "add":
        row = {}
    for item in group["rows"]:
        field = str(item.get("field", "")).strip()
        if not field:
            continue
        row[field] = str(item.get("value", ""))
    if table == "evidence" and action == "add":
        row.setdefault("Evidence_ID", "")
    return row


def format_row(row: dict[str, str]) -> str:
    if not row:
        return "  (none)"
    lines = []
    for key, value in row.items():
        lines.append(f"  {key}: {value}")
    return "\n".join(lines)


def format_diff(group: dict[str, Any]) -> str:
    current = dict(group.get("current_row") or {})
    updates = {
        str(item.get("field", "")).strip(): str(item.get("value", ""))
        for item in group["rows"]
        if str(item.get("field", "")).strip()
    }
    if not updates:
        return "  (none)"
    lines = []
    for field, new_value in updates.items():
        old_value = current.get(field, "")
        lines.append(f"  {field}:")
        lines.append(f"    old: {old_value}")
        lines.append(f"    new: {new_value}")
    return "\n".join(lines)


def render_group(group: dict[str, Any]) -> str:
    first = group["rows"][0]
    action = str(first["action"])
    table = str(group["table"])
    key = str(group["key"])
    parts = [
        f"Change ID: {group['change_id']}",
        f"D: {group['D'] or 'pending'}",
        f"Reasoning D: {group['Reasoning D']}",
        f"I: {group['I'] or 'pending'}",
        f"Reasoning I: {group['Reasoning I']}",
        f"Table: {table}",
        f"Action: {action}",
        f"Key: {key or '(blank)'}",
        f"Reasoning: {group['reasoning']}",
    ]
    if action == "add":
        parts.append("Proposed new row:")
        parts.append(format_row(proposed_row(group)))
    elif action == "del":
        parts.append("Current row that would be deleted:")
        parts.append(format_row(dict(group.get("current_row") or {})))
    elif action == "mod":
        parts.append("Current row:")
        parts.append(format_row(dict(group.get("current_row") or {})))
        parts.append("Proposed field changes:")
        parts.append(format_diff(group))
        parts.append("Resulting row if approved:")
        parts.append(format_row(proposed_row(group)))
    return "\n".join(parts)


def render_candidate_packet(state: str, race: str, candidate: str, groups: list[dict[str, Any]]) -> str:
    header = [
        "You are reviewing proposed changes for the AI Voter Guide.",
        "Do not do any extra web searching.",
        "Judge only on the basis of the information presented here.",
        "Question: Do you approve or disapprove of these proposed changes given the final goal of displaying this to single-issue AI voters?",
        "",
        f"State: {state}",
        f"Race: {race}",
        f"Candidate: {candidate}",
        "",
        "Review each change group below.",
    ]
    body = []
    for idx, group in enumerate(groups, start=1):
        body.append("")
        body.append(f"=== Change Group {idx} ===")
        body.append(render_group(group))
    footer = [
        "",
        "Respond with concise review judgments for each change group and an overall recommendation for this candidate.",
    ]
    return "\n".join(header + body + footer) + "\n"


def safe_slug(text: str) -> str:
    return (
        text.replace("/", "_")
        .replace("\\", "_")
        .replace(" ", "_")
        .replace(":", "_")
        .replace("|", "_")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Export candidate-scoped LLM review packets from changes.csv.")
    parser.add_argument("--all", action="store_true", help="Include all statuses instead of only pending rows")
    parser.add_argument("--candidate", default="", help="Only export a specific candidate")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory to write packet text files into")
    parser.add_argument("--single-file", default="", help="Write all exported candidate packets into one text file")
    args = parser.parse_args()

    rows = load_changes()
    if not args.all:
        rows = [row for row in rows if not row["D"] or not row["I"]]
    groups = build_review_groups(rows)
    hierarchy = build_hierarchy(groups)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    combined_packets: list[str] = []
    for (state, race, candidate), candidate_groups in sorted(hierarchy.items()):
        if args.candidate and candidate != args.candidate:
            continue
        packet = render_candidate_packet(state, race, candidate, candidate_groups)
        filename = f"{safe_slug(state)}__{safe_slug(race)}__{safe_slug(candidate)}.txt"
        (output_dir / filename).write_text(packet, encoding="utf-8")
        combined_packets.append(packet.rstrip())
        written += 1

    if args.single_file:
        combined_path = Path(args.single_file)
        if not combined_path.is_absolute():
            combined_path = ROOT / combined_path
        combined_path.parent.mkdir(parents=True, exist_ok=True)
        intro = [
            "You are reviewing proposed changes for the AI Voter Guide.",
            "Do not do any extra web searching.",
            "Judge only on the basis of the information presented here.",
            "For each candidate section, say whether you approve or disapprove each change group and give an overall recommendation for that candidate.",
            "",
        ]
        combined_text = "\n".join(intro)
        if combined_packets:
            combined_text += "\n\n".join(
                f"##### Candidate Packet {idx} #####\n\n{packet}"
                for idx, packet in enumerate(combined_packets, start=1)
            )
            combined_text += "\n"
        combined_path.write_text(combined_text, encoding="utf-8")

    print(f"Wrote {written} review packet(s) to {output_dir}")
    if args.single_file:
        print(f"Wrote combined review file to {combined_path}")


if __name__ == "__main__":
    main()
