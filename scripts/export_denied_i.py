from __future__ import annotations

import argparse
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CHANGES_CSV = ROOT / "changes.csv"
DEFAULT_OUTPUT_PATH = ROOT / "reviewed_change_ids.txt"
REVIEWERS = ("D", "I")
STATUSES = ("approved", "denied")


def parse_bool_flag(value: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected true/false value, got: {value}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export change_ids matching any enabled reviewer/status cell across the D/I x approved/denied matrix."
    )
    parser.add_argument("--changes-file", default=str(CHANGES_CSV), help="Path to changes CSV")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Output text file path")
    parser.add_argument("--d-approved", type=parse_bool_flag, default=True, help="Include change_ids with D=approved")
    parser.add_argument("--d-denied", type=parse_bool_flag, default=False, help="Include change_ids with D=denied")
    parser.add_argument("--i-approved", type=parse_bool_flag, default=False, help="Include change_ids with I=approved")
    parser.add_argument("--i-denied", type=parse_bool_flag, default=False, help="Include change_ids with I=denied")
    args = parser.parse_args()

    enabled_pairs = {
        ("D", "approved"): args.d_approved,
        ("D", "denied"): args.d_denied,
        ("I", "approved"): args.i_approved,
        ("I", "denied"): args.i_denied,
    }

    selected_ids: set[str] = set()
    changes_path = Path(args.changes_file)
    output_path = Path(args.output)

    if not any(enabled_pairs.values()):
        output_path.write_text("", encoding="utf-8")
        print(f"No reviewer/status cells enabled; wrote empty file to {output_path}")
        return

    with changes_path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            change_id = row.get("change_id", "").strip()
            if not change_id:
                continue
            for reviewer in REVIEWERS:
                status = row.get(reviewer, "").strip().lower()
                if status not in STATUSES:
                    continue
                if enabled_pairs.get((reviewer, status), False):
                    selected_ids.add(change_id)
                    break

    ordered = sorted(selected_ids, key=lambda value: (not value.isdigit(), int(value) if value.isdigit() else value))
    output_path.write_text("\n".join(ordered) + ("\n" if ordered else ""), encoding="utf-8")

    enabled_labels = [
        f"{reviewer}={status}"
        for (reviewer, status), enabled in enabled_pairs.items()
        if enabled
    ]
    print(f"Wrote {len(ordered)} change ids to {output_path}")
    print(f"Included cells: {', '.join(enabled_labels)}")


if __name__ == "__main__":
    main()
