from __future__ import annotations

import csv
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
WIKIPEDIA_TITLES_CSV = ROOT / "data" / "wikipedia_titles.csv"

EVIDENCE_FIELDS = ["Evidence_ID", "Candidate", "Source_Description", "URL"]
TITLE_FIELDS = ["URL", "Display_Title"]


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = parsed.path or ""
    query = parsed.query or ""
    if query:
        return f"{scheme}://{netloc}{path}?{query}"
    return f"{scheme}://{netloc}{path}"


def load_mapping() -> dict[str, str]:
    with WIKIPEDIA_TITLES_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != TITLE_FIELDS:
            raise SystemExit(f"Unexpected mapping columns: {reader.fieldnames!r}")
        mapping: dict[str, str] = {}
        for row in reader:
            url = normalize_url(row.get("URL", ""))
            title = row.get("Display_Title", "").strip()
            if url and title:
                mapping[url] = title
    return mapping


def load_evidence() -> list[dict[str, str]]:
    with EVIDENCE_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EVIDENCE_FIELDS:
            raise SystemExit(f"Unexpected evidence columns: {reader.fieldnames!r}")
        return [dict(row) for row in reader]


def write_evidence(rows: list[dict[str, str]]) -> None:
    with EVIDENCE_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EVIDENCE_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    mapping = load_mapping()
    rows = load_evidence()
    updated = 0
    matched = 0
    for row in rows:
        url = normalize_url(row.get("URL", ""))
        new_title = mapping.get(url)
        if not new_title:
            continue
        matched += 1
        if row.get("Source_Description", "") != new_title:
            row["Source_Description"] = new_title
            updated += 1
    write_evidence(rows)
    print(f"Matched {matched} evidence rows from {len(mapping)} Wikipedia URL mappings.")
    print(f"Updated {updated} Source_Description values in {EVIDENCE_CSV}")


if __name__ == "__main__":
    main()
