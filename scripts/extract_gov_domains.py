from __future__ import annotations

import csv
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
OUTPUT_CSV = ROOT / "reports" / "gov_domains.csv"

OUTPUT_FIELDS = ["Domain", "Candidate", "Current_Descriptions", "Suggested_Description"]


def normalize_domain(url: str) -> str:
    parsed = urlparse(url.strip())
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def suggested_description(domain: str, candidate: str) -> str:
    if domain.endswith(".house.gov"):
        return f"{candidate} (U.S. House Website)"
    if domain.endswith(".senate.gov"):
        return f"{candidate} (U.S. Senate Website)"
    return ""


def read_domains() -> list[dict[str, str]]:
    pairs: dict[tuple[str, str], set[str]] = {}
    with EVIDENCE_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            domain = normalize_domain(str(row.get("URL", "")).strip())
            candidate = str(row.get("Candidate", "")).strip()
            description = str(row.get("Source_Description", "")).strip()
            if domain.endswith(".gov") or domain == "gov":
                key = (domain, candidate)
                if key not in pairs:
                    pairs[key] = set()
                if description:
                    pairs[key].add(description)
    return [
        {
            "Domain": domain,
            "Candidate": candidate,
            "Current_Descriptions": " | ".join(sorted(descriptions)),
            "Suggested_Description": suggested_description(domain, candidate),
        }
        for (domain, candidate), descriptions in sorted(pairs.items())
    ]


def write_domains(rows: list[dict[str, str]]) -> None:
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = read_domains()
    write_domains(rows)
    print(f"Wrote {len(rows)} unique .gov domain/candidate pairs to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
