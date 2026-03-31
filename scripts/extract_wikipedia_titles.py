from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, unquote, urlparse


ROOT = Path(__file__).resolve().parent.parent
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
OUTPUT_CSV = ROOT / "data" / "wikipedia_titles.csv"

INPUT_FIELDS = ["Evidence_ID", "Candidate", "Source_Description", "URL"]
OUTPUT_FIELDS = ["URL", "Display_Title"]


def read_evidence_urls() -> list[str]:
    with EVIDENCE_CSV.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [str(row.get("URL", "")).strip() for row in reader if str(row.get("URL", "")).strip()]


def normalize_wikipedia_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = parsed.path or ""
    query = parsed.query or ""
    fragment = ""
    if query:
        return f"{scheme}://{netloc}{path}?{query}"
    return f"{scheme}://{netloc}{path}"


def infer_title_from_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("missing scheme or host")
    host = parsed.netloc.lower()
    if "wikipedia.org" not in host:
        raise ValueError("not a wikipedia.org URL")

    title_raw = ""
    if parsed.path.startswith("/wiki/"):
        title_raw = parsed.path[len("/wiki/") :]
    elif parsed.path == "/w/index.php":
        title_raw = parse_qs(parsed.query).get("title", [""])[0]
    else:
        raise ValueError("unsupported wikipedia URL shape")

    title_raw = title_raw.strip().strip("/")
    if not title_raw:
        raise ValueError("missing wiki title path")

    title = unquote(title_raw).replace("_", " ").strip()
    if not title:
        raise ValueError("empty decoded title")
    return title, f"{title} (Wikipedia)"


def make_display_title(title: str) -> str:
    return (
        title
        .replace("United States House of Representatives", "U.S. House")
        .replace("United States Senate", "U.S. Senate")
    )


def build_rows(urls: Iterable[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_url in urls:
        normalized_url = normalize_wikipedia_url(raw_url)
        if "wikipedia.org" not in normalized_url.lower():
            continue
        if normalized_url in seen:
            continue
        seen.add(normalized_url)
        try:
            title, _ = infer_title_from_url(raw_url)
        except ValueError as exc:
            raise SystemExit(f"Failed to parse Wikipedia URL {raw_url!r}: {exc}") from exc
        rows.append(
            {
                "URL": normalized_url,
                "Display_Title": f"{make_display_title(title)} (Wikipedia)",
            }
        )
    rows.sort(key=lambda row: row["URL"])
    return rows


def write_rows(rows: list[dict[str, str]]) -> None:
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = build_rows(read_evidence_urls())
    write_rows(rows)
    print(f"Wrote {len(rows)} Wikipedia URL mappings to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
