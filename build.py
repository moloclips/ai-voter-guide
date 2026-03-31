from pathlib import Path
from datetime import datetime
import json
from urllib.parse import quote

import pandas as pd
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
TEMPLATE_HTML = ROOT / "template.html"
GUIDE_HTML = ROOT / "guide.html"
XLSX_PATH = ROOT / "data.xlsx"
FAVICON_SVG = ROOT / "brand" / "plug-logo.svg"
RACE_PARTIAL_HTML = ROOT / "race.html"

CSV_SPECS = [
    ("Candidates", DATA_DIR / "candidates.csv"),
    ("Evidence", DATA_DIR / "evidence.csv"),
    ("Races", DATA_DIR / "races.csv"),
    ("Verdicts", DATA_DIR / "verdicts.csv"),
]

VERDICT_ORDER = ["nice", "nuanced", "no_record", "naughty"]
EXCLUDED_STATUS_TERMS = (
    "removed from ballot",
    "failed to qualify",
    "exploratory",
    "potential candidate",
)


def load_tables():
    tables = {}
    for sheet_name, csv_path in CSV_SPECS:
        tables[sheet_name] = pd.read_csv(csv_path, keep_default_na=False)
    return tables


def present(*vals):
    return all(str(v).strip() and str(v).strip().lower() != "nan" for v in vals)


def build_voter_data(candidates, evidence):
    evidence_map = {}
    for _, row in evidence.iterrows():
        name = str(row.get("Candidate", "")).strip()
        desc = str(row.get("Source_Description", "")).strip()
        url = str(row.get("URL", "")).strip()
        if not present(name, desc, url):
            continue
        evidence_map.setdefault(name, []).append({
            "description": desc,
            "url": url,
        })

    races = {}
    for _, row in candidates.iterrows():
        candidate = str(row.get("Candidate", "")).strip()
        state = str(row.get("State", "")).strip()
        office = str(row.get("Office", "")).strip()
        party = str(row.get("Party", "")).strip()
        status = str(row.get("Status", "")).strip()
        verdict = str(row.get("Verdict", "")).strip()
        preference = str(row.get("Preference", "")).strip()

        if not present(candidate, state, office, party, status):
            continue
        status_lower = status.lower()
        if any(term in status_lower for term in EXCLUDED_STATUS_TERMS):
            continue
        if verdict not in VERDICT_ORDER:
            verdict = "no_record"

        races.setdefault((state, office), {})[candidate] = {
            "candidate": candidate,
            "party": party,
            "status": status,
            "verdict": verdict,
            "preference": int(preference) if preference.isdigit() else None,
            "sources": evidence_map.get(candidate, []),
        }

    voter_data = []
    for (state, office), cands in sorted(races.items()):
        sorted_candidates = sorted(
            cands.values(),
            key=lambda c: (
                VERDICT_ORDER.index(c["verdict"]),
                c["preference"] if c["preference"] is not None else 999,
                c["candidate"],
            ),
        )
        voter_data.append({
            "state": state,
            "office": office,
            "candidates": sorted_candidates,
        })
    return voter_data


def write_xlsx(tables):
    with pd.ExcelWriter(XLSX_PATH, engine="openpyxl") as writer:
        for sheet_name, _ in CSV_SPECS:
            tables[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)


def write_guide_html(voter_data):
    with TEMPLATE_HTML.open("r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    script = soup.find("script", id="voter-data")
    if script is None:
        raise RuntimeError("Missing <script id='voter-data'> in template.html")

    script.string = json.dumps(voter_data, ensure_ascii=False, indent=2)

    last_updated = soup.find(id="lastUpdated")
    if last_updated is not None:
        now = datetime.now()
        last_updated["datetime"] = now.strftime("%Y-%m-%d")
        last_updated.string = now.strftime("%B %-d, %Y")

    build_stamp = soup.find("meta", attrs={"name": "build-stamp"})
    if build_stamp is not None:
        build_stamp["content"] = datetime.now().isoformat(timespec="seconds")

    favicon = soup.find("link", rel="icon")
    if favicon is not None:
        favicon_svg = FAVICON_SVG.read_text(encoding="utf-8").strip()
        favicon["href"] = f"data:image/svg+xml;charset=utf-8,{quote(favicon_svg)}"

    race_partial = soup.find("template", id="race-partial")
    if race_partial is None:
        race_partial = soup.new_tag("template", id="race-partial")
        if soup.body is None:
            raise RuntimeError("Missing <body> in template.html")
        soup.body.append(race_partial)
    race_partial.clear()
    partial_soup = BeautifulSoup(RACE_PARTIAL_HTML.read_text(encoding="utf-8"), "html.parser")
    for node in list(partial_soup.contents):
        race_partial.append(node)

    with GUIDE_HTML.open("w", encoding="utf-8") as f:
        f.write(str(soup))


def main():
    tables = load_tables()
    voter_data = build_voter_data(tables["Candidates"], tables["Evidence"])
    write_xlsx(tables)
    write_guide_html(voter_data)

    total_candidates = sum(len(r["candidates"]) for r in voter_data)
    total_evidence = sum(len(c["sources"]) for r in voter_data for c in r["candidates"])
    total_states = len({r["state"] for r in voter_data})
    total_races = len(voter_data)

    print(
        f"Done: {total_candidates} candidates | "
        f"{total_evidence} sources | {total_states} states | {total_races} races"
    )
    print("Source of truth: data/*.csv")
    print(
        f"Used {TEMPLATE_HTML.name}, wrote {XLSX_PATH.name}, "
        f"and updated {GUIDE_HTML.name}"
    )


if __name__ == "__main__":
    main()
