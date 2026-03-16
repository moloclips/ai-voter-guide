# AI Voter Guide

Single-issue voter guide for the 2026 US midterms. Every major candidate for
Governor and Senate is rated on one dimension: whether they support
responsible AI regulation. The guide is a static HTML file.

Edit `template.html` when changing layout or styling.
Open `guide.html` in a browser to view the generated guide.
Append `?all` to the URL to see all states at once for review.

## Files

Canonical source of truth:
- `data/candidates.csv`
- `data/evidence.csv`
- `data/races.csv`
- `data/verdicts.csv`

Generated outputs:
- `data.xlsx`
- `guide.html`

Build script:
- `build.py`

Editable HTML template:
- `template.html`

Archived or unused material:
- `Old/`

Ignored local-only material:
- `.claude/`

## Verdicts

Every candidate gets exactly one verdict. Full definitions with examples live in
`data/verdicts.csv`. Sort order within each race:

`nice -> nuanced -> no_record -> naughty`

When uncertain about a verdict, enter a preliminary verdict in the CSV, then
stop and explain the uncertainty before moving on to the next race.

## Data Model

`data/candidates.csv`
- One row per candidate.
- Columns: `Candidate, State, Office, Party, Status, Verdict`
- `Candidate` must exactly match `data/evidence.csv`.
- `State` uses full state names.
- `Office` is `Governor` or `Senate`.

`data/evidence.csv`
- One row per source.
- Columns: `Candidate, Source_Description, URL`
- Multiple rows per candidate are expected.
- Every row must include a direct URL.

`data/races.csv`
- One row per race.
- Columns: `State, Office, Completed`
- Ordered by priority. Research top to bottom.
- Mark `Completed=True` once a race is fully researched and entered.

`data/verdicts.csv`
- Reference definitions for verdict values.

## Research Workflow

1. Find the next incomplete race in `data/races.csv`.
2. Identify the current declared candidates for that race.
3. Research each candidate's public AI-related record.
4. Update `data/candidates.csv` and `data/evidence.csv`.
5. Mark the race complete in `data/races.csv`.
6. Run `python3 build.py`.
7. Stop and check in about any uncertain verdicts before starting the next race.

Do not work multiple races in parallel. Finish one race, rebuild, and then move
to the next. That keeps the guide and the data files synchronized.

## Research Standards

Search examples:
- `"[Candidate Name] AI artificial intelligence policy 2025 2026"`
- `"[Candidate Name] AI regulation bills statements"`
- `"[State] [Governor/Senate] 2026 candidates"`

Preferred sources:
- official campaign pages
- official `.gov` pages
- direct press releases
- direct interviews or candidate statements

Use third-party coverage only when it adds necessary context or when no direct
statement exists.

Look for:
- AI safety bills
- deepfake legislation
- algorithmic accountability
- AI in elections
- autonomous weapons
- state AI regulation
- executive orders
- deregulation or anti-regulation positions

If nothing material is found after a thorough search, use `no_record`.

## Build

Run from the project directory:

```bash
python3 build.py
```

`build.py`:
- reads from `data/*.csv`
- regenerates `data.xlsx`
- reads `template.html`
- writes the generated output to `guide.html`

Python dependencies:

```bash
pip3 install pandas openpyxl beautifulsoup4
```

## Notes For Assistants

This repository is intended to be maintainable by both humans and coding
assistants. Before editing data, read this file and follow the CSV-first
workflow above.
