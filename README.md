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

Workflow artifacts and derived reports:
- `reports/`

Proposed change queue:
- `changes.csv`

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
- `__pycache__/`
- `*.pyc`
- `Old/`

## Verdicts

Every candidate gets exactly one verdict. Full definitions with examples live in
`data/verdicts.csv`. Sort order within each race:

`nice -> nuanced -> no_record -> naughty`

When uncertain about a verdict, enter a preliminary verdict in the CSV, then
stop and explain the uncertainty before moving on to the next race.

## Data Model

`data/candidates.csv`
- One row per candidate.
- Columns: `Candidate, State, Office, Party, Status, Active, Verdict`
- `Candidate` must exactly match `data/evidence.csv`.
- `State` uses full state names.
- `Office` is `Governor` or `Senate`.

`data/evidence.csv`
- One row per source.
- Columns: `Evidence_ID, Candidate, Source_Description, URL`
- Multiple rows per candidate are expected.
- Every row must include a direct URL.
- `Evidence_ID` is the stable primary key for review and change tracking.

`data/races.csv`
- One row per race.
- Columns: `Priority, State, Office, Rating, Claude, Codex, Gemini`
- Ordered by priority. Research top to bottom.
- `Claude`, `Codex`, and `Gemini` track review passes by reviewer.

## Change Queue

Do not let assistants write directly into the canonical CSVs during research
passes. Proposed edits go into `changes.csv` first and are reviewed before
application.

`changes.csv`
- Columns:
- `change_id`
- `table`
- `key`
  - `action`
  - `reasoning`
- `field`
  - `value`
  - `D`
  - `Reasoning D`
  - `I`
  - `Reasoning I`
- `change_id` is the stable logical change identifier. Use simple integers.
  Multiple rows may share the same `change_id` when one proposed edit touches
  multiple fields.
- `D` and `I` are reviewer decision columns. Blank means pending. Non-blank values are:
  - `approved`
  - `denied`
  - `applied`
  - `conflict`
- `Reasoning D` and `Reasoning I` are optional reviewer notes

Queue semantics:
- `table` is one of `candidates`, `evidence`, `races`
- `key` identifies the target row
  - `candidates`: candidate name
  - `evidence`: `Evidence_ID` for existing rows; for `add`, key may be blank
  - `races`: `State|Office`
- `action` is one of:
  - `mod`
  - `add`
  - `del`
  - `check`
- `reasoning` is the human-readable explanation for the proposal
- `field` and `value` are a single atomic change row

Examples:

Candidate verdict change:

```csv
1,candidates,Katie Porter,mod,New evidence supports stronger verdict,Verdict,nice,,,,
```

Evidence add (same `change_id` across multiple rows):

```csv
2,evidence,,add,New direct campaign source found,Candidate,Katie Porter,,,,
2,evidence,,add,New direct campaign source found,Source_Description,Meaningful AI regulation statement,,,,
2,evidence,,add,New direct campaign source found,URL,https://example.com,,,,
```

Race review increment:

```csv
3,races,California|Governor,check,Completed Codex review pass,Codex,+1,,,,
```

Review workflow:
1. Assistants research a race and append proposed changes to `changes.csv`.
2. Review changes in the local UI and mark reviewer decisions in `D` and `I`.
3. A later apply step merges changes into the canonical CSVs once both reviewers have `approved` them.

Review UI:

```bash
python3 scripts/review_changes_ui.py
```

Hot-reload watcher:

```bash
python3 scripts/watch_review_changes_ui.py
```

Then open:

```text
http://127.0.0.1:8767
```

Apply approved changes:

```bash
python3 scripts/apply_changes.py
```

The review UI includes tabs for:
- `Run Research`
- `Review Changes`
- `Inspect Data`
- `Inspect Logs`

The Inspect Logs tab reads structured candidate-level summaries generated from
raw per-candidate logs stored under `.claude/race_runner_logs`.

Sequential Race Runner:

```bash
python3 scripts/race_runner.py --race "Arizona|Governor"
```

This runs one candidate at a time for the selected race, appends any proposed
edits to `changes.csv`, and then queues a single `races` check to add `+1` to
the selected provider column after the whole race finishes successfully.
Supported providers are `claude`, `codex`, and `gemini`. Use `--max-races 0`
to keep moving through races in priority order, or `--dry-run` to preview
selection without calling a provider.

`data/verdicts.csv`
- Reference definitions for verdict values.

## Research Workflow

1. Find the next race in `data/races.csv` with the fewest reviewer passes.
2. Identify the current declared candidates for that race.
3. Research each candidate's public AI-related record.
4. Append proposed edits to `changes.csv`, not directly to the canonical CSVs.
5. Review the queued edits and mark reviewer decisions in `D` and `I`.
6. Apply changes that both reviewers approved to the canonical CSVs.
7. Run `python3 build.py`.
8. Stop and check in about any uncertain verdicts before starting the next race.

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

Watch mode:

```bash
python3 scripts/watch_build.py
```

This watches:
- `template.html`
- `build.py`
- `data/*.csv`

and reruns `build.py` after each save.

Watch mode with browser auto-reload:

```bash
python3 scripts/watch_build.py --serve --host 127.0.0.1 --port 5503
```

This serves:
- `http://127.0.0.1:5503/guide.html`

and reloads the browser only after a build completes successfully.

Python dependencies:

```bash
pip3 install pandas openpyxl beautifulsoup4
```

## Notes For Assistants

This repository is intended to be maintainable by both humans and coding
assistants. Before editing data, read this file and follow the CSV-first,
change-queue workflow above.
