from __future__ import annotations

import os
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CHANGES_FILENAME = "changes.csv"
SECONDARY_CHANGES_FILENAME = "changes.v2.csv"
ENV_VAR = "AI_VOTER_GUIDE_CHANGES_FILE"


def available_changes_filenames() -> list[str]:
    names = sorted(
        path.name
        for path in ROOT.glob("changes*.csv")
        if path.is_file()
    )
    for required in (DEFAULT_CHANGES_FILENAME, SECONDARY_CHANGES_FILENAME):
        if required not in names:
            names.append(required)
    names = sorted(set(names), key=lambda name: (name != DEFAULT_CHANGES_FILENAME, name != SECONDARY_CHANGES_FILENAME, name))
    return names


def resolve_changes_csv(filename: str | None = None) -> Path:
    chosen = (filename or os.environ.get(ENV_VAR) or DEFAULT_CHANGES_FILENAME).strip()
    if not chosen:
        chosen = DEFAULT_CHANGES_FILENAME
    path = ROOT / Path(chosen).name
    return path
