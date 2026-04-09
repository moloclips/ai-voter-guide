from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERDICT_LEGEND_PATH = ROOT / "scripts" / "verdict_legend.txt"
VERDICT_ORDER = ("nice", "nuanced", "no_record", "naughty")


def load_verdict_labels() -> dict[str, str]:
    labels: dict[str, str] = {}
    for raw_line in VERDICT_LEGEND_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"Invalid verdict legend line: {raw_line}")
        labels[key.strip()] = value.strip()
    missing = [key for key in VERDICT_ORDER if key not in labels]
    if missing:
        raise ValueError(f"Missing verdict legend entries: {', '.join(missing)}")
    return labels


def render_verdict_legend(prefix: str = "- ", align: bool = False) -> str:
    labels = load_verdict_labels()
    if align:
        width = max(len(key) for key in VERDICT_ORDER)
        return "\n".join(f"{prefix}{key.ljust(width)} = {labels[key]}" for key in VERDICT_ORDER)
    return "\n".join(f"{prefix}`{key}` = {labels[key]}" for key in VERDICT_ORDER)


def inject_verdict_legend(template: str) -> str:
    return template.replace("%VERDICT_LEGEND%", render_verdict_legend(prefix="\t- "))
