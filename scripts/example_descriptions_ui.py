from __future__ import annotations

import csv
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from html import unescape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
EVIDENCE_CSV = DATA_DIR / "evidence.csv"
OUTPUT_CSV = DATA_DIR / "exampleDescriptions.csv"
RACE_PARTIAL_HTML = ROOT / "race.html"
HOST = "127.0.0.1"
PORT = 8766
SUGGESTION_TIMEOUT_SECONDS = 90
USER_BIN_DIR = Path.home() / ".npm-global" / "bin"
NODE20_BIN = Path("/opt/homebrew/opt/node@20/bin/node")
HOME_GEMINI_DIR = Path.home() / ".gemini"
AUTH_ENV_VARS = (
    "GEMINI_API_KEY",
    "GOOGLE_GENAI_USE_VERTEXAI",
    "GOOGLE_GENAI_USE_GCA",
)


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Example Descriptions</title>
  <style>
    :root {
      --bg: #f5f1e8;
      --panel: #fffaf0;
      --text: #1f1a17;
      --muted: #6b6158;
      --border: #d9cdbd;
      --accent: #8d4f2d;
      --accent-2: #5f7c62;
      --line: #d9cdbd;
      --heading: #2a221c;
      --card: #fffaf0;
      --accent-faint: #f3ebe1;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: linear-gradient(180deg, #efe6d5 0%, var(--bg) 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 900px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    h1 {
      margin: 0 0 6px;
      font-size: 2rem;
      line-height: 1.1;
    }
    .sub {
      margin: 0 0 24px;
      color: var(--muted);
      font-size: 1rem;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 20px;
      box-shadow: 0 8px 30px rgba(40, 28, 17, 0.08);
    }
    .label {
      display: block;
      font-size: 0.78rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 4px;
    }
    .value {
      font-size: 1.05rem;
      line-height: 1.35;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin: 14px 0 18px;
    }
    button, a.button {
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      padding: 10px 14px;
      border-radius: 999px;
      font: inherit;
      cursor: pointer;
      text-decoration: none;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
    }
    button.secondary {
      background: var(--accent-2);
      border-color: var(--accent-2);
      color: #fff;
    }
    textarea {
      width: 100%;
      min-height: 110px;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
      font: inherit;
      resize: vertical;
      background: #fff;
      color: var(--text);
    }
    .editor-actions {
      display: flex;
      justify-content: flex-end;
      margin: 10px 0 0;
    }
    .editor-actions button:disabled {
      opacity: 0.5;
      cursor: default;
    }
    .status {
      margin-top: 14px;
      min-height: 1.4em;
      color: var(--muted);
    }
    .status.error { color: #8a1e12; }
    .status.ok { color: #1c5a2a; }
    .suggestions {
      margin-top: 20px;
      border-top: 1px solid var(--border);
      padding-top: 18px;
    }
    .suggestion-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin: 10px 0 14px;
    }
    .provider-toggle {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: #fff;
      cursor: pointer;
    }
    .provider-toggle input {
      margin: 0;
    }
    .suggestion-grid {
      display: grid;
      gap: 12px;
    }
    .suggestion-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      background: #fff;
    }
    .suggestion-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .suggestion-text {
      margin: 6px 0 0;
      line-height: 1.5;
      white-space: pre-wrap;
    }
    .suggestion-use {
      padding: 6px 10px;
      font-size: 0.84rem;
    }
    .preview-wrap {
      margin-bottom: 18px;
    }
    .race-results {
      display: grid;
      gap: 14px;
    }
    .state-header {
      margin: 0 0 12px;
      padding: 10px 0 8px;
      border-bottom: 2px solid var(--line);
      font-size: 1.3rem;
      letter-spacing: -0.01em;
      color: var(--heading);
    }
    .office-block {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff;
      padding: 0 14px 14px;
    }
    .office-title {
      margin: 0 0 10px;
      font-size: 1.02rem;
      color: var(--heading);
      padding: 10px 14px 8px;
      margin-left: -15px;
      margin-right: -15px;
      border: 1px solid var(--line);
      border-bottom: 0;
      border-radius: 8px 8px 0 0;
      background: #fff;
    }
    .race-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.94rem;
    }
    .race-table th,
    .race-table td {
      border: 1px solid var(--line);
      padding: 9px 11px;
      text-align: left;
      vertical-align: top;
    }
    .race-table th {
      background: #f3efe7;
      font-weight: 700;
      font-family: "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif;
      color: var(--heading);
    }
    .mobile-race-list {
      display: none;
    }
    .mobile-race-item {
      padding: 10px 0;
      border-top: 1px solid #ece6da;
    }
    .mobile-race-item:first-child {
      border-top: 0;
      padding-top: 0;
    }
    .mobile-race-head {
      margin: 0 0 8px;
      line-height: 1.45;
      overflow-wrap: anywhere;
      display: flex;
      flex-wrap: wrap;
      align-items: baseline;
      gap: 6px;
    }
    .evidence-link {
      display: block;
      padding: 0;
      border: 0;
      background: none;
      font-size: 0.86rem;
      line-height: 1.4;
      color: var(--accent);
      text-decoration: underline;
      text-decoration-color: rgba(41, 90, 115, 0.35);
      text-underline-offset: 2px;
      overflow-wrap: anywhere;
    }
    .evidence-domain {
      display: block;
      margin-left: 20px;
      margin-top: 2px;
      font-family: "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: 0.74rem;
      color: var(--muted);
      text-decoration: none;
      letter-spacing: 0.01em;
    }
    .evidence-index {
      font-family: "Segoe UI", "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: 0.77rem;
      font-weight: 700;
      color: var(--muted);
      margin-right: 6px;
    }
    .done {
      text-align: center;
      padding: 48px 20px;
      color: var(--muted);
      font-size: 1.1rem;
    }
    @media (max-width: 700px) {
      .office-block {
        padding: 0 12px 12px;
      }
      .office-title {
        margin-left: -13px;
        margin-right: -13px;
      }
      .race-table {
        display: none;
      }
      .mobile-race-list {
        display: grid;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Example Descriptions</h1>
    <p class="sub">One evidence row per not-yet-covered candidate. Save your rewritten description to <code>exampleDescriptions.csv</code>.</p>
    <div id="app" class="panel">
      <div class="done">Loading…</div>
    </div>
  </div>
  <template id="race-partial">__RACE_PARTIAL__</template>
  <script>
    let currentItem = null;
    let providers = [];
    let descriptionHistory = [];
    const PROVIDER_STORAGE_KEY = "example-description-enabled-providers-v1";
    const VERDICT_META = {
      nice: { label: "+ Safety", bg: "#E8F5E9", color: "#2E7D32", border: "#C8E6C9" },
      nuanced: { label: "~ Mixed", bg: "#FFF8E1", color: "#8D6E63", border: "#FFE0B2" },
      no_record: { label: "– No Record", bg: "#F2F2F2", color: "#444", border: "#ccc" },
      naughty: { label: "× Acceleration", bg: "#FDECEC", color: "#C62828", border: "#F5C6CB" }
    };

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function setStatus(text, cls="") {
      const el = document.getElementById("status");
      if (!el) return;
      el.className = "status" + (cls ? " " + cls : "");
      el.textContent = text;
    }

    function verdictBadge(verdict) {
      const m = VERDICT_META[verdict] || VERDICT_META.no_record;
      return `<span style="display:inline-block;padding:2px 8px;border-radius:6px;font-size:0.78rem;font-weight:700;background:${m.bg};color:${m.color};border:1px solid ${m.border}">${m.label}</span>`;
    }

    function getRacePartialHtml() {
      const el = document.getElementById("race-partial");
      if (el) return el.innerHTML.trim();
      return `<article class="{{ARTICLE_CLASS}}"><h3 class="{{TITLE_CLASS}}">{{TITLE}}</h3>{{BODY}}</article>`;
    }

    function renderRacePartial(values) {
      let html = getRacePartialHtml();
      for (const [key, value] of Object.entries(values)) {
        html = html.replaceAll(`{{${key}}}`, value);
      }
      return html;
    }

    function render(item) {
      const app = document.getElementById("app");
      if (!item) {
        app.innerHTML = '<div class="done">No uncovered candidates remain.</div>';
        return;
      }
      currentItem = item;
      descriptionHistory = [item.source_description || ""];
      app.innerHTML = `
        <div class="preview-wrap">
          <span class="label">Guide Preview</span>
          <div class="race-results">
            <section>
              <h2 class="state-header">${escapeHtml(item.state)}</h2>
              <div id="guidePreviewBlock"></div>
            </section>
          </div>
        </div>
        <label class="label" for="newDescription">Your Description</label>
        <textarea id="newDescription" placeholder="Short display description..."></textarea>
        <div class="editor-actions">
          <button id="undoBtn" type="button">Undo</button>
        </div>
        <div class="actions">
          <a class="button" href="${escapeHtml(item.url)}" target="_blank" rel="noopener noreferrer">Open Source</a>
          <button id="skipBtn">Skip</button>
          <button id="saveBtn" class="primary">Save and Next</button>
          <button id="refreshBtn" class="secondary">Randomize</button>
        </div>
        <div class="suggestions">
          <span class="label">AI Second Opinions</span>
          <div id="suggestionActions" class="suggestion-actions"></div>
          <div id="suggestionGrid" class="suggestion-grid"></div>
        </div>
        <div id="status" class="status"></div>
      `;
      const textarea = document.getElementById("newDescription");
      textarea.value = item.source_description || "";
      textarea.addEventListener("input", updatePreviewFromTextarea);
      document.getElementById("undoBtn").addEventListener("click", undoDescriptionChange);
      updateUndoButton();
      updatePreviewFromTextarea();
      textarea.focus();
      document.getElementById("skipBtn").addEventListener("click", loadNext);
      document.getElementById("refreshBtn").addEventListener("click", loadNext);
      document.getElementById("saveBtn").addEventListener("click", saveCurrent);
      renderSuggestionButtons();
      loadEnabledSuggestions();
    }

    function buildPreviewOfficeBlock(item, description) {
      const party = item.party ? ` (${escapeHtml(item.party)})` : "";
      const verdict = verdictBadge(item.verdict || "no_record");
      const url = item.url || "";
      const domain = safeDomain(url);
      const desktopEvidence = description
        ? `<div><a class="evidence-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">
            <span class="evidence-index">[1]</span>${escapeHtml(description)}
           </a>${domain ? `<span class="evidence-domain">${escapeHtml(domain)}</span>` : ""}</div>`
        : '<span style="color:var(--muted);font-size:0.85rem">—</span>';
      const mobileEvidence = description
        ? `<li><a class="evidence-link" href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(description)}</a></li>`
        : '<li style="color:var(--muted)">No evidence linked.</li>';
      const body = `<table class="race-table">
                  <thead>
                    <tr>
                      <th>Candidate</th>
                      <th>Party</th>
                      <th>AI Verdict</th>
                      <th>Evidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td><strong>${escapeHtml(item.candidate)}</strong></td>
                      <td style="text-align:center">${escapeHtml(item.party || "")}</td>
                      <td style="text-align:center">${verdict}</td>
                      <td>${desktopEvidence}</td>
                    </tr>
                  </tbody>
                </table>
                <div class="mobile-race-list">
                  <div class="mobile-race-item">
                    <p class="mobile-race-head"><strong>${escapeHtml(item.candidate)}</strong>${party} <span class="verdict-inline">${verdict}</span></p>
                    <div class="evidence-wrap">
                      <ul class="mobile-evidence-list">${mobileEvidence}</ul>
                    </div>
                  </div>
                </div>`;
      return renderRacePartial({
        ARTICLE_CLASS: "office-block",
        TITLE_CLASS: "office-title",
        TITLE: escapeHtml(item.office),
        BODY: body
      });
    }

    function getEnabledProviders() {
      if (!providers.length) return [];
      const raw = localStorage.getItem(PROVIDER_STORAGE_KEY);
      if (!raw) {
        const defaults = providers.map((p) => p.name);
        localStorage.setItem(PROVIDER_STORAGE_KEY, JSON.stringify(defaults));
        return defaults;
      }
      try {
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) throw new Error("bad");
        return providers.map((p) => p.name).filter((name) => parsed.includes(name));
      } catch {
        const defaults = providers.map((p) => p.name);
        localStorage.setItem(PROVIDER_STORAGE_KEY, JSON.stringify(defaults));
        return defaults;
      }
    }

    function setEnabledProviders(names) {
      localStorage.setItem(PROVIDER_STORAGE_KEY, JSON.stringify(names));
    }

    function suggestionCardId(provider) {
      return "suggestion-" + provider;
    }

    function removeSuggestion(provider) {
      const existing = document.getElementById(suggestionCardId(provider));
      if (existing) existing.remove();
    }

    function syncSuggestionStatusMessage() {
      const grid = document.getElementById("suggestionGrid");
      if (!grid) return;
      const enabled = getEnabledProviders();
      const cards = Array.from(grid.querySelectorAll(".suggestion-card"));
      const statusCard = document.getElementById("suggestion-status");

      if (!enabled.length) {
        if (!statusCard) {
          const card = document.createElement("div");
          card.className = "suggestion-card";
          card.id = "suggestion-status";
          card.innerHTML = '<span class="label">Status</span><p class="suggestion-text">No providers enabled.</p>';
          grid.appendChild(card);
        }
        return;
      }

      if (statusCard && cards.length > 1) {
        statusCard.remove();
      } else if (statusCard && enabled.length) {
        statusCard.remove();
      }
    }

    function renderSuggestionButtons() {
      const actions = document.getElementById("suggestionActions");
      const grid = document.getElementById("suggestionGrid");
      if (!actions || !grid) return;
      actions.innerHTML = "";
      if (!providers.length) {
        grid.innerHTML = '<div class="suggestion-card"><span class="label">Status</span><p class="suggestion-text">No supported AI CLIs found on PATH.</p></div>';
        return;
      }
      const enabled = new Set(getEnabledProviders());
      for (const provider of providers) {
        const label = document.createElement("label");
        label.className = "provider-toggle";
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = enabled.has(provider.name);
        checkbox.addEventListener("change", () => {
          const current = new Set(getEnabledProviders());
          if (checkbox.checked) {
            current.add(provider.name);
          } else {
            current.delete(provider.name);
          }
          setEnabledProviders(Array.from(current));
          syncEnabledSuggestions();
        });
        const text = document.createElement("span");
        text.textContent = provider.label;
        label.appendChild(checkbox);
        label.appendChild(text);
        actions.appendChild(label);
      }
    }

    function renderSuggestion(name, text, error=false) {
      const grid = document.getElementById("suggestionGrid");
      if (!grid) return;
      const card = document.createElement("div");
      card.className = "suggestion-card";
      card.innerHTML = `
        <div class="suggestion-head">
          <span class="label">${escapeHtml(name)}</span>
          ${error ? "" : `<button type="button" class="suggestion-use" data-suggestion="${escapeHtml(name)}">Use</button>`}
        </div>
        <p class="suggestion-text"${error ? ' style="color:#8a1e12;"' : ""}>${escapeHtml(text)}</p>
      `;
      const existing = document.getElementById(suggestionCardId(name));
      if (existing) existing.remove();
      card.id = suggestionCardId(name);
      grid.appendChild(card);
      const useButton = card.querySelector(".suggestion-use");
      if (useButton) {
        useButton.addEventListener("click", () => applySuggestionToDescription(text));
      }
      syncSuggestionStatusMessage();
    }

    function updateUndoButton() {
      const button = document.getElementById("undoBtn");
      if (!button) return;
      button.disabled = descriptionHistory.length <= 1;
    }

    function pushDescriptionHistory(value) {
      if (!descriptionHistory.length || descriptionHistory[descriptionHistory.length - 1] !== value) {
        descriptionHistory.push(value);
      }
      updateUndoButton();
    }

    function updatePreviewFromTextarea() {
      const textarea = document.getElementById("newDescription");
      const target = document.getElementById("guidePreviewBlock");
      if (!textarea || !target) return;
      const description = textarea.value.trim();
      target.innerHTML = buildPreviewOfficeBlock(currentItem, description);
    }

    function applySuggestionToDescription(text) {
      const textarea = document.getElementById("newDescription");
      if (!textarea) return;
      pushDescriptionHistory(textarea.value);
      textarea.value = text;
      textarea.focus();
      updatePreviewFromTextarea();
    }

    function undoDescriptionChange() {
      const textarea = document.getElementById("newDescription");
      if (!textarea || descriptionHistory.length <= 1) return;
      descriptionHistory.pop();
      textarea.value = descriptionHistory[descriptionHistory.length - 1];
      textarea.focus();
      updateUndoButton();
      updatePreviewFromTextarea();
    }

    function safeDomain(url) {
      try {
        return new URL(url).hostname.replace(/^www\\./, "");
      } catch {
        return "";
      }
    }

    async function fetchSuggestion(provider) {
      renderSuggestion(provider, "Loading…");
      const res = await fetch("/api/suggest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider,
          candidate: currentItem.candidate,
          url: currentItem.url,
          source_description: currentItem.source_description
        })
      });
      const data = await res.json();
      if (!res.ok) {
        renderSuggestion(provider, data.error || "Suggestion failed.", true);
        return;
      }
      renderSuggestion(provider, data.suggestion || "(empty)");
    }

    function syncEnabledSuggestions() {
      const grid = document.getElementById("suggestionGrid");
      if (!grid) return;
      const enabled = getEnabledProviders();
      const enabledSet = new Set(enabled);
      for (const provider of providers) {
        if (!enabledSet.has(provider.name)) {
          removeSuggestion(provider.name);
        }
      }
      for (const provider of enabled) {
        if (!document.getElementById(suggestionCardId(provider))) {
          fetchSuggestion(provider);
        }
      }
      syncSuggestionStatusMessage();
    }

    async function loadEnabledSuggestions() {
      const grid = document.getElementById("suggestionGrid");
      if (!grid) return;
      grid.innerHTML = "";
      syncEnabledSuggestions();
    }

    async function loadNext() {
      setStatus("Loading…");
      const res = await fetch("/api/random");
      const data = await res.json();
      render(data.item);
    }

    async function loadProviders() {
      const res = await fetch("/api/providers");
      const data = await res.json();
      providers = data.providers || [];
    }

    async function saveCurrent() {
      const textarea = document.getElementById("newDescription");
      const description = textarea.value.trim();
      if (!description) {
        setStatus("Write a description before saving.", "error");
        return;
      }
      const payload = {
        candidate: currentItem.candidate,
        source_description: description,
        url: currentItem.url
      };
      setStatus("Saving…");
      const res = await fetch("/api/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Save failed.", "error");
        return;
      }
      setStatus("Saved.", "ok");
      await loadNext();
    }

    Promise.all([loadProviders(), loadNext()]).catch((err) => {
      document.getElementById("app").innerHTML = '<div class="done">Failed to load.</div>';
      console.error(err);
    });
  </script>
</body>
</html>
"""


def load_candidates_by_name() -> dict[str, dict[str, str]]:
    by_name: dict[str, dict[str, str]] = {}
    with (DATA_DIR / "candidates.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            candidate = row["Candidate"].strip()
            if candidate and candidate not in by_name:
                by_name[candidate] = row
    return by_name


def ensure_output_csv() -> None:
    if OUTPUT_CSV.exists():
        return
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Candidate", "Source_Description", "URL"])
        writer.writeheader()


def covered_candidates() -> set[str]:
    ensure_output_csv()
    covered: set[str] = set()
    with OUTPUT_CSV.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            candidate = row["Candidate"].strip()
            if candidate:
                covered.add(candidate)
    return covered


def random_item() -> dict[str, str] | None:
    candidates_by_name = load_candidates_by_name()
    covered = covered_candidates()
    pool: list[dict[str, str]] = []
    with EVIDENCE_CSV.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            candidate = row["Candidate"].strip()
            if not candidate or candidate in covered:
                continue
            candidate_row = candidates_by_name.get(candidate)
            if not candidate_row:
                continue
            pool.append(
                {
                    "candidate": candidate,
                    "state": candidate_row["State"].strip(),
                    "office": candidate_row["Office"].strip(),
                    "party": candidate_row.get("Party", "").strip(),
                    "verdict": candidate_row.get("Verdict", "").strip() or "no_record",
                    "source_description": row["Source_Description"].strip(),
                    "url": row["URL"].strip(),
                }
            )
    if not pool:
        return None
    return random.choice(pool)


def append_example(row: dict[str, str]) -> None:
    ensure_output_csv()
    with OUTPUT_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Candidate", "Source_Description", "URL"])
        writer.writerow(
            {
                "Candidate": row["candidate"].strip(),
                "Source_Description": row["source_description"].strip(),
                "URL": row["url"].strip(),
            }
        )


def available_providers() -> list[dict[str, str]]:
    providers = []
    if resolve_cli("codex"):
        providers.append({"name": "codex", "label": "Codex"})
    if resolve_cli("claude"):
        providers.append({"name": "claude", "label": "Claude"})
    if resolve_cli("gemini") and gemini_auth_configured():
        providers.append({"name": "gemini", "label": "Gemini"})
    return providers


def resolve_cli(name: str) -> str | None:
    path = shutil.which(name)
    if path:
        return path
    candidate = USER_BIN_DIR / name
    if candidate.exists():
        return str(candidate)
    return None


def env_file_has_auth(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        for name in AUTH_ENV_VARS:
            if re.match(rf"^(export\s+)?{re.escape(name)}\s*=", stripped):
                return True
    return False


def gemini_auth_configured() -> bool:
    for name in AUTH_ENV_VARS:
        if os.environ.get(name):
            return True

    candidate_files = [
        ROOT / ".gemini" / ".env",
        HOME_GEMINI_DIR / ".env",
        Path.home() / ".env",
        HOME_GEMINI_DIR / "settings.json",
    ]
    return any(path.exists() and (path.suffix == ".json" or env_file_has_auth(path)) for path in candidate_files)


def provider_env() -> dict[str, str]:
    env = dict(os.environ)
    path_parts = []
    if NODE20_BIN.exists():
        path_parts.append(str(NODE20_BIN.parent))
    path_parts.append(env.get("PATH", ""))
    env["PATH"] = ":".join(part for part in path_parts if part)
    return env


def build_provider_command(provider: str, prompt: str) -> list[str]:
    if provider == "codex":
        raise ValueError("Codex command requires a temporary output path")

    cli_path = resolve_cli(provider) or provider
    if provider == "claude":
        if NODE20_BIN.exists():
            return [str(NODE20_BIN), cli_path, "-p", prompt]
        return [cli_path, "-p", prompt]
    if provider == "gemini":
        if NODE20_BIN.exists():
            return [str(NODE20_BIN), cli_path, "-p", prompt]
        return [cli_path, "-p", prompt]
    return [cli_path, prompt]


def fetch_page_excerpt(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ExampleDescriptionsUI/1.0)"
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", raw)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:12000]


def suggest_with_provider(provider: str, candidate: str, url: str, source_description: str) -> str:
    page_excerpt = fetch_page_excerpt(url)
    prompt = (
        "You are helping standardize short evidence descriptions for a voter guide.\n"
        "Write exactly one concise description for the source.\n"
        "Rules:\n"
        "- Do not mention the candidate's name.\n"
        "- Aim for 4 to 10 words before the source name.\n"
        "- Use factual, neutral wording.\n"
        "- No quote marks.\n"
        "- Output only the description text, not bullets or explanation.\n\n"
        f"Candidate: {candidate}\n"
        f"Existing description: {source_description}\n"
        f"URL: {url}\n"
        f"Page excerpt: {page_excerpt}\n"
    )

    if provider not in {"codex", "claude", "gemini"}:
        raise ValueError(f"Unsupported provider: {provider}")

    output_file: Path | None = None
    if provider == "codex":
        fd, temp_path = tempfile.mkstemp(prefix="codex-last-message-", suffix=".txt")
        os.close(fd)
        output_file = Path(temp_path)
        cmd = [
            resolve_cli("codex") or "codex",
            "exec",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--output-last-message",
            str(output_file),
            prompt,
        ]
    else:
        cmd = build_provider_command(provider, prompt)

    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=SUGGESTION_TIMEOUT_SECONDS,
        env=provider_env(),
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip() or proc.stdout.strip() or "Unknown error"
        if output_file is not None:
            output_file.unlink(missing_ok=True)
        raise RuntimeError(stderr)
    if output_file is not None:
        output = output_file.read_text(encoding="utf-8", errors="ignore").strip()
        output_file.unlink(missing_ok=True)
    else:
        output = proc.stdout.strip()
    if not output:
        raise RuntimeError("Empty response.")
    return output.splitlines()[-1].strip()


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.replace(
            "__RACE_PARTIAL__",
            RACE_PARTIAL_HTML.read_text(encoding="utf-8").strip(),
        ).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/", "/index.html"):
            self._send_html(HTML)
            return
        if self.path == "/api/providers":
            self._send_json({"providers": available_providers()})
            return
        if self.path == "/api/random":
            self._send_json({"item": random_item()})
            return
        self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in {"/api/save", "/api/suggest"}:
            self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)
            return

        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/api/suggest":
            provider = str(payload.get("provider", "")).strip()
            candidate = str(payload.get("candidate", "")).strip()
            source_description = str(payload.get("source_description", "")).strip()
            url = str(payload.get("url", "")).strip()
            if not provider or not candidate or not source_description or not url:
                self._send_json({"error": "Missing required fields."}, HTTPStatus.BAD_REQUEST)
                return
            try:
                suggestion = suggest_with_provider(provider, candidate, url, source_description)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": str(exc)}, HTTPStatus.BAD_GATEWAY)
                return
            self._send_json({"provider": provider, "suggestion": suggestion})
            return

        candidate = str(payload.get("candidate", "")).strip()
        source_description = str(payload.get("source_description", "")).strip()
        url = str(payload.get("url", "")).strip()

        if not candidate or not source_description or not url:
            self._send_json({"error": "Missing required fields."}, HTTPStatus.BAD_REQUEST)
            return

        append_example(
            {
                "candidate": candidate,
                "source_description": source_description,
                "url": url,
            }
        )
        self._send_json({"ok": True})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def main() -> None:
    ensure_output_csv()
    try:
        server = ThreadingHTTPServer((HOST, PORT), Handler)
    except PermissionError as exc:
        print(
            f"Failed to bind http://{HOST}:{PORT} ({exc}). "
            "Try a different port or run outside a restricted sandbox.",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    except OSError as exc:
        print(
            f"Failed to bind http://{HOST}:{PORT} ({exc}). "
            "The port may already be in use.",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc
    print(f"Serving example description UI at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
