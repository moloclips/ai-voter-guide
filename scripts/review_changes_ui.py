from __future__ import annotations

import csv
import json
import os
import tempfile
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parent.parent
CHANGES_CSV = ROOT / "changes.csv"
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
RACES_CSV = ROOT / "data" / "races.csv"
RACE_RUNNER = ROOT / "scripts" / "race_runner.py"
RACE_RUNNER_PROMPT = ROOT / "scripts" / "race_runner_prompt.txt"
APPLY_CHANGES = ROOT / "scripts" / "apply_changes.py"
SUMMARIZE_RACE_RUNNER_LOGS = ROOT / "scripts" / "summarize_race_runner_logs.py"
WIKIPEDIA_EXCLUSIONS_CSV = ROOT / "data" / "wikipedia_description_exclusions.csv"
RACE_RUNNER_LOG_SUMMARY_CSV = ROOT / "data" / "race_runner_log_summary.csv"
HOST = "127.0.0.1"
PORT = 8767
FIELDNAMES = ["change_id", "table", "key", "action", "reasoning", "field", "value", "status"]
VALID_STATUSES = {"pending", "approved", "denied", "applied", "conflict"}
POLL_SECONDS = 1.5
MAX_LOG_CHARS = 50000


def load_race_runner_prompt_template() -> str:
    return RACE_RUNNER_PROMPT.read_text(encoding="utf-8")


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Review Changes</title>
  <link rel="icon" href="data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 viewBox=%270 0 100 100%27%3E%3Ctext y=%27.9em%27 font-size=%2790%27%3E%F0%9F%87%BA%F0%9F%87%B8%3C/text%3E%3C/svg%3E">
  <style>
    :root {
      --bg: #f5f1e8;
      --panel: #fffaf0;
      --panel-dark: #f2e4cf;
      --panel-light: #fffaf2;
      --panel-lighter: #fffdf9;
      --text: #1f1a17;
      --muted: #6b6158;
      --border: #d9cdbd;
      --accent: #8d4f2d;
      --ok: #1c5a2a;
      --bad: #8a1e12;
      --info: #365a7b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background:
        radial-gradient(circle at top right, rgba(141,79,45,0.08), transparent 28%),
        linear-gradient(180deg, #efe6d5 0%, var(--bg) 100%);
      color: var(--text);
      font-family: Georgia, "Times New Roman", serif;
    }
    .wrap {
      max-width: none;
      width: 100%;
      margin: 0;
      padding: 28px 20px 48px;
    }
    .tabs {
      display: flex;
      gap: 10px;
      margin: 0 0 18px;
      flex-wrap: wrap;
    }
    .tab-btn {
      background: rgba(255,255,255,0.72);
    }
    .tab-btn.active {
      background: var(--accent);
      border-color: var(--accent);
      color: #fffaf0;
    }
    .tab-panel[hidden] {
      display: none;
    }
    h1 {
      margin: 0 0 6px;
      font-size: 2rem;
    }
    .sub {
      margin: 0 0 18px;
      color: var(--muted);
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(360px, 1fr) minmax(720px, 2fr);
      gap: 18px;
      align-items: start;
    }
    .panel {
      border: 1px solid var(--border);
      border-radius: 16px;
      background: var(--panel);
      box-shadow: 0 10px 28px rgba(40, 28, 17, 0.08);
      overflow: hidden;
    }
    .panel-header {
      padding: 14px 16px;
      border-bottom: 1px solid var(--border);
      background: rgba(255,255,255,0.6);
      font-weight: 700;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
    }
    .panel-body {
      padding: 16px;
    }
    .stack {
      display: grid;
      gap: 12px;
    }
    label {
      display: grid;
      gap: 6px;
      font-size: 0.92rem;
    }
    select, input, button, textarea {
      font: inherit;
    }
    select, input, textarea {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 12px;
      background: #fff;
      color: var(--text);
    }
    textarea {
      min-height: 320px;
      resize: vertical;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.85rem;
      line-height: 1.45;
    }
    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    button {
      border: 1px solid var(--border);
      background: #fff;
      color: var(--text);
      padding: 9px 14px;
      border-radius: 999px;
      cursor: pointer;
    }
    button.primary {
      background: var(--accent);
      color: #fffaf0;
      border-color: var(--accent);
    }
    button.danger {
      color: var(--bad);
    }
    button.good {
      color: var(--ok);
    }
    .statusline {
      min-height: 1.4em;
      color: var(--muted);
      margin-bottom: 14px;
    }
    .statusline.ok { color: var(--ok); }
    .statusline.error { color: var(--bad); }
    .statusline.info { color: var(--info); }
    .jobs {
      display: grid;
      gap: 12px;
    }
    .job-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      background: #fff;
      padding: 12px;
    }
    .job-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      margin-bottom: 8px;
    }
    .job-title {
      font-weight: 700;
    }
    .job-meta {
      color: var(--muted);
      font-size: 0.84rem;
    }
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 0.78rem;
      border: 1px solid var(--border);
      background: #f8f3ea;
    }
    .action-badge {
      font-weight: 700;
    }
    .inline-cards {
      display: grid;
      gap: 12px;
      margin-top: 12px;
    }
    .badge.running { color: var(--info); }
    .badge.succeeded { color: var(--ok); }
    .badge.failed { color: var(--bad); }
    pre.log {
      margin: 10px 0 0;
      padding: 12px;
      border-radius: 10px;
      background: #201915;
      color: #f7f0e7;
      font-size: 0.82rem;
      line-height: 1.45;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .state-group, .race-group, .candidate-group {
      border: 1px solid var(--border);
      border-radius: 14px;
      margin-bottom: 16px;
      overflow: hidden;
      box-shadow: 0 8px 24px rgba(40, 28, 17, 0.08);
    }
    .state-group, .candidate-group {
      background: var(--panel-dark);
    }
    .race-group, .change-card {
      background: var(--panel-light);
    }
    .state-header, .race-header, .candidate-header {
      padding: 12px 16px;
      border-bottom: 1px solid var(--border);
      background: rgba(255,255,255,0.6);
      font-weight: 700;
    }
    .race-group, .candidate-group {
      margin: 12px;
      box-shadow: none;
    }
    .change-block {
      border-top: 1px solid #eadfce;
      padding: 14px 16px 18px;
      background: var(--panel-lighter);
    }
    .change-block:first-of-type { border-top: 0; }
    .key-header {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      margin-bottom: 10px;
    }
    .key-title {
      font-size: 1.05rem;
      font-weight: 700;
    }
    .key-meta {
      color: var(--muted);
      font-size: 0.88rem;
    }
    .change-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px 14px;
      margin-top: 10px;
    }
    .change-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      margin-bottom: 8px;
    }
    .reasoning {
      margin: 0 0 10px;
      line-height: 1.45;
    }
    .diff-old, .diff-new {
      display: block;
      padding: 3px 6px;
      border-radius: 6px;
      margin-top: 4px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.86rem;
    }
    .diff-old {
      background: #f8d9d5;
      color: #7a2117;
    }
    .diff-new {
      background: #dff2de;
      color: #1f5c2c;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.92rem;
      table-layout: fixed;
    }
    th, td {
      border: 1px solid #e7dccb;
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    th {
      background: #f3efe7;
      font-weight: 700;
    }
    .auto-table {
      width: auto;
      min-width: 100%;
      table-layout: auto;
    }
    .sort-btn {
      width: 100%;
      padding: 0;
      border: 0;
      background: transparent;
      text-align: left;
      font: inherit;
      color: inherit;
      border-radius: 0;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }
    .empty {
      color: var(--muted);
      padding: 30px 8px 0;
      text-align: center;
    }
    .inspect-controls {
      display: flex;
      gap: 12px;
      align-items: end;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }
    .inspect-controls label {
      min-width: 220px;
      flex: 0 1 320px;
    }
    .inspect-chart {
      display: grid;
      grid-template-columns: minmax(420px, 700px) minmax(260px, 1fr);
      gap: 18px;
      align-items: center;
      margin-bottom: 18px;
      padding: 12px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: #fffdf8;
    }
    .inspect-chart svg {
      width: 100%;
      height: auto;
      display: block;
    }
    .inspect-legend {
      display: grid;
      gap: 8px;
      align-content: start;
    }
    .legend-row {
      display: grid;
      grid-template-columns: 14px minmax(0, 1fr) auto;
      gap: 10px;
      align-items: center;
      font-size: 0.92rem;
    }
    .legend-swatch {
      width: 14px;
      height: 14px;
      border-radius: 4px;
      border: 1px solid rgba(31, 26, 23, 0.12);
    }
    .legend-label {
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .legend-value {
      color: var(--muted);
      white-space: nowrap;
    }
    .inspect-table-wrap {
      overflow: auto;
      max-height: calc(100vh - 260px);
    }
    .wiki-panel {
      margin-bottom: 18px;
      padding: 12px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: #fffdf8;
    }
    .wiki-toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      margin-bottom: 12px;
    }
    .wiki-list {
      display: grid;
      gap: 10px;
      max-height: 520px;
      overflow: auto;
    }
    .wiki-item {
      border: 1px solid #e7dccb;
      border-radius: 12px;
      padding: 10px 12px;
      background: #fff;
    }
    .wiki-item label {
      display: flex;
      gap: 10px;
      align-items: flex-start;
    }
    .wiki-main {
      display: grid;
      gap: 6px;
      min-width: 0;
    }
    .wiki-url {
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.84rem;
      overflow-wrap: anywhere;
    }
    .wiki-descs {
      color: var(--muted);
      font-size: 0.9rem;
      overflow-wrap: anywhere;
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
      .inspect-chart { grid-template-columns: 1fr; }
    }
    @media (max-width: 700px) {
      .change-head, .key-header, .job-head {
        flex-direction: column;
      }
      table, thead, tbody, tr, th, td { display: block; }
      thead { display: none; }
      tr { margin-bottom: 10px; }
      td { border: 1px solid #e7dccb; }
      td::before {
        content: attr(data-label);
        display: block;
        font-size: 0.78rem;
        color: var(--muted);
        text-transform: uppercase;
        margin-bottom: 4px;
      }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Review Changes</h1>
    <p class="sub">Launch background research runs, inspect live logs, review queued changes, and apply approved edits from one place.</p>
    <div id="statusline" class="statusline"></div>
    <nav class="tabs" aria-label="Primary">
      <button id="tabRunBtn" class="tab-btn active" type="button" data-tab="run">Run Research</button>
      <button id="tabReviewBtn" class="tab-btn" type="button" data-tab="review">Review Changes</button>
      <button id="tabInspectBtn" class="tab-btn" type="button" data-tab="inspect">Inspect Data</button>
      <button id="tabLogsBtn" class="tab-btn" type="button" data-tab="logs">Inspect Logs</button>
    </nav>
    <section id="tab-run" class="tab-panel">
      <div class="grid">
        <section class="panel">
          <div class="panel-header">Run Research</div>
          <div class="panel-body stack">
            <label>Provider
              <select id="provider"></select>
            </label>
            <label>Race
              <input id="raceSearch" type="text" placeholder="Filter races, e.g. colorado house">
              <select id="race"></select>
            </label>
            <label>Candidate
              <select id="candidate"></select>
            </label>
            <label>Verdict Filter
              <select id="candidateVerdict">
                <option value="">Any verdict</option>
                <option value="no_record">no_record</option>
                <option value="nice">nice</option>
                <option value="nuanced">nuanced</option>
                <option value="naughty">naughty</option>
              </select>
            </label>
            <label><input id="skipCandidatesWithChanges" type="checkbox"> Skip candidates already covered in changes.csv</label>
            <label>Prompt Template
              <textarea id="promptTemplate" spellcheck="false"></textarea>
            </label>
            <div class="toolbar">
              <button id="runBtn" class="primary" type="button">Start Run</button>
              <button id="runAllBtn" type="button">Run All Races</button>
              <button id="clearFinishedBtn" type="button">Clear Finished Jobs</button>
              <button id="reloadBtn" type="button">Reload</button>
            </div>
          </div>
        </section>
        <section class="panel">
          <div class="panel-header">Live Job Output</div>
          <div class="panel-body">
            <div id="jobsMirror" class="jobs"></div>
          </div>
        </section>
      </div>
    </section>
    <section id="tab-review" class="tab-panel" hidden>
      <section class="panel">
        <div class="panel-header">
          <span id="queuedChangesTitle">Queued Changes</span>
          <div class="toolbar">
            <label><input id="pendingOnly" type="checkbox" checked> Show only pending</label>
            <label><input id="excludeNoRecord" type="checkbox"> Exclude no_record</label>
            <label><input id="verdictChange" type="checkbox"> Verdict change</label>
            <button id="applyBtn" class="good" type="button">Apply Approved</button>
          </div>
        </div>
        <div class="panel-body">
          <div id="changes"></div>
        </div>
      </section>
    </section>
    <section id="tab-inspect" class="tab-panel" hidden>
      <section class="panel">
        <div class="panel-header">Inspect Data</div>
        <div class="panel-body">
          <div class="inspect-chart">
            <div id="domainChart"></div>
            <div id="domainLegend" class="inspect-legend"></div>
          </div>
          <div class="inspect-chart">
            <div id="verdictChart"></div>
            <div id="verdictLegend" class="inspect-legend"></div>
          </div>
          <div class="inspect-chart">
            <div id="coverageChart"></div>
            <div id="coverageLegend" class="inspect-legend"></div>
          </div>
          <div class="wiki-panel">
            <div class="wiki-toolbar">
              <button id="exportWikiExclusionsBtn" type="button">Export Wikipedia Exclusions</button>
              <span class="job-meta">Check rows whose current descriptions should be preserved.</span>
            </div>
            <div id="wikiList" class="wiki-list"></div>
          </div>
          <div class="inspect-controls">
            <label>Table
              <select id="inspectTable">
                <option value="races">races.csv</option>
                <option value="candidates">candidates.csv</option>
                <option value="evidence">evidence.csv</option>
                <option value="changes">changes.csv</option>
              </select>
            </label>
            <label>Filter
              <input id="inspectFilter" type="text" placeholder="Substring filter">
            </label>
            <button id="inspectReloadBtn" type="button">Reload Data</button>
          </div>
          <div class="inspect-table-wrap">
            <div id="inspectData"></div>
          </div>
        </div>
      </section>
    </section>
    <section id="tab-logs" class="tab-panel" hidden>
      <section class="panel">
        <div class="panel-header">Inspect Logs</div>
        <div class="panel-body">
          <div class="inspect-controls">
            <label>Filter
              <input id="logsFilter" type="text" placeholder="Substring filter">
            </label>
            <button id="logsReloadBtn" type="button">Reload Logs</button>
          </div>
          <div id="logsColumns" class="toolbar"></div>
          <div class="inspect-table-wrap">
            <div id="logsData"></div>
          </div>
        </div>
      </section>
    </section>
  </div>
  <script>
    let meta = { providers: [], races: [], candidates_by_race: {}, default_prompt_template: "" };
    let logSort = { column: "Candidate", direction: "asc" };
    let logsDataCache = { columns: [], rows: [] };
    const defaultHiddenLogColumns = new Set([
      "Total_Web_Lookups",
      "Total_Command_Executions",
      "Total_Todo_Lists",
      "Total_Rate_Limits",
      "Total_Turns",
    ]);

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function actionLabel(action) {
      const labels = {
        add: "➕ Add",
        mod: "✏️ Mod",
        del: "🗑️ Delete",
        check: "✅ Check"
      };
      return labels[action] || action;
    }

    function formatStatus(status, changeId) {
      const label = status ? status.charAt(0).toUpperCase() + status.slice(1) : "Unknown";
      const icons = {
        pending: "🟡",
        approved: "🟢",
        denied: "🔴",
        applied: "✅",
        conflict: "⚠️"
      };
      return `${icons[status] || "⚪"} ${label} (change_id:${changeId})`;
    }

    function cardTitle(group) {
      const action = group.rows[0]?.action || "";
      if (action === "mod" && group.table === "evidence") {
        return `${actionLabel(action)} (ev_id:${group.key || "?"})`;
      }
      if (action === "add" || action === "del") {
        return actionLabel(action);
      }
      if (action === "check") {
        return actionLabel(action);
      }
      return actionLabel(action);
    }

    function setStatus(text, cls="") {
      const el = document.getElementById("statusline");
      el.className = "statusline" + (cls ? " " + cls : "");
      el.textContent = text;
    }

    function buildHierarchy(groups) {
      const states = new Map();
      for (const group of groups) {
        if (!states.has(group.state)) states.set(group.state, new Map());
        const races = states.get(group.state);
        if (!races.has(group.race)) races.set(group.race, new Map());
        const candidates = races.get(group.race);
        if (!candidates.has(group.candidate)) candidates.set(group.candidate, []);
        candidates.get(group.candidate).push(group);
      }
      return states;
    }

    function visibleFields(group, currentRow) {
      let fields = Object.keys(currentRow || {});
      if (group.table === "evidence") {
        fields = fields.filter((field) => field !== "Candidate" && field !== "Evidence_ID");
      }
      if (group.table === "candidates") {
        fields = fields.filter((field) => field !== "Candidate");
      }
      return fields;
    }

    function renderModTable(group) {
      const currentRow = group.current_row || {};
      const updates = new Map(
        group.rows
          .filter((row) => row.field)
          .map((row) => [row.field, row.value || ""])
      );
      const headers = visibleFields(group, currentRow);
      if (!headers.length) {
        return '<p class="reasoning">No canonical row found for diff view.</p>';
      }
      const headerHtml = headers.map((field) => `<th>${escapeHtml(field)}</th>`).join("");
      const valueHtml = headers.map((field) => {
        const oldValue = currentRow[field] || "—";
        if (!updates.has(field)) {
          return `<td data-label="${escapeHtml(field)}">${escapeHtml(oldValue)}</td>`;
        }
        const newValue = updates.get(field) || "—";
        return `<td data-label="${escapeHtml(field)}"><span class="diff-old">${escapeHtml(oldValue)}</span><span class="diff-new">${escapeHtml(newValue)}</span></td>`;
      }).join("");
      return `
        <table>
          <thead><tr>${headerHtml}</tr></thead>
          <tbody><tr>${valueHtml}</tr></tbody>
        </table>
      `;
    }

    function renderChangeTable(group) {
      const rows = group.rows;
      const action = rows[0]?.action || "";
      if (action === "mod") {
        return renderModTable(group);
      }
      if (action === "del") {
        return '<p class="reasoning">Deletes this row.</p>';
      }
      const cells = rows
        .filter((row) => row.field)
        .filter((row) => !(group.table === "evidence" && (row.field === "Candidate" || row.field === "Evidence_ID")))
        .filter((row) => !(group.table === "candidates" && row.field === "Candidate"))
        .map((row) => ({ field: row.field, value: row.value || "—" }));
      if (!cells.length) {
        return '<p class="reasoning">No field values.</p>';
      }
      const headers = cells.map((cell) => `<th>${escapeHtml(cell.field)}</th>`).join("");
      const values = cells.map((cell) => `<td data-label="${escapeHtml(cell.field)}">${escapeHtml(cell.value)}</td>`).join("");
      return `
        <table>
          <thead><tr>${headers}</tr></thead>
          <tbody><tr>${values}</tr></tbody>
        </table>
      `;
    }

    function renderChanges(groups) {
      const app = document.getElementById("changes");
      const title = document.getElementById("queuedChangesTitle");
      const candidateCount = new Set(groups.map((group) => group.candidate).filter((candidate) => candidate && candidate !== "(no candidate)")).size;
      title.textContent = `Queued Changes (${candidateCount} candidates)`;
      if (!groups.length) {
        app.innerHTML = '<div class="empty">No changes to show.</div>';
        return;
      }
      const grouped = buildHierarchy(groups);
      let html = "";
      for (const [state, races] of grouped) {
        html += `<section class="state-group"><div class="state-header">${escapeHtml(state)}</div>`;
        for (const [race, candidates] of races) {
          html += `<section class="race-group"><div class="race-header">${escapeHtml(race)}</div>`;
          for (const [candidate, changeGroups] of candidates) {
            html += `<section class="candidate-group"><div class="candidate-header">${escapeHtml(candidate)}</div>`;
            const cards = [];
            for (const group of changeGroups) {
              cards.push(`
                <div class="change-block">
                  <article class="change-card">
                    <div class="change-head">
                      <div>
                        <div class="badge action-badge">${escapeHtml(cardTitle(group))}</div>
                        <p class="reasoning"><strong>Reasoning</strong><br>${escapeHtml(group.reasoning || "")}</p>
                      </div>
                      <span class="badge">${escapeHtml(formatStatus(group.status, group.change_id))}</span>
                    </div>
                    ${renderChangeTable(group)}
                    <div class="actions">
                      <button class="good" data-change-id="${escapeHtml(group.change_id)}" data-status="approved">Approve</button>
                      <button class="danger" data-change-id="${escapeHtml(group.change_id)}" data-status="denied">Deny</button>
                      <button data-change-id="${escapeHtml(group.change_id)}" data-status="pending">Reset to Pending</button>
                    </div>
                  </article>
                </div>`);
            }
            if (cards.length) {
              html += `<div class="inline-cards">${cards.join("")}</div>`;
            }
            html += `</section>`;
          }
          html += `</section>`;
        }
        html += `</section>`;
      }
      app.innerHTML = html;
      app.querySelectorAll("[data-change-id]").forEach((button) => {
        button.addEventListener("click", async () => {
          await updateStatus(button.dataset.changeId, button.dataset.status);
        });
      });
    }

    function renderJobs(jobs) {
      const mirror = document.getElementById("jobsMirror");
      if (!jobs.length) {
        if (mirror) mirror.innerHTML = '<div class="empty">No jobs started yet.</div>';
        return;
      }
      const html = jobs.map((job) => `
        <article class="job-card">
          <div class="job-head">
            <div>
              <div class="job-title">${escapeHtml(job.provider.toUpperCase())} · ${escapeHtml(job.race || "apply")}</div>
              <div class="job-meta">${escapeHtml(job.candidate || "all candidates")} · started ${escapeHtml(job.started_at || "")}</div>
            </div>
            <span class="badge ${escapeHtml(job.status)}">${escapeHtml(job.status)}</span>
          </div>
          <div class="job-meta">Job ${escapeHtml(job.id)}</div>
          ${job.can_stop ? `<div class="actions"><button data-stop-job-id="${escapeHtml(job.id)}">Stop After Current Candidate</button></div>` : ""}
          <pre class="log">${escapeHtml(job.log || "")}</pre>
        </article>
      `).join("");
      if (mirror) mirror.innerHTML = html;
      if (mirror) {
        mirror.querySelectorAll("[data-stop-job-id]").forEach((button) => {
          button.addEventListener("click", async () => {
            await requestJobStop(button.dataset.stopJobId);
          });
        });
      }
    }

    function compareValues(a, b, direction) {
      const aText = String(a ?? "").trim();
      const bText = String(b ?? "").trim();
      const aNumber = Number(aText);
      const bNumber = Number(bText);
      const bothNumeric = aText !== "" && bText !== "" && Number.isFinite(aNumber) && Number.isFinite(bNumber);
      if (bothNumeric) {
        return direction === "asc" ? aNumber - bNumber : bNumber - aNumber;
      }
      const aDate = Date.parse(aText);
      const bDate = Date.parse(bText);
      const bothDateLike = !Number.isNaN(aDate) && !Number.isNaN(bDate);
      if (bothDateLike) {
        return direction === "asc" ? aDate - bDate : bDate - aDate;
      }
      return direction === "asc"
        ? aText.localeCompare(bText, undefined, { numeric: true, sensitivity: "base" })
        : bText.localeCompare(aText, undefined, { numeric: true, sensitivity: "base" });
    }

    function renderSortableTable(rootId, data, sortState, onSort, visibleColumns=null, headerLabelFn=null, cellValueFn=null, tableClass="") {
      const root = document.getElementById(rootId);
      const rows = data.rows || [];
      const columns = visibleColumns || data.columns || [];
      if (!rows.length || !columns.length) {
        root.innerHTML = '<div class="empty">No rows to show.</div>';
        return;
      }
      const sortedRows = [...rows].sort((left, right) =>
        compareValues(left[sortState.column] || "", right[sortState.column] || "", sortState.direction)
      );
      const headers = columns.map((column) => {
        const active = sortState.column === column;
        const marker = active ? (sortState.direction === "asc" ? " ▲" : " ▼") : "";
        const label = headerLabelFn ? headerLabelFn(column, rows) : column;
        return `<th><button type="button" class="sort-btn" data-sort-column="${escapeHtml(column)}">${escapeHtml(label)}${marker}</button></th>`;
      }).join("");
      const body = sortedRows.map((row) => {
        const cells = columns.map((column) => {
          const value = cellValueFn ? cellValueFn(column, row[column] || "", row) : (row[column] || "");
          return `<td data-label="${escapeHtml(column)}">${escapeHtml(value)}</td>`;
        }).join("");
        return `<tr>${cells}</tr>`;
      }).join("");
      root.innerHTML = `<table class="${escapeHtml(tableClass)}"><thead><tr>${headers}</tr></thead><tbody>${body}</tbody></table>`;
      root.querySelectorAll("[data-sort-column]").forEach((button) => {
        button.addEventListener("click", () => onSort(button.dataset.sortColumn || ""));
      });
    }

    function renderInspectTable(data) {
      renderSortableTable("inspectData", data, { column: data.columns?.[0] || "", direction: "asc" }, () => {});
      const root = document.getElementById("inspectData");
      root.querySelectorAll(".sort-btn").forEach((button) => {
        button.replaceWith(...button.childNodes);
      });
    }

    function renderLogsTable(data) {
      const visibleColumns = getVisibleLogColumns(data.columns || []);
      const displayNames = {
        Latest_Run: "Updated",
        Total_Web_Lookups: "Web Search",
        Total_Command_Executions: "Commands",
        Total_Todo_Lists: "Todo Lists",
        Total_Rate_Limits: "Rate Limits",
        Total_Turns: "Turns",
        Input_Tokens: "Tokens In",
        Output_Tokens: "Tokens Out",
        Claude_Total_Cost_USD: "Price",
      };
      const headerLabelFn = (column, rows) => {
        if (column !== "Claude_Total_Cost_USD") {
          return displayNames[column] || column;
        }
        const total = rows.reduce((sum, row) => sum + Number(row[column] || 0), 0);
        return `${displayNames[column] || column} (Total: $${total.toFixed(2)})`;
      };
      renderSortableTable("logsData", data, logSort, (column) => {
        if (!column) return;
        if (logSort.column === column) {
          logSort = { column, direction: logSort.direction === "asc" ? "desc" : "asc" };
        } else {
          logSort = { column, direction: "asc" };
        }
        renderLogsTable(data);
      }, visibleColumns, headerLabelFn, (column, value) => {
        if (column === "Claude_Total_Cost_USD") {
          const amount = Number(value || 0);
          return Number.isFinite(amount) ? `$${amount.toFixed(2)}` : value;
        }
        return value;
      }, "auto-table");
    }

    function logColumnStorageKey(column) {
      return `reviewChanges.logColumn.${column}`;
    }

    function isLogColumnVisible(column) {
      try {
        const stored = localStorage.getItem(logColumnStorageKey(column));
        if (stored === "true") return true;
        if (stored === "false") return false;
      } catch {}
      return !defaultHiddenLogColumns.has(column);
    }

    function getVisibleLogColumns(columns) {
      return columns.filter((column) => isLogColumnVisible(column));
    }

    function renderLogColumnToggles(columns) {
      const root = document.getElementById("logsColumns");
      if (!columns.length) {
        root.innerHTML = "";
        return;
      }
      const displayNames = {
        Latest_Run: "Updated",
        Total_Web_Lookups: "Web Search",
        Total_Command_Executions: "Commands",
        Total_Todo_Lists: "Todo Lists",
        Total_Rate_Limits: "Rate Limits",
        Total_Turns: "Turns",
        Input_Tokens: "Tokens In",
        Output_Tokens: "Tokens Out",
        Claude_Total_Cost_USD: "Price",
      };
      root.innerHTML = columns.map((column) => `
        <label><input type="checkbox" data-log-column="${escapeHtml(column)}" ${isLogColumnVisible(column) ? "checked" : ""}> ${escapeHtml(displayNames[column] || column)}</label>
      `).join("");
      root.querySelectorAll("[data-log-column]").forEach((input) => {
        input.addEventListener("change", () => {
          const column = input.dataset.logColumn || "";
          try {
            localStorage.setItem(logColumnStorageKey(column), String(input.checked));
          } catch {}
          renderLogsTable(logsDataCache);
        });
      });
    }

    function normalizeDomain(url) {
      try {
        const hostname = new URL(url).hostname.toLowerCase();
        const domain = hostname.startsWith("www.") ? hostname.slice(4) : hostname;
        if (domain.endsWith(".gov") || domain === "gov") {
          return ".gov";
        }
        return domain;
      } catch {
        return "";
      }
    }

    function polarToCartesian(cx, cy, radius, angleDeg) {
      const angleRad = (angleDeg - 90) * Math.PI / 180;
      return {
        x: cx + radius * Math.cos(angleRad),
        y: cy + radius * Math.sin(angleRad),
      };
    }

    function slicePath(cx, cy, radius, startAngle, endAngle) {
      const start = polarToCartesian(cx, cy, radius, endAngle);
      const end = polarToCartesian(cx, cy, radius, startAngle);
      const largeArc = endAngle - startAngle > 180 ? 1 : 0;
      return `M ${cx} ${cy} L ${start.x} ${start.y} A ${radius} ${radius} 0 ${largeArc} 0 ${end.x} ${end.y} Z`;
    }

    function renderDomainChart(rows) {
      const chartRoot = document.getElementById("domainChart");
      const legendRoot = document.getElementById("domainLegend");
      const counts = new Map();
      for (const row of rows) {
        const domain = normalizeDomain(row.URL || "");
        if (!domain) continue;
        counts.set(domain, (counts.get(domain) || 0) + 1);
      }
      const items = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]);
      if (!items.length) {
        chartRoot.innerHTML = '<div class="empty">No evidence domains to chart.</div>';
        legendRoot.innerHTML = "";
        return;
      }
      const palette = ["#8d4f2d", "#b86b3d", "#cf8e52", "#d4aa5f", "#7f8c53", "#5d7b6f", "#4f6d7a", "#6f5f90", "#a85d7f", "#7a4a3a", "#c36d6d"];
      const top = items.slice(0, 10);
      const remainder = items.slice(10);
      if (remainder.length) {
        top.push(["Other", remainder.reduce((sum, entry) => sum + entry[1], 0)]);
      }
      const total = top.reduce((sum, [, count]) => sum + count, 0);
      const radius = 180;
      const cx = 220;
      const cy = 220;
      let angle = 0;
      const slices = top.map(([domain, count], index) => {
        const sweep = (count / total) * 360;
        const startAngle = angle;
        angle += sweep;
        return {
          domain,
          count,
          color: palette[index % palette.length],
          path: slicePath(cx, cy, radius, startAngle, angle),
        };
      });
      chartRoot.innerHTML = `
        <svg viewBox="0 0 440 440" role="img" aria-label="Evidence domains pie chart">
          ${slices.map((slice) => `<path d="${slice.path}" fill="${slice.color}"></path>`).join("")}
          <circle cx="${cx}" cy="${cy}" r="72" fill="#fffaf0"></circle>
          <text x="${cx}" y="${cy - 8}" text-anchor="middle" font-size="20" font-weight="700" fill="#1f1a17">${total}</text>
          <text x="${cx}" y="${cy + 18}" text-anchor="middle" font-size="13" fill="#6b6158">evidence URLs</text>
        </svg>
      `;
      legendRoot.innerHTML = slices.map((slice) => {
        const percent = ((slice.count / total) * 100).toFixed(1);
        return `
          <div class="legend-row">
            <span class="legend-swatch" style="background:${slice.color}"></span>
            <span class="legend-label">${escapeHtml(slice.domain)}</span>
            <span class="legend-value">${escapeHtml(String(slice.count))} · ${escapeHtml(percent)}%</span>
          </div>
        `;
      }).join("");
    }

    function renderVerdictChart(rows) {
      const chartRoot = document.getElementById("verdictChart");
      const legendRoot = document.getElementById("verdictLegend");
      const counts = new Map();
      for (const row of rows) {
        const verdict = (row.Verdict || "").trim() || "(blank)";
        counts.set(verdict, (counts.get(verdict) || 0) + 1);
      }
      const items = Array.from(counts.entries()).sort((a, b) => b[1] - a[1]);
      if (!items.length) {
        chartRoot.innerHTML = '<div class="empty">No candidate verdicts to chart.</div>';
        legendRoot.innerHTML = "";
        return;
      }
      const paletteByVerdict = {
        nice: "#5d7b6f",
        nuanced: "#d4aa5f",
        no_record: "#8d4f2d",
        naughty: "#a85d7f",
        "(blank)": "#9a9085",
      };
      const palette = ["#5d7b6f", "#d4aa5f", "#8d4f2d", "#a85d7f", "#4f6d7a", "#9a9085"];
      const total = items.reduce((sum, [, count]) => sum + count, 0);
      const radius = 180;
      const cx = 220;
      const cy = 220;
      let angle = 0;
      const slices = items.map(([verdict, count], index) => {
        const sweep = (count / total) * 360;
        const startAngle = angle;
        angle += sweep;
        return {
          verdict,
          count,
          color: paletteByVerdict[verdict] || palette[index % palette.length],
          path: slicePath(cx, cy, radius, startAngle, angle),
        };
      });
      chartRoot.innerHTML = `
        <svg viewBox="0 0 440 440" role="img" aria-label="Candidate verdicts pie chart">
          ${slices.map((slice) => `<path d="${slice.path}" fill="${slice.color}"></path>`).join("")}
          <circle cx="${cx}" cy="${cy}" r="72" fill="#fffaf0"></circle>
          <text x="${cx}" y="${cy - 8}" text-anchor="middle" font-size="20" font-weight="700" fill="#1f1a17">${total}</text>
          <text x="${cx}" y="${cy + 18}" text-anchor="middle" font-size="13" fill="#6b6158">candidates</text>
        </svg>
      `;
      legendRoot.innerHTML = slices.map((slice) => {
        const percent = ((slice.count / total) * 100).toFixed(1);
        return `
          <div class="legend-row">
            <span class="legend-swatch" style="background:${slice.color}"></span>
            <span class="legend-label">${escapeHtml(slice.verdict)}</span>
            <span class="legend-value">${escapeHtml(String(slice.count))} · ${escapeHtml(percent)}%</span>
          </div>
        `;
      }).join("");
    }

    function renderChangeCoverageChart(candidateRows, evidenceRows, changeRows) {
      const chartRoot = document.getElementById("coverageChart");
      const legendRoot = document.getElementById("coverageLegend");
      const allCandidates = candidateRows
        .map((row) => ({
          candidate: (row.Candidate || "").trim(),
          verdict: (row.Verdict || "").trim() || "no_record",
        }))
        .filter((row) => row.candidate);
      if (!allCandidates.length) {
        chartRoot.innerHTML = '<div class="empty">No candidates to chart.</div>';
        legendRoot.innerHTML = "";
        return;
      }
      const evidenceToCandidate = new Map();
      for (const row of evidenceRows) {
        const evId = (row.Evidence_ID || "").trim();
        const candidate = (row.Candidate || "").trim();
        if (evId && candidate) {
          evidenceToCandidate.set(evId, candidate);
        }
      }
      const coveredCandidates = new Set();
      for (const row of changeRows) {
        const table = (row.table || "").trim();
        const key = (row.key || "").trim();
        if (table === "candidates" && key) {
          coveredCandidates.add(key);
        } else if (table === "evidence") {
          if (key && evidenceToCandidate.has(key)) {
            coveredCandidates.add(evidenceToCandidate.get(key));
          } else if ((row.field || "").trim() === "Candidate" && (row.value || "").trim()) {
            coveredCandidates.add((row.value || "").trim());
          }
        }
      }
      const combos = new Map([
        ["0-0", { label: "no_record + no proposed changes", count: 0, color: "rgb(0,0,0)" }],
        ["0-255", { label: "no_record + proposed changes", count: 0, color: "rgb(0,255,0)" }],
        ["255-0", { label: "reviewed verdict + no proposed changes", count: 0, color: "rgb(255,0,0)" }],
        ["255-255", { label: "reviewed verdict + proposed changes", count: 0, color: "rgb(255,255,0)" }],
      ]);
      for (const row of allCandidates) {
        const reviewed = row.verdict !== "no_record";
        const hasChanges = coveredCandidates.has(row.candidate);
        const key = `${reviewed ? 255 : 0}-${hasChanges ? 255 : 0}`;
        combos.get(key).count += 1;
      }
      const items = Array.from(combos.values()).filter((item) => item.count > 0);
      const total = items.reduce((sum, item) => sum + item.count, 0);
      const radius = 180;
      const cx = 220;
      const cy = 220;
      let angle = 0;
      const slices = items.map((item) => {
        const sweep = (item.count / total) * 360;
        const startAngle = angle;
        angle += sweep;
        return {
          ...item,
          path: slicePath(cx, cy, radius, startAngle, angle),
        };
      });
      chartRoot.innerHTML = `
        <svg viewBox="0 0 440 440" role="img" aria-label="Candidate verdict and proposed-change coverage pie chart">
          ${slices.map((slice) => `<path d="${slice.path}" fill="${slice.color}"></path>`).join("")}
          <circle cx="${cx}" cy="${cy}" r="72" fill="#fffaf0"></circle>
          <text x="${cx}" y="${cy - 8}" text-anchor="middle" font-size="20" font-weight="700" fill="#1f1a17">${total}</text>
          <text x="${cx}" y="${cy + 18}" text-anchor="middle" font-size="13" fill="#6b6158">candidates</text>
        </svg>
      `;
      legendRoot.innerHTML = slices.map((slice) => {
        const percent = ((slice.count / total) * 100).toFixed(1);
        return `
          <div class="legend-row">
            <span class="legend-swatch" style="background:${slice.color}"></span>
            <span class="legend-label">${escapeHtml(slice.label)}</span>
            <span class="legend-value">${escapeHtml(String(slice.count))} · ${escapeHtml(percent)}%</span>
          </div>
        `;
      }).join("");
    }

    function wikipediaEntries(rows) {
      const grouped = new Map();
      for (const row of rows) {
        const url = row.URL || "";
        if (!url.toLowerCase().includes("wikipedia.org/wiki/")) continue;
        const key = url.trim();
        if (!grouped.has(key)) {
          grouped.set(key, new Set());
        }
        const desc = (row.Source_Description || "").trim();
        if (desc) {
          grouped.get(key).add(desc);
        }
      }
      return Array.from(grouped.entries())
        .map(([url, descriptions]) => ({
          url,
          descriptions: Array.from(descriptions).sort(),
        }))
        .sort((a, b) => a.url.localeCompare(b.url));
    }

    function renderWikipediaList(rows) {
      const root = document.getElementById("wikiList");
      const entries = wikipediaEntries(rows);
      if (!entries.length) {
        root.innerHTML = '<div class="empty">No Wikipedia links found in evidence.csv.</div>';
        return;
      }
      root.innerHTML = entries.map((entry, index) => `
        <div class="wiki-item">
          <label>
            <input type="checkbox" class="wiki-exclusion" data-url="${escapeHtml(entry.url)}" ${index === 0 ? "" : ""}>
            <div class="wiki-main">
              <div class="wiki-url">${escapeHtml(entry.url)}</div>
              <div class="wiki-descs">${escapeHtml(entry.descriptions.join(" | ") || "(no description)")}</div>
            </div>
          </label>
        </div>
      `).join("");
    }

    function populateProviders() {
      const select = document.getElementById("provider");
      let savedProvider = "";
      try {
        savedProvider = localStorage.getItem("reviewChanges.selectedProvider") || "";
      } catch {}
      const currentProvider = select.value;
      select.innerHTML = meta.providers.map((provider) => `
        <option value="${escapeHtml(provider.name)}">${escapeHtml(provider.label)}</option>
      `).join("");
      if (savedProvider && meta.providers.some((provider) => provider.name === savedProvider)) {
        select.value = savedProvider;
      } else if (currentProvider && meta.providers.some((provider) => provider.name === currentProvider)) {
        select.value = currentProvider;
      }
    }

    function filteredRaces() {
      const query = document.getElementById("raceSearch").value.trim().toLowerCase();
      if (!query) return meta.races;
      const parts = query.split(/\\s+/).filter(Boolean);
      return meta.races.filter((race) => {
        const haystack = `${race.key} ${race.label}`.toLowerCase();
        return parts.every((part) => haystack.includes(part));
      });
    }

    function populateRaces() {
      const select = document.getElementById("race");
      const races = filteredRaces();
      let savedRace = "";
      try {
        savedRace = localStorage.getItem("reviewChanges.selectedRace") || "";
      } catch {}
      const currentRace = select.value;
      select.innerHTML = races.map((race) => `
        <option value="${escapeHtml(race.key)}">${escapeHtml(race.label)} (${escapeHtml(String((meta.candidates_by_race[race.key] || []).length))})</option>
      `).join("");
      if (!races.length) {
        select.innerHTML = '<option value="">No matching races</option>';
      } else if (savedRace && races.some((race) => race.key === savedRace)) {
        select.value = savedRace;
      } else if (currentRace && races.some((race) => race.key === currentRace)) {
        select.value = currentRace;
      }
      populateCandidates();
    }

    function populateCandidates() {
      const raceKey = document.getElementById("race").value;
      const select = document.getElementById("candidate");
      const candidates = meta.candidates_by_race[raceKey] || [];
      let savedCandidate = "";
      try {
        savedCandidate = localStorage.getItem("reviewChanges.selectedCandidate") || "";
      } catch {}
      const currentCandidate = select.value;
      let html = '<option value="">All candidates</option>';
      html += candidates.map((candidate) => `<option value="${escapeHtml(candidate)}">${escapeHtml(candidate)}</option>`).join("");
      select.innerHTML = html;
      if (savedCandidate && candidates.includes(savedCandidate)) {
        select.value = savedCandidate;
      } else if (currentCandidate && candidates.includes(currentCandidate)) {
        select.value = currentCandidate;
      }
    }

    async function fetchMeta() {
      const res = await fetch("/api/meta");
      meta = await res.json();
      populateProviders();
      populateRaces();
      document.getElementById("promptTemplate").value = meta.default_prompt_template || "";
      try {
        document.getElementById("candidateVerdict").value = localStorage.getItem("reviewChanges.candidateVerdict") || "";
        document.getElementById("skipCandidatesWithChanges").checked = localStorage.getItem("reviewChanges.skipCandidatesWithChanges") === "true";
      } catch {}
    }

    async function fetchChanges() {
      const pendingOnly = document.getElementById("pendingOnly").checked;
      const excludeNoRecord = document.getElementById("excludeNoRecord").checked;
      const verdictChange = document.getElementById("verdictChange").checked;
      const res = await fetch(`/api/changes?pending_only=${pendingOnly ? "1" : "0"}&exclude_no_record=${excludeNoRecord ? "1" : "0"}&verdict_change=${verdictChange ? "1" : "0"}`);
      const data = await res.json();
      renderChanges(data.groups || []);
    }

    async function fetchJobs() {
      const res = await fetch("/api/jobs");
      const data = await res.json();
      renderJobs(data.jobs || []);
    }

    async function fetchInspectData() {
      const table = document.getElementById("inspectTable").value;
      const filter = document.getElementById("inspectFilter").value.trim();
      const params = new URLSearchParams({ table });
      if (filter) params.set("filter", filter);
      const res = await fetch(`/api/data?${params.toString()}`);
      const data = await res.json();
      renderInspectTable(data);
    }

    async function fetchLogsData() {
      const filter = document.getElementById("logsFilter").value.trim();
      const params = new URLSearchParams();
      if (filter) params.set("filter", filter);
      const suffix = params.toString() ? `?${params.toString()}` : "";
      const res = await fetch(`/api/logs${suffix}`);
      const data = await res.json();
      logsDataCache = data;
      renderLogColumnToggles(data.columns || []);
      renderLogsTable(data);
    }

    async function reloadLogsData() {
      setStatus("Rebuilding log summary…", "info");
      const res = await fetch("/api/logs/reload", { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to rebuild log summary.", "error");
        return;
      }
      setStatus(data.message || "Rebuilt log summary.", "ok");
      await fetchLogsData();
    }

    async function fetchDomainChart() {
      const [evidenceRes, candidatesRes, changesRes] = await Promise.all([
        fetch("/api/data?table=evidence"),
        fetch("/api/data?table=candidates"),
        fetch("/api/data?table=changes"),
      ]);
      const evidenceData = await evidenceRes.json();
      const candidateData = await candidatesRes.json();
      const changesData = await changesRes.json();
      renderDomainChart(evidenceData.rows || []);
      renderWikipediaList(evidenceData.rows || []);
      renderVerdictChart(candidateData.rows || []);
      renderChangeCoverageChart(candidateData.rows || [], evidenceData.rows || [], changesData.rows || []);
    }

    async function exportWikipediaExclusions() {
      const items = Array.from(document.querySelectorAll(".wiki-exclusion:checked"))
        .map((input) => input.dataset.url || "");
      const res = await fetch("/api/export-wikipedia-exclusions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ urls: items })
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to export Wikipedia exclusions.", "error");
        return;
      }
      setStatus(`Exported ${data.count} Wikipedia exclusions to ${data.path}`, "ok");
    }

    async function updateStatus(changeId, status) {
      setStatus("Saving…", "info");
      const res = await fetch("/api/status", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ change_id: changeId, status })
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Save failed.", "error");
        return;
      }
      setStatus(`Updated ${changeId} -> ${status}`, "ok");
      await fetchChanges();
    }

    async function startRun() {
      setStatus("Starting job…", "info");
      const payload = {
        provider: document.getElementById("provider").value,
        race: document.getElementById("race").value,
        candidate: document.getElementById("candidate").value,
        candidate_verdict: document.getElementById("candidateVerdict").value,
        skip_candidates_with_changes: document.getElementById("skipCandidatesWithChanges").checked,
        prompt_template: document.getElementById("promptTemplate").value
      };
      const res = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to start job.", "error");
        return;
      }
      setStatus(`Started ${data.job.provider} job ${data.job.id}`, "ok");
      await fetchJobs();
    }

    async function startAllRacesRun() {
      setStatus("Starting all-races job…", "info");
      const payload = {
        provider: document.getElementById("provider").value,
        race: "",
        candidate: "",
        candidate_verdict: document.getElementById("candidateVerdict").value,
        skip_candidates_with_changes: document.getElementById("skipCandidatesWithChanges").checked,
        prompt_template: document.getElementById("promptTemplate").value,
        all_races: true
      };
      const res = await fetch("/api/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to start all-races job.", "error");
        return;
      }
      setStatus(`Started ${data.job.provider} all-races job ${data.job.id}`, "ok");
      await fetchJobs();
    }

    async function applyApproved() {
      setStatus("Starting apply job…", "info");
      const res = await fetch("/api/apply", { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to apply changes.", "error");
        return;
      }
      setStatus(`Started apply job ${data.job.id}`, "ok");
      await fetchJobs();
    }

    async function requestJobStop(jobId) {
      setStatus("Requesting graceful stop…", "info");
      const res = await fetch("/api/stop-job", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: jobId })
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to request stop.", "error");
        return;
      }
      setStatus(`Stop requested for job ${jobId}`, "ok");
      await fetchJobs();
    }

    async function clearFinishedJobs() {
      setStatus("Clearing finished jobs…", "info");
      const res = await fetch("/api/jobs/clear-finished", { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to clear finished jobs.", "error");
        return;
      }
      setStatus(`Cleared ${data.cleared} finished jobs`, "ok");
      await fetchJobs();
    }

    async function refreshAll() {
      await Promise.all([fetchChanges(), fetchJobs()]);
    }

    function switchTab(tabName) {
      const names = ["run", "review", "inspect", "logs"];
      for (const name of names) {
        const panel = document.getElementById(`tab-${name}`);
        const button = document.querySelector(`[data-tab="${name}"]`);
        const active = name === tabName;
        panel.hidden = !active;
        button.classList.toggle("active", active);
      }
      try {
        localStorage.setItem("reviewChanges.activeTab", tabName);
      } catch {}
      if (tabName === "review") {
        fetchChanges().catch(console.error);
      } else if (tabName === "inspect") {
        fetchDomainChart().catch(console.error);
        fetchInspectData().catch(console.error);
      } else if (tabName === "logs") {
        fetchLogsData().catch(console.error);
      }
    }

    document.getElementById("provider").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.selectedProvider", document.getElementById("provider").value);
      } catch {}
    });
    document.getElementById("race").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.selectedRace", document.getElementById("race").value);
      } catch {}
      populateCandidates();
    });
    document.getElementById("candidate").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.selectedCandidate", document.getElementById("candidate").value);
      } catch {}
    });
    document.getElementById("candidateVerdict").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.candidateVerdict", document.getElementById("candidateVerdict").value);
      } catch {}
    });
    document.getElementById("skipCandidatesWithChanges").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.skipCandidatesWithChanges", String(document.getElementById("skipCandidatesWithChanges").checked));
      } catch {}
    });
    document.getElementById("raceSearch").addEventListener("input", populateRaces);
    document.getElementById("pendingOnly").addEventListener("change", fetchChanges);
    document.getElementById("excludeNoRecord").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.excludeNoRecord", String(document.getElementById("excludeNoRecord").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("verdictChange").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.verdictChange", String(document.getElementById("verdictChange").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("reloadBtn").addEventListener("click", refreshAll);
    document.getElementById("runBtn").addEventListener("click", startRun);
    document.getElementById("runAllBtn").addEventListener("click", startAllRacesRun);
    document.getElementById("clearFinishedBtn").addEventListener("click", clearFinishedJobs);
    document.getElementById("applyBtn").addEventListener("click", applyApproved);
    document.getElementById("inspectReloadBtn").addEventListener("click", fetchInspectData);
    document.getElementById("inspectTable").addEventListener("change", fetchInspectData);
    document.getElementById("inspectFilter").addEventListener("input", fetchInspectData);
    document.getElementById("logsReloadBtn").addEventListener("click", reloadLogsData);
    document.getElementById("logsFilter").addEventListener("input", fetchLogsData);
    document.getElementById("exportWikiExclusionsBtn").addEventListener("click", exportWikipediaExclusions);
    document.querySelectorAll("[data-tab]").forEach((button) => {
      button.addEventListener("click", () => switchTab(button.dataset.tab));
    });

    async function init() {
      await fetchMeta();
      await refreshAll();
      await fetchDomainChart();
      let initialTab = "run";
      try {
        const savedTab = localStorage.getItem("reviewChanges.activeTab");
        if (savedTab && ["run", "review", "inspect", "logs"].includes(savedTab)) {
          initialTab = savedTab;
        }
        document.getElementById("excludeNoRecord").checked = localStorage.getItem("reviewChanges.excludeNoRecord") === "true";
        document.getElementById("verdictChange").checked = localStorage.getItem("reviewChanges.verdictChange") === "true";
      } catch {}
      switchTab(initialTab);
      setInterval(fetchJobs, %POLL_SECONDS%);
    }

    init().catch((err) => {
      setStatus("Failed to load UI data.", "error");
      console.error(err);
    });
  </script>
</body>
</html>
""".replace("%POLL_SECONDS%", str(POLL_SECONDS * 1000))


def ensure_changes_csv() -> None:
    if CHANGES_CSV.exists():
        return
    with CHANGES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_changes() -> list[dict[str, str]]:
    ensure_changes_csv()
    rows = load_csv(CHANGES_CSV)
    out = []
    for row in rows:
        normalized = {field: row.get(field, "").strip() for field in FIELDNAMES}
        if not normalized["status"]:
            normalized["status"] = "pending"
        out.append(normalized)
    return out


def load_data_table(name: str, filter_text: str) -> dict[str, object]:
    table_map = {
        "races": RACES_CSV,
        "candidates": CANDIDATES_CSV,
        "evidence": EVIDENCE_CSV,
        "changes": CHANGES_CSV,
    }
    path = table_map.get(name)
    if path is None:
        raise KeyError(name)
    rows = load_csv(path)
    if filter_text:
        needle = filter_text.lower()
        rows = [
            row for row in rows
            if any(needle in str(value).lower() for value in row.values())
        ]
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": rows}


def load_logs_table(filter_text: str) -> dict[str, object]:
    if not RACE_RUNNER_LOG_SUMMARY_CSV.exists():
        return {"columns": [], "rows": []}
    rows = load_csv(RACE_RUNNER_LOG_SUMMARY_CSV)
    if filter_text:
        needle = filter_text.lower()
        rows = [
            row for row in rows
            if any(needle in str(value).lower() for value in row.values())
        ]
    columns = list(rows[0].keys()) if rows else []
    return {"columns": columns, "rows": rows}


def rebuild_logs_table() -> str:
    proc = subprocess.run(
        [sys.executable, str(SUMMARIZE_RACE_RUNNER_LOGS)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "Failed to rebuild log summary")
    return proc.stdout.strip() or "Rebuilt log summary"


def wikipedia_exclusion_rows(urls: list[str]) -> list[dict[str, str]]:
    selected = {url.strip() for url in urls if url.strip()}
    if not selected:
        return []
    evidence_rows = load_csv(EVIDENCE_CSV)
    grouped: dict[str, set[str]] = {}
    for row in evidence_rows:
        url = row.get("URL", "").strip()
        if url not in selected:
            continue
        grouped.setdefault(url, set())
        desc = row.get("Source_Description", "").strip()
        if desc:
            grouped[url].add(desc)
    out: list[dict[str, str]] = []
    for url in sorted(grouped):
        descriptions = " | ".join(sorted(grouped[url]))
        out.append({"URL": url, "Source_Description": descriptions})
    return out


def write_wikipedia_exclusions(urls: list[str]) -> int:
    rows = wikipedia_exclusion_rows(urls)
    with WIKIPEDIA_EXCLUSIONS_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["URL", "Source_Description"])
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def write_changes(rows: list[dict[str, str]]) -> None:
    with CHANGES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def set_change_status(change_id: str, status: str) -> int:
    if status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status}")
    rows = load_changes()
    count = 0
    for row in rows:
        if row["change_id"] == change_id:
            row["status"] = status
            count += 1
    if count == 0:
        raise KeyError(change_id)
    write_changes(rows)
    return count


def resolve_cli(name: str) -> str | None:
    path = shutil.which(name)
    if path:
        return path
    candidate = Path.home() / ".npm-global" / "bin" / name
    if candidate.exists():
        return str(candidate)
    return None


def available_providers() -> list[dict[str, str]]:
    providers: list[dict[str, str]] = []
    if resolve_cli("claude"):
        providers.append({"name": "claude", "label": "Claude"})
    if resolve_cli("codex"):
        providers.append({"name": "codex", "label": "Codex"})
    if resolve_cli("gemini"):
        providers.append({"name": "gemini", "label": "Gemini"})
    return providers


def races_payload() -> tuple[list[dict[str, str]], dict[str, list[str]]]:
    race_rows = load_csv(RACES_CSV)
    candidate_rows = load_csv(CANDIDATES_CSV)
    candidates_by_race: dict[str, list[str]] = {}
    for race in race_rows:
        key = f'{race.get("State", "").strip()}|{race.get("Office", "").strip()}'
        candidates = [
            row.get("Candidate", "").strip()
            for row in candidate_rows
            if row.get("State", "").strip() == race.get("State", "").strip()
            and row.get("Office", "").strip() == race.get("Office", "").strip()
        ]
        candidates_by_race[key] = [candidate for candidate in candidates if candidate]
    races = [
        {
            "key": f'{race.get("State", "").strip()}|{race.get("Office", "").strip()}',
            "label": f'{race.get("Priority", "").strip()}. {race.get("State", "").strip()} | {race.get("Office", "").strip()}',
        }
        for race in race_rows
    ]
    return races, candidates_by_race


def parse_race_key(key: str) -> tuple[str, str]:
    state, sep, office = key.partition("|")
    if not sep:
        return "", ""
    return state.strip(), office.strip()


def build_review_groups(rows: list[dict[str, str]]) -> list[dict[str, object]]:
    candidate_rows = load_csv(CANDIDATES_CSV)
    evidence_rows = load_csv(EVIDENCE_CSV)
    candidate_lookup = {
        row.get("Candidate", "").strip(): {
            "state": row.get("State", "").strip(),
            "office": row.get("Office", "").strip(),
            "row": dict(row),
        }
        for row in candidate_rows
    }
    evidence_lookup = {
        row.get("Evidence_ID", "").strip(): {
            "candidate": row.get("Candidate", "").strip(),
            "description": row.get("Source_Description", "").strip(),
            "url": row.get("URL", "").strip(),
            "row": dict(row),
        }
        for row in evidence_rows
    }

    grouped: dict[str, list[dict[str, str]]] = {}
    order: list[str] = []
    for row in rows:
        change_id = row.get("change_id", "").strip()
        if change_id not in grouped:
            order.append(change_id)
            grouped[change_id] = []
        grouped[change_id].append(row)

    out: list[dict[str, object]] = []
    for change_id in order:
        group = grouped[change_id]
        first = group[0]
        table = first.get("table", "").strip()
        key = first.get("key", "").strip()
        state = ""
        office = ""
        candidate = ""
        evidence_label = ""
        current_row: dict[str, str] = {}

        if table == "races":
            state, office = parse_race_key(key)
            evidence_label = "(race change)"
        elif table == "candidates":
            candidate = key
            candidate_meta = candidate_lookup.get(candidate, {})
            state = str(candidate_meta.get("state", "")).strip()
            office = str(candidate_meta.get("office", "")).strip()
            evidence_label = "(candidate change)"
            current_row = dict(candidate_meta.get("row", {}))
        elif table == "evidence":
            if key and key in evidence_lookup:
                evidence_meta = evidence_lookup[key]
                candidate = str(evidence_meta.get("candidate", "")).strip()
                desc = str(evidence_meta.get("description", "")).strip()
                evidence_label = f'{key}: {desc}' if desc else f"Evidence {key}"
                current_row = dict(evidence_meta.get("row", {}))
            else:
                candidate = next(
                    (
                        row.get("value", "").strip()
                        for row in group
                        if row.get("field", "").strip() == "Candidate" and row.get("value", "").strip()
                    ),
                    "",
                )
                evidence_label = "New evidence"
            candidate_meta = candidate_lookup.get(candidate, {})
            state = str(candidate_meta.get("state", "")).strip()
            office = str(candidate_meta.get("office", "")).strip()

        race = office or "(unknown race)"
        if not state:
            state = "(unknown state)"
        if not candidate:
            candidate = "(no candidate)"
        if not evidence_label:
            evidence_label = "(row change)"

        out.append(
            {
                "change_id": change_id,
                "table": table,
                "key": key,
                "status": first.get("status", "").strip(),
                "reasoning": first.get("reasoning", "").strip(),
                "state": state,
                "race": race,
                "candidate": candidate,
                "evidence": evidence_label,
                "current_row": current_row,
                "rows": group,
            }
        )
    return out


def filter_review_groups(groups: list[dict[str, object]], exclude_no_record: bool, verdict_change: bool) -> list[dict[str, object]]:
    if not exclude_no_record and not verdict_change:
        return groups
    candidate_rows = load_csv(CANDIDATES_CSV)
    verdict_by_candidate = {
        row.get("Candidate", "").strip(): row.get("Verdict", "").strip()
        for row in candidate_rows
    }
    included_candidates: set[str] = set()
    for group in groups:
        candidate = str(group.get("candidate", "")).strip()
        if not candidate or candidate == "(no candidate)":
            continue
        current_verdict = verdict_by_candidate.get(candidate, "")
        if exclude_no_record and current_verdict in {"nice", "nuanced", "naughty"}:
            included_candidates.add(candidate)
            continue
        if verdict_change and str(group.get("table", "")).strip() == "candidates":
            for row in group.get("rows", []):
                if row.get("field", "").strip() == "Verdict":
                    included_candidates.add(candidate)
                    break
    return [group for group in groups if str(group.get("candidate", "")).strip() in included_candidates]


@dataclass
class Job:
    id: str
    kind: str
    provider: str
    race: str
    candidate: str
    command: list[str]
    started_at: str
    status: str = "running"
    log: str = ""
    returncode: int | None = None
    stop_file: str = ""
    stop_requested: bool = False
    process: subprocess.Popen[str] | None = field(default=None, repr=False)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def append(self, text: str) -> None:
        if not text:
            return
        with self.lock:
            self.log += text
            if len(self.log) > MAX_LOG_CHARS:
                self.log = self.log[-MAX_LOG_CHARS:]

    def snapshot(self) -> dict[str, str | int | None]:
        with self.lock:
            return {
                "id": self.id,
                "kind": self.kind,
                "provider": self.provider,
                "race": self.race,
                "candidate": self.candidate,
                "started_at": self.started_at,
                "status": self.status,
                "log": self.log,
                "returncode": self.returncode,
                "stop_requested": self.stop_requested,
                "can_stop": self.kind == "run" and self.status == "running",
            }


class JobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()

    def list_jobs(self) -> list[dict[str, str | int | None]]:
        with self._lock:
            jobs = list(self._jobs.values())
        jobs.sort(key=lambda job: job.started_at, reverse=True)
        return [job.snapshot() for job in jobs]

    def start(self, kind: str, provider: str, race: str, candidate: str, command: list[str]) -> dict[str, str | int | None]:
        stop_file = ""
        if kind == "run":
            fd, stop_path = tempfile.mkstemp(prefix="race-runner-stop-", suffix=".flag")
            os.close(fd)
            Path(stop_path).unlink(missing_ok=True)
            stop_file = stop_path
            command = command + ["--stop-file", stop_file]
        job = Job(
            id=uuid.uuid4().hex[:8],
            kind=kind,
            provider=provider,
            race=race,
            candidate=candidate,
            command=command,
            started_at=time.strftime("%Y-%m-%d %H:%M:%S"),
            stop_file=stop_file,
        )
        proc = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        job.process = proc
        with self._lock:
            self._jobs[job.id] = job
        thread = threading.Thread(target=self._watch_job, args=(job,), daemon=True)
        thread.start()
        return job.snapshot()

    def request_stop(self, job_id: str) -> dict[str, str | int | None]:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if job.kind != "run" or job.status != "running":
            raise ValueError("Job cannot be stopped")
        if not job.stop_file:
            raise ValueError("Job has no stop marker")
        Path(job.stop_file).write_text("stop\n", encoding="utf-8")
        with job.lock:
            job.stop_requested = True
            job.log += "\n[ui] Graceful stop requested. The runner will stop after the current candidate.\n"
            if len(job.log) > MAX_LOG_CHARS:
                job.log = job.log[-MAX_LOG_CHARS:]
        return job.snapshot()

    def clear_finished(self) -> int:
        with self._lock:
            finished_ids = [
                job_id
                for job_id, job in self._jobs.items()
                if job.status in {"succeeded", "failed"}
            ]
            for job_id in finished_ids:
                self._jobs.pop(job_id, None)
        return len(finished_ids)

    def _watch_job(self, job: Job) -> None:
        assert job.process is not None
        assert job.process.stdout is not None
        for line in job.process.stdout:
            job.append(line)
        returncode = job.process.wait()
        with job.lock:
            job.returncode = returncode
            job.status = "succeeded" if returncode == 0 else "failed"
        if job.stop_file:
            Path(job.stop_file).unlink(missing_ok=True)


JOB_MANAGER = JobManager()


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._send_html(HTML)
            return
        if parsed.path == "/api/meta":
            races, candidates_by_race = races_payload()
            self._send_json({
                "providers": available_providers(),
                "races": races,
                "candidates_by_race": candidates_by_race,
                "default_prompt_template": load_race_runner_prompt_template(),
            })
            return
        if parsed.path == "/api/changes":
            params = parse_qs(parsed.query)
            pending_only = params.get("pending_only", ["0"])[0] == "1"
            exclude_no_record = params.get("exclude_no_record", ["0"])[0] == "1"
            verdict_change = params.get("verdict_change", ["0"])[0] == "1"
            rows = load_changes()
            if pending_only:
                rows = [row for row in rows if row["status"] == "pending"]
            groups = build_review_groups(rows)
            groups = filter_review_groups(groups, exclude_no_record, verdict_change)
            self._send_json({"groups": groups})
            return
        if parsed.path == "/api/jobs":
            self._send_json({"jobs": JOB_MANAGER.list_jobs()})
            return
        if parsed.path == "/api/data":
            params = parse_qs(parsed.query)
            table = params.get("table", ["races"])[0].strip()
            filter_text = params.get("filter", [""])[0].strip()
            try:
                payload = load_data_table(table, filter_text)
            except KeyError:
                self._send_json({"error": f"Unknown table: {table}"}, HTTPStatus.BAD_REQUEST)
                return
            self._send_json(payload)
            return
        if parsed.path == "/api/logs":
            params = parse_qs(parsed.query)
            filter_text = params.get("filter", [""])[0].strip()
            self._send_json(load_logs_table(filter_text))
            return
        self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")

        if self.path == "/api/status":
            change_id = str(payload.get("change_id", "")).strip()
            status = str(payload.get("status", "")).strip()
            if not change_id or not status:
                self._send_json({"error": "Missing change_id or status."}, HTTPStatus.BAD_REQUEST)
                return
            try:
                updated = set_change_status(change_id, status)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return
            except KeyError:
                self._send_json({"error": f"Unknown change_id: {change_id}"}, HTTPStatus.NOT_FOUND)
                return
            self._send_json({"ok": True, "updated": updated})
            return

        if self.path == "/api/run":
            provider = str(payload.get("provider", "")).strip() or "claude"
            race = str(payload.get("race", "")).strip()
            candidate = str(payload.get("candidate", "")).strip()
            candidate_verdict = str(payload.get("candidate_verdict", "")).strip()
            skip_candidates_with_changes = bool(payload.get("skip_candidates_with_changes", False))
            model = str(payload.get("model", "")).strip()
            prompt_template = str(payload.get("prompt_template", "")).strip()
            all_races = bool(payload.get("all_races", False))
            if not all_races and not race:
                self._send_json({"error": "Missing race."}, HTTPStatus.BAD_REQUEST)
                return
            if provider not in {item["name"] for item in available_providers()}:
                self._send_json({"error": f"Provider unavailable: {provider}"}, HTTPStatus.BAD_REQUEST)
                return
            command = [sys.executable, str(RACE_RUNNER), "--provider", provider]
            if all_races:
                command.extend(["--max-races", "0"])
            else:
                command.extend(["--race", race])
            if candidate:
                command.extend(["--candidate", candidate])
            if candidate_verdict:
                command.extend(["--candidate-verdict", candidate_verdict])
            if skip_candidates_with_changes:
                command.append("--skip-candidates-with-changes")
            if model:
                command.extend(["--model", model])
            if prompt_template:
                command.extend(["--prompt-template", prompt_template])
            job_race = race or "ALL RACES"
            job_candidate = candidate or (f"verdict={candidate_verdict}" if candidate_verdict else "")
            job = JOB_MANAGER.start(kind="run", provider=provider, race=job_race, candidate=job_candidate, command=command)
            self._send_json({"ok": True, "job": job})
            return

        if self.path == "/api/apply":
            command = [sys.executable, str(APPLY_CHANGES)]
            job = JOB_MANAGER.start(kind="apply", provider="system", race="", candidate="", command=command)
            self._send_json({"ok": True, "job": job})
            return

        if self.path == "/api/stop-job":
            job_id = str(payload.get("job_id", "")).strip()
            if not job_id:
                self._send_json({"error": "Missing job_id."}, HTTPStatus.BAD_REQUEST)
                return
            try:
                job = JOB_MANAGER.request_stop(job_id)
            except KeyError:
                self._send_json({"error": f"Unknown job_id: {job_id}"}, HTTPStatus.NOT_FOUND)
                return
            except ValueError as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return
            self._send_json({"ok": True, "job": job})
            return

        if self.path == "/api/jobs/clear-finished":
            cleared = JOB_MANAGER.clear_finished()
            self._send_json({"ok": True, "cleared": cleared})
            return

        if self.path == "/api/export-wikipedia-exclusions":
            urls = payload.get("urls", [])
            if not isinstance(urls, list):
                self._send_json({"error": "urls must be a list."}, HTTPStatus.BAD_REQUEST)
                return
            count = write_wikipedia_exclusions([str(url) for url in urls])
            self._send_json({"ok": True, "count": count, "path": str(WIKIPEDIA_EXCLUSIONS_CSV)})
            return

        if self.path == "/api/logs/reload":
            try:
                message = rebuild_logs_table()
            except RuntimeError as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)
                return
            self._send_json({"ok": True, "message": message})
            return

        self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def main() -> None:
    ensure_changes_csv()
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

    print(f"Serving change review UI at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
