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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.changes_file import DEFAULT_CHANGES_FILENAME, available_changes_filenames, resolve_changes_csv
from scripts.verdict_legend import inject_verdict_legend

CHANGES_CSV = resolve_changes_csv()
CANDIDATES_CSV = ROOT / "data" / "candidates.csv"
EVIDENCE_CSV = ROOT / "data" / "evidence.csv"
RACES_CSV = ROOT / "data" / "races.csv"
REPORTS_DIR = ROOT / "reports"
RACE_RUNNER = ROOT / "scripts" / "race_runner.py"
VERDICT_REVIEW = ROOT / "scripts" / "verdict_review.py"
RACE_RUNNER_PROMPT = ROOT / "scripts" / "race_runner_prompt.txt"
APPLY_CHANGES = ROOT / "scripts" / "apply_changes.py"
SUMMARIZE_RACE_RUNNER_LOGS = ROOT / "scripts" / "summarize_race_runner_logs.py"
WIKIPEDIA_EXCLUSIONS_CSV = REPORTS_DIR / "wikipedia_description_exclusions.csv"
RACE_RUNNER_LOG_SUMMARY_CSV = REPORTS_DIR / "race_runner_log_summary.csv"
HOST = "127.0.0.1"
PORT = 8767
FIELDNAMES = ["change_id", "table", "key", "action", "reasoning", "Model", "field", "value", "D", "Reasoning D", "I", "Reasoning I"]
REVIEW_COLUMNS = ("D", "I")
VALID_STATUSES = {"pending", "approved", "denied", "applied", "conflict"}
POLL_SECONDS = 1.5
MAX_LOG_CHARS = 50000


def load_race_runner_prompt_template() -> str:
    return inject_verdict_legend(RACE_RUNNER_PROMPT.read_text(encoding="utf-8"))


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
    .navbar {
      display: flex;
      gap: 12px;
      margin: 0 0 18px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 16px;
      background: rgba(255,250,240,0.84);
      box-shadow: 0 10px 28px rgba(40, 28, 17, 0.05);
      position: sticky;
      top: 0;
      z-index: 10;
      backdrop-filter: blur(6px);
    }
    .nav-tabs {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    .nav-controls {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
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
    .grid {
      display: grid;
      grid-template-columns: minmax(360px, 1fr) minmax(720px, 2fr);
      gap: 18px;
      align-items: start;
    }
    .run-stack {
      display: grid;
      gap: 18px;
      align-items: start;
    }
    .verify-stack {
      display: grid;
      gap: 18px;
      align-items: start;
    }
    .run-controls-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      align-items: end;
      margin-bottom: 14px;
    }
    .verify-controls-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      align-items: end;
      margin-bottom: 14px;
    }
    .checkbox-inline {
      display: flex;
      align-items: center;
      gap: 8px;
      min-height: 42px;
      padding-top: 22px;
      white-space: nowrap;
    }
    .checkbox-inline input {
      width: auto;
      margin: 0;
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
    .review-header {
      position: sticky;
      top: 74px;
      z-index: 6;
      backdrop-filter: blur(6px);
      background: rgba(255,250,240,0.96);
      margin-bottom: 12px;
    }
    .review-header .toolbar {
      justify-content: flex-start;
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
    .filter-group {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      align-items: center;
      padding: 6px 8px;
      border: 1px solid var(--border);
      border-radius: 12px;
      background: rgba(255,255,255,0.5);
    }
    .review-toolbar {
      position: sticky;
      top: 0;
      z-index: 5;
      background: rgba(255,250,240,0.96);
      padding: 6px 0;
      backdrop-filter: blur(4px);
      gap: 0;
    }
    .review-count {
      font-weight: 700;
      white-space: nowrap;
      padding-right: 8px;
    }
    .reviewer-toggle {
      min-width: 0;
    }
    .verdict-filter {
      align-items: flex-start;
    }
    .verdict-grid {
      display: grid;
      grid-template-columns: auto repeat(4, auto);
      gap: 8px 12px;
      align-items: center;
    }
    .verdict-grid-header {
      font-size: 0.78rem;
      font-weight: 700;
      color: var(--muted);
      white-space: nowrap;
    }
    .verdict-grid-row {
      font-size: 0.82rem;
      font-weight: 700;
      color: var(--muted);
      white-space: nowrap;
      padding-right: 4px;
    }
    .checkbox-chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
      font-size: 0.88rem;
    }
    .checkbox-chip input {
      width: auto;
      margin: 0;
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
    .jobs {
      display: grid;
      gap: 12px;
      min-width: 0;
    }
    .job-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      background: #fff;
      padding: 12px;
      min-width: 0;
      max-width: 100%;
      overflow: hidden;
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
      overflow-wrap: anywhere;
      max-width: 100%;
      overflow-x: hidden;
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
    .reasoning a {
      color: var(--accent);
      font-weight: 700;
      text-decoration: underline;
      text-underline-offset: 2px;
    }
    .review-notes {
      display: grid;
      gap: 8px;
      margin-top: 10px;
    }
    .review-notes textarea {
      min-height: 78px;
      font-family: Georgia, "Times New Roman", serif;
      font-size: 0.92rem;
      line-height: 1.4;
      resize: vertical;
    }
    .review-notes .save-note {
      color: var(--muted);
      font-size: 0.82rem;
      min-height: 1.2em;
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
    .coverage-key {
      display: grid;
      gap: 16px;
    }
    .coverage-grid-title {
      font-weight: 700;
      margin: 0 0 6px;
    }
    .coverage-grid {
      display: grid;
      grid-template-columns: auto 1fr 1fr;
      gap: 6px;
      align-items: stretch;
    }
    .coverage-grid-head,
    .coverage-grid-rowhead,
    .coverage-grid-cell {
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 8px 10px;
      background: #fff;
      font-size: 0.9rem;
    }
    .coverage-grid-head,
    .coverage-grid-rowhead {
      font-weight: 700;
    }
    .coverage-grid-head {
      text-align: center;
    }
    .coverage-grid-rowhead {
      display: flex;
      align-items: center;
      white-space: nowrap;
    }
    .coverage-grid-corner {
      border: none;
      background: transparent;
      padding: 0;
    }
    .coverage-grid-cell {
      min-height: 56px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      gap: 2px;
      cursor: pointer;
    }
    .coverage-grid-cell:hover {
      outline: 2px solid rgba(31, 26, 23, 0.18);
      outline-offset: -2px;
    }
    .coverage-grid-count {
      font-weight: 700;
    }
    .coverage-grid-percent {
      color: var(--muted);
      font-size: 0.82rem;
    }
    .coverage-detail {
      margin-top: 14px;
      padding: 12px;
      border: 1px solid var(--border);
      border-radius: 12px;
      background: #fffdf8;
      display: grid;
      gap: 8px;
    }
    .coverage-detail[hidden] {
      display: none;
    }
    .coverage-detail-title {
      font-weight: 700;
    }
    .coverage-detail-list {
      display: grid;
      gap: 6px;
      max-height: 240px;
      overflow: auto;
    }
    .coverage-detail-item {
      padding: 6px 8px;
      border: 1px solid #e7dccb;
      border-radius: 10px;
      background: #fff;
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
      .run-controls-grid { grid-template-columns: 1fr 1fr; }
      .verify-controls-grid { grid-template-columns: 1fr 1fr; }
      .inspect-chart { grid-template-columns: 1fr; }
      .navbar { position: static; }
    }
    @media (max-width: 700px) {
      .run-controls-grid { grid-template-columns: 1fr; }
      .verify-controls-grid { grid-template-columns: 1fr; }
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
    <nav class="navbar" aria-label="Primary">
      <div class="nav-tabs">
        <button id="tabRunBtn" class="tab-btn active" type="button" data-tab="run">Run Research</button>
        <button id="tabVerifyVerdictsBtn" class="tab-btn" type="button" data-tab="verify-verdicts">Verify Verdicts</button>
        <button id="tabReviewBtn" class="tab-btn" type="button" data-tab="review">Review Changes</button>
        <button id="tabInspectBtn" class="tab-btn" type="button" data-tab="inspect">Inspect Data</button>
        <button id="tabLogsBtn" class="tab-btn" type="button" data-tab="logs">Inspect Logs</button>
      </div>
      <div class="nav-controls">
        <label>Changes File
          <select id="changesFile"></select>
        </label>
      </div>
    </nav>
    <section id="tab-run" class="tab-panel">
      <div class="run-stack">
        <section class="panel">
          <div class="panel-header">Run Research</div>
          <div class="panel-body stack">
            <div class="run-controls-grid">
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
              <label class="checkbox-inline"><input id="skipCandidatesWithChanges" type="checkbox"> Skip reviewed candidates</label>
            </div>
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
    <section id="tab-verify-verdicts" class="tab-panel" hidden>
      <div class="verify-stack">
        <section class="panel">
          <div class="panel-header">Verify Verdicts</div>
          <div class="panel-body stack">
            <div class="verify-controls-grid">
              <label>Provider
                <select id="verdictReviewProvider"></select>
              </label>
              <label>Candidate Key
                <input id="verdictReviewCandidate" type="text" placeholder="Optional exact Candidate_Key">
              </label>
              <label>Timeout (seconds)
                <input id="verdictReviewTimeout" type="number" min="1" step="1" value="300">
              </label>
            </div>
            <div class="filter-group verdict-filter">
              <div class="verdict-grid">
                <span></span>
                <span class="verdict-grid-header">no_record</span>
                <span class="verdict-grid-header">nuanced</span>
                <span class="verdict-grid-header">naughty</span>
                <span class="verdict-grid-header">nice</span>
                <span class="verdict-grid-row">Before</span>
                <label class="checkbox-chip"><input id="verdictReviewBeforeNoRecord" type="checkbox" checked><span></span></label>
                <label class="checkbox-chip"><input id="verdictReviewBeforeNuanced" type="checkbox" checked><span></span></label>
                <label class="checkbox-chip"><input id="verdictReviewBeforeNaughty" type="checkbox" checked><span></span></label>
                <label class="checkbox-chip"><input id="verdictReviewBeforeNice" type="checkbox" checked><span></span></label>
                <span class="verdict-grid-row">After</span>
                <label class="checkbox-chip"><input id="verdictReviewAfterNoRecord" type="checkbox" checked><span></span></label>
                <label class="checkbox-chip"><input id="verdictReviewAfterNuanced" type="checkbox" checked><span></span></label>
                <label class="checkbox-chip"><input id="verdictReviewAfterNaughty" type="checkbox" checked><span></span></label>
                <label class="checkbox-chip"><input id="verdictReviewAfterNice" type="checkbox" checked><span></span></label>
              </div>
            </div>
            <div class="toolbar">
              <button id="startVerdictReviewBtn" class="primary" type="button">Start Verdict Review</button>
              <button id="clearFinishedVerdictJobsBtn" type="button">Clear Finished Jobs</button>
              <span class="job-meta">Runs `scripts/verdict_review.py` and streams the live log below.</span>
            </div>
          </div>
        </section>
        <section class="panel">
          <div class="panel-header">Verdict Review Output</div>
          <div class="panel-body">
            <div id="verdictJobsMirror" class="jobs"></div>
          </div>
        </section>
      </div>
    </section>
    <section id="tab-review" class="tab-panel" hidden>
      <div class="panel-header review-header">
        <div class="toolbar review-toolbar">
          <span id="reviewCandidateCount" class="review-count">0 candidates</span>
          <label class="reviewer-toggle">
            <select id="reviewer">
              <option value="D">D</option>
              <option value="I">I</option>
            </select>
          </label>
          <div class="filter-group">
            <label><input id="showPending" type="checkbox" checked> Pending</label>
            <label><input id="showApproved" type="checkbox"> Approved</label>
            <label><input id="showDenied" type="checkbox"> Denied</label>
          </div>
          <div class="filter-group">
            <label><input id="showMod" type="checkbox" checked> Mod</label>
            <label><input id="showAdd" type="checkbox" checked> Add</label>
            <label><input id="showDel" type="checkbox" checked> Delete</label>
          </div>
          <div class="filter-group verdict-filter">
            <div class="verdict-grid">
              <span></span>
              <span class="verdict-grid-header">no_record</span>
              <span class="verdict-grid-header">nuanced</span>
              <span class="verdict-grid-header">naughty</span>
              <span class="verdict-grid-header">nice</span>
              <span class="verdict-grid-row">Before</span>
              <label class="checkbox-chip"><input id="showBeforeNoRecord" type="checkbox" checked><span></span></label>
              <label class="checkbox-chip"><input id="showBeforeNuanced" type="checkbox" checked><span></span></label>
              <label class="checkbox-chip"><input id="showBeforeNaughty" type="checkbox" checked><span></span></label>
              <label class="checkbox-chip"><input id="showBeforeNice" type="checkbox" checked><span></span></label>
              <span class="verdict-grid-row">After</span>
              <label class="checkbox-chip"><input id="showAfterNoRecord" type="checkbox" checked><span></span></label>
              <label class="checkbox-chip"><input id="showAfterNuanced" type="checkbox" checked><span></span></label>
              <label class="checkbox-chip"><input id="showAfterNaughty" type="checkbox" checked><span></span></label>
              <label class="checkbox-chip"><input id="showAfterNice" type="checkbox" checked><span></span></label>
            </div>
          </div>
        </div>
      </div>
      <section class="panel">
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
          <div class="inspect-chart">
            <div id="changeTypeChart"></div>
            <div id="changeTypeLegend" class="inspect-legend"></div>
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
    let meta = { providers: [], verdict_review_providers: [], races: [], candidates_by_race: {}, default_prompt_template: "" };
    let logSort = { column: "Candidate", direction: "asc" };
    let logsDataCache = { columns: [], rows: [] };
    const reasoningDrafts = new Map();
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
      const normalized = status || "pending";
      const label = normalized ? normalized.charAt(0).toUpperCase() + normalized.slice(1) : "Pending";
      const icons = {
        pending: "🟡",
        approved: "🟢",
        denied: "🔴",
        applied: "✅",
        conflict: "⚠️"
      };
      return `${icons[normalized] || "⚪"} ${label} (change_id:${changeId})`;
    }

    function selectedReviewer() {
      return document.getElementById("reviewer")?.value || "D";
    }

    function formatReviewBadge(reviewer, status, changeId) {
      return `${reviewer}: ${formatStatus(status, changeId)}`;
    }

    function reviewerReasoningField(reviewer) {
      return reviewer === "D" ? "Reasoning D" : "Reasoning I";
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
      if (!el) {
        if (cls === "error") console.error(text);
        else console.log(text);
        return;
      }
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

    function renderLinkedValue(field, value, className="") {
      const text = value || "—";
      if (field !== "URL" || !value) {
        return className ? `<span class="${className}">${escapeHtml(text)}</span>` : escapeHtml(text);
      }
      const safeHref = escapeHtml(value);
      const safeText = escapeHtml(value);
      const link = `<a href="${safeHref}" target="_blank" rel="noopener noreferrer">${safeText}</a>`;
      return className ? `<span class="${className}">${link}</span>` : link;
    }

    function renderReasoningHtml(text) {
      const source = String(text || "");
      const pattern = /(evidence\\s+(\\d{3,4}))/gi;
      let html = "";
      let lastIndex = 0;
      let match;
      while ((match = pattern.exec(source)) !== null) {
        html += escapeHtml(source.slice(lastIndex, match.index));
        html += `<a href="#evidence-${escapeHtml(match[2])}">${escapeHtml(match[1])}</a>`;
        lastIndex = match.index + match[1].length;
      }
      html += escapeHtml(source.slice(lastIndex));
      return html;
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
          return `<td data-label="${escapeHtml(field)}">${renderLinkedValue(field, oldValue)}</td>`;
        }
        const newValue = updates.get(field) || "—";
        return `<td data-label="${escapeHtml(field)}">${renderLinkedValue(field, oldValue, "diff-old")}${renderLinkedValue(field, newValue, "diff-new")}</td>`;
      }).join("");
      return `
        <table>
          <thead><tr>${headerHtml}</tr></thead>
          <tbody><tr>${valueHtml}</tr></tbody>
        </table>
      `;
    }

    function renderDeleteTable(group) {
      const currentRow = group.current_row || {};
      const headers = visibleFields(group, currentRow);
      if (!headers.length) {
        return '<p class="reasoning">No canonical row found for delete view.</p>';
      }
      const headerHtml = headers.map((field) => `<th>${escapeHtml(field)}</th>`).join("");
      const valueHtml = headers.map((field) => {
        const value = currentRow[field] || "—";
        return `<td data-label="${escapeHtml(field)}">${renderLinkedValue(field, value, "diff-old")}</td>`;
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
        return renderDeleteTable(group);
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
      const values = cells.map((cell) => `<td data-label="${escapeHtml(cell.field)}">${renderLinkedValue(cell.field, cell.value)}</td>`).join("");
      return `
        <table>
          <thead><tr>${headers}</tr></thead>
          <tbody><tr>${values}</tr></tbody>
        </table>
      `;
    }

    function renderChanges(groups) {
      const app = document.getElementById("changes");
      const countEl = document.getElementById("reviewCandidateCount");
      const activeReviewer = selectedReviewer();
      const assignedEvidenceAnchors = new Set();
      const candidateCount = new Set(groups.map((group) => group.candidate).filter((candidate) => candidate && candidate !== "(no candidate)")).size;
      if (countEl) countEl.textContent = `${candidateCount} candidates`;
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
            const verdict = changeGroups[0]?.candidate_verdict || "";
            const candidateLabel = verdict ? `${candidate} (${verdict})` : candidate;
            html += `<section class="candidate-group"><div class="candidate-header">${escapeHtml(candidateLabel)}</div>`;
            const cards = [];
            for (const group of changeGroups) {
              const reasoningField = reviewerReasoningField(activeReviewer);
              const draftKey = `${group.change_id}-${activeReviewer}`;
              const reviewerReasoning = reasoningDrafts.has(draftKey)
                ? reasoningDrafts.get(draftKey)
                : (group[reasoningField] || "");
              let anchorAttrs = "";
              const evidenceKey = String(group.key || "").trim();
              if (group.table === "evidence" && /^\\d{3,4}$/.test(evidenceKey)) {
                anchorAttrs += ` data-evidence-id="${escapeHtml(evidenceKey)}"`;
                if (!assignedEvidenceAnchors.has(evidenceKey)) {
                  assignedEvidenceAnchors.add(evidenceKey);
                  anchorAttrs += ` id="evidence-${escapeHtml(evidenceKey)}"`;
                }
              }
              cards.push(`
                <div class="change-block">
                  <article class="change-card"${anchorAttrs}>
                    <div class="change-head">
                      <div>
                        <div class="badge action-badge">${escapeHtml(cardTitle(group))}</div>
                        <p class="reasoning"><strong>Reasoning</strong><br>${renderReasoningHtml(group.reasoning || "")}</p>
                      </div>
                      <div style="display:grid;gap:6px">
                        <span class="badge">${escapeHtml(formatReviewBadge("D", group.D, group.change_id))}</span>
                        <span class="badge">${escapeHtml(formatReviewBadge("I", group.I, group.change_id))}</span>
                      </div>
                    </div>
                    ${renderChangeTable(group)}
                    <div class="actions">
                      <button class="good" data-change-id="${escapeHtml(group.change_id)}" data-status="approved">Approve ${escapeHtml(activeReviewer)}</button>
                      <button class="danger" data-change-id="${escapeHtml(group.change_id)}" data-status="denied">Deny ${escapeHtml(activeReviewer)}</button>
                      <button data-change-id="${escapeHtml(group.change_id)}" data-status="">Reset ${escapeHtml(activeReviewer)}</button>
                    </div>
                    <div class="review-notes">
                      <label>Reasoning ${escapeHtml(activeReviewer)}
                        <textarea data-change-id="${escapeHtml(group.change_id)}" data-reviewer="${escapeHtml(activeReviewer)}" data-reasoning-field="${escapeHtml(reasoningField)}" placeholder="Optional reviewer reasoning">${escapeHtml(reviewerReasoning)}</textarea>
                      </label>
                      <div class="actions">
                        <button class="primary" type="button" data-save-reasoning="${escapeHtml(group.change_id)}" data-save-reviewer="${escapeHtml(activeReviewer)}">Save ${escapeHtml(activeReviewer)} Reasoning</button>
                      </div>
                      <div class="save-note" data-save-note="${escapeHtml(group.change_id)}-${escapeHtml(activeReviewer)}"></div>
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
      app.querySelectorAll("button[data-change-id][data-status]").forEach((button) => {
        button.addEventListener("click", async () => {
          await updateStatus(button.dataset.changeId, selectedReviewer(), button.dataset.status);
        });
      });
      app.querySelectorAll("[data-save-reasoning]").forEach((button) => {
        button.addEventListener("click", async () => {
          const changeId = button.dataset.saveReasoning || "";
          const reviewer = button.dataset.saveReviewer || "";
          const noteKey = `${changeId}-${reviewer}`;
          const textarea = app.querySelector(`textarea[data-change-id="${CSS.escape(changeId)}"][data-reviewer="${CSS.escape(reviewer)}"]`);
          const statusEl = app.querySelector(`[data-save-note="${CSS.escape(noteKey)}"]`);
          const value = textarea ? textarea.value : "";
          try {
            if (statusEl) statusEl.textContent = "Saving…";
            await updateReviewerReasoning(changeId, reviewer, value);
            reasoningDrafts.set(noteKey, value);
            if (statusEl) statusEl.textContent = "Saved";
          } catch (err) {
            if (statusEl) statusEl.textContent = "Save failed";
          }
        });
      });
    }

    function renderJobs(jobs, rootId, allowedKinds, emptyText) {
      const mirror = document.getElementById(rootId);
      const filteredJobs = jobs.filter((job) => allowedKinds.includes(job.kind));
      if (!filteredJobs.length) {
        if (mirror) mirror.innerHTML = `<div class="empty">${escapeHtml(emptyText)}</div>`;
        return;
      }
      const html = filteredJobs.map((job) => `
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

    function renderChangeTypeChart(rows) {
      const chartRoot = document.getElementById("changeTypeChart");
      const legendRoot = document.getElementById("changeTypeLegend");
      const grouped = new Map();
      for (const row of rows) {
        const changeId = (row.change_id || "").trim();
        if (!changeId) continue;
        if (!grouped.has(changeId)) grouped.set(changeId, []);
        grouped.get(changeId).push(row);
      }
      const counts = new Map([["mod", 0], ["add", 0], ["del", 0]]);
      for (const groupRows of grouped.values()) {
        const action = ((groupRows[0]?.action) || "").trim();
        if (counts.has(action)) {
          counts.set(action, (counts.get(action) || 0) + 1);
        }
      }
      const items = Array.from(counts.entries()).filter(([, count]) => count > 0);
      if (!items.length) {
        chartRoot.innerHTML = '<div class="empty">No change groups to chart.</div>';
        legendRoot.innerHTML = "";
        return;
      }
      const total = items.reduce((sum, [, count]) => sum + count, 0);
      const palette = { mod: "#d4aa5f", add: "#5d7b6f", del: "#a85d7f" };
      const labels = { mod: "Mod", add: "Add", del: "Delete" };
      const radius = 180;
      const cx = 220;
      const cy = 220;
      let angle = 0;
      const slices = items.map(([action, count]) => {
        const sweep = (count / total) * 360;
        const startAngle = angle;
        angle += sweep;
        return {
          action,
          count,
          color: palette[action] || "#9a9085",
          path: slicePath(cx, cy, radius, startAngle, angle),
        };
      });
      chartRoot.innerHTML = `
        <svg viewBox="0 0 440 440" role="img" aria-label="Change type pie chart">
          ${slices.map((slice) => `<path d="${slice.path}" fill="${slice.color}"></path>`).join("")}
          <circle cx="${cx}" cy="${cy}" r="72" fill="#fffaf0"></circle>
          <text x="${cx}" y="${cy - 8}" text-anchor="middle" font-size="20" font-weight="700" fill="#1f1a17">${total}</text>
          <text x="${cx}" y="${cy + 18}" text-anchor="middle" font-size="13" fill="#6b6158">change groups</text>
        </svg>
      `;
      legendRoot.innerHTML = slices.map((slice) => {
        const percent = ((slice.count / total) * 100).toFixed(1);
        return `
          <div class="legend-row">
            <span class="legend-swatch" style="background:${slice.color}"></span>
            <span class="legend-label">${escapeHtml(labels[slice.action] || slice.action)}</span>
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
          candidateKey: (row.Candidate_Key || "").trim() || `${(row.State || "").trim()}|${(row.Office || "").trim()}|${(row.Candidate || "").trim()}`,
          candidate: (row.Candidate || "").trim(),
          state: (row.State || "").trim(),
          office: (row.Office || "").trim(),
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
        const candidateKey = (row.Candidate_Key || "").trim() || `${(row.State || "").trim()}|${(row.Office || "").trim()}|${(row.Candidate || "").trim()}`;
        if (evId && candidateKey) {
          evidenceToCandidate.set(evId, candidateKey);
        }
      }
      const coveredCandidates = new Set();
      const latestVerdictChange = new Map();
      function parseChangeId(value) {
        const n = Number.parseInt(String(value || "").trim(), 10);
        return Number.isFinite(n) ? n : -1;
      }
      for (const row of changeRows) {
        const table = (row.table || "").trim();
        const key = (row.key || "").trim();
        if (table === "candidates" && key) {
          coveredCandidates.add(key);
          if ((row.field || "").trim() === "Verdict") {
            const changeId = parseChangeId(row.change_id);
            const prev = latestVerdictChange.get(key);
            if (!prev || changeId >= prev.changeId) {
              latestVerdictChange.set(key, {
                changeId,
                verdict: (row.value || "").trim() || "no_record",
              });
            }
          }
        } else if (table === "evidence") {
          if (key && evidenceToCandidate.has(key)) {
            coveredCandidates.add(evidenceToCandidate.get(key));
          } else if ((row.field || "").trim() === "Candidate_Key" && (row.value || "").trim()) {
            coveredCandidates.add((row.value || "").trim());
          }
        }
      }
      const combos = new Map();
      for (const row of allCandidates) {
        const reviewed = row.verdict !== "no_record";
        const hasChanges = coveredCandidates.has(row.candidateKey);
        const effectiveVerdict = latestVerdictChange.get(row.candidateKey)?.verdict || row.verdict;
        const effectiveReviewed = effectiveVerdict !== "no_record";
        const active = 128;
        const key = `${reviewed ? active : 0}-${hasChanges ? active : 0}-${effectiveReviewed ? active : 0}`;
        if (!combos.has(key)) {
          combos.set(key, {
            currentReviewed: reviewed,
            hasChanges,
            effectiveReviewed,
            count: 0,
            color: `rgb(${reviewed ? active : 0},${hasChanges ? active : 0},${effectiveReviewed ? active : 0})`,
            candidates: [],
          });
        }
        combos.get(key).count += 1;
        combos.get(key).candidates.push(row);
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
      function getSlice(finalReviewed, startReviewed, reviewed) {
        return slices.find((slice) =>
          slice.effectiveReviewed === finalReviewed &&
          slice.currentReviewed === startReviewed &&
          slice.hasChanges === reviewed
        ) || { count: 0, color: "rgb(255,255,255)", candidates: [] };
      }
      function candidateLabel(candidate) {
        const office = candidate.office || "(unknown office)";
        const state = candidate.state || "(unknown state)";
        return `${candidate.candidate} (${state} - ${office})`;
      }
      function renderCoverageDetail(slice, title) {
        if (!slice.candidates || !slice.candidates.length) {
          return `
            <div class="coverage-detail" hidden>
              <div class="coverage-detail-title"></div>
              <div class="coverage-detail-list"></div>
            </div>
          `;
        }
        const labels = [...slice.candidates]
          .sort((a, b) => candidateLabel(a).localeCompare(candidateLabel(b)))
          .map((candidate) => `<div class="coverage-detail-item">${escapeHtml(candidateLabel(candidate))}</div>`)
          .join("");
        return `
          <div class="coverage-detail">
            <div class="coverage-detail-title">${escapeHtml(title)} (${escapeHtml(String(slice.count))})</div>
            <div class="coverage-detail-list">${labels}</div>
          </div>
        `;
      }
      function renderCoverageGrid(finalReviewed, title) {
        const columns = [
          { reviewed: false, label: "No record (start)" },
          { reviewed: true, label: "AI record (start)" },
        ];
        const rows = [
          { reviewed: true, label: "Reviewed" },
          { reviewed: false, label: "Unreviewed" },
        ];
        return `
          <div>
            <div class="coverage-grid-title">${escapeHtml(title)}</div>
            <div class="coverage-grid">
              <div class="coverage-grid-corner"></div>
              ${columns.map((column) => `<div class="coverage-grid-head">${escapeHtml(column.label)}</div>`).join("")}
              ${rows.map((row) => {
                return `
                  <div class="coverage-grid-rowhead">${escapeHtml(row.label)}</div>
                  ${columns.map((column) => {
                    const slice = getSlice(finalReviewed, column.reviewed, row.reviewed);
                    const percent = total ? ((slice.count / total) * 100).toFixed(1) : "0.0";
                    const detailTitle = `${title} / ${row.label} / ${column.label}`;
                    return `
                      <div class="coverage-grid-cell" style="background:${slice.color}" data-coverage-detail="${escapeHtml(detailTitle)}" data-coverage-key="${escapeHtml(`${finalReviewed ? "1" : "0"}-${column.reviewed ? "1" : "0"}-${row.reviewed ? "1" : "0"}`)}">
                        <div class="coverage-grid-count">${escapeHtml(String(slice.count))}</div>
                        <div class="coverage-grid-percent">${escapeHtml(percent)}%</div>
                      </div>
                    `;
                  }).join("")}
                `;
              }).join("")}
            </div>
          </div>
        `;
      }
      legendRoot.innerHTML = `
        <div class="coverage-key">
          ${renderCoverageGrid(false, "No record (final)")}
          ${renderCoverageGrid(true, "AI record (final)")}
        </div>
        <div id="coverageDetailHost">
          ${renderCoverageDetail({ candidates: [] }, "")}
        </div>
      `;
      const detailHost = document.getElementById("coverageDetailHost");
      function detailSliceForKey(key) {
        const [finalReviewed, startReviewed, reviewed] = String(key || "").split("-");
        return getSlice(finalReviewed === "1", startReviewed === "1", reviewed === "1");
      }
      legendRoot.querySelectorAll("[data-coverage-key]").forEach((cell) => {
        cell.addEventListener("click", () => {
          const detailKey = cell.getAttribute("data-coverage-key") || "";
          const detailTitle = cell.getAttribute("data-coverage-detail") || "Coverage bucket";
          detailHost.innerHTML = renderCoverageDetail(detailSliceForKey(detailKey), detailTitle);
        });
      });
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

    function populateVerdictReviewProviders() {
      const select = document.getElementById("verdictReviewProvider");
      const providers = meta.verdict_review_providers || [];
      let savedProvider = "";
      try {
        savedProvider = localStorage.getItem("reviewChanges.verdictReviewProvider") || "";
      } catch {}
      const currentProvider = select.value;
      select.innerHTML = providers.map((provider) => `
        <option value="${escapeHtml(provider.name)}">${escapeHtml(provider.label)}</option>
      `).join("");
      if (savedProvider && providers.some((provider) => provider.name === savedProvider)) {
        select.value = savedProvider;
      } else if (currentProvider && providers.some((provider) => provider.name === currentProvider)) {
        select.value = currentProvider;
      } else if (providers.length) {
        select.value = providers[0].name;
      }
    }

    function selectedChangesFile() {
      return document.getElementById("changesFile").value || "changes.csv";
    }

    function populateChangesFiles() {
      const select = document.getElementById("changesFile");
      let saved = "";
      try {
        saved = localStorage.getItem("reviewChanges.selectedChangesFile") || "";
      } catch {}
      const current = select.value;
      const files = meta.change_files || ["changes.csv"];
      select.innerHTML = files.map((name) => `
        <option value="${escapeHtml(name)}">${escapeHtml(name)}</option>
      `).join("");
      if (saved && files.includes(saved)) {
        select.value = saved;
      } else if (current && files.includes(current)) {
        select.value = current;
      } else if ((meta.current_changes_file || "") && files.includes(meta.current_changes_file)) {
        select.value = meta.current_changes_file;
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
      populateChangesFiles();
      populateProviders();
      populateVerdictReviewProviders();
      populateRaces();
      document.getElementById("promptTemplate").value = meta.default_prompt_template || "";
      try {
        document.getElementById("candidateVerdict").value = localStorage.getItem("reviewChanges.candidateVerdict") || "";
        document.getElementById("skipCandidatesWithChanges").checked = localStorage.getItem("reviewChanges.skipCandidatesWithChanges") === "true";
        document.getElementById("verdictReviewCandidate").value = localStorage.getItem("reviewChanges.verdictReviewCandidate") || "";
        document.getElementById("verdictReviewTimeout").value = localStorage.getItem("reviewChanges.verdictReviewTimeout") || "300";
      } catch {}
    }

    async function fetchChanges() {
      const showPending = document.getElementById("showPending").checked;
      const showApproved = document.getElementById("showApproved").checked;
      const showDenied = document.getElementById("showDenied").checked;
      const showMod = document.getElementById("showMod").checked;
      const showAdd = document.getElementById("showAdd").checked;
      const showDel = document.getElementById("showDel").checked;
      const showBeforeNoRecord = document.getElementById("showBeforeNoRecord").checked;
      const showBeforeNuanced = document.getElementById("showBeforeNuanced").checked;
      const showBeforeNaughty = document.getElementById("showBeforeNaughty").checked;
      const showBeforeNice = document.getElementById("showBeforeNice").checked;
      const showAfterNoRecord = document.getElementById("showAfterNoRecord").checked;
      const showAfterNuanced = document.getElementById("showAfterNuanced").checked;
      const showAfterNaughty = document.getElementById("showAfterNaughty").checked;
      const showAfterNice = document.getElementById("showAfterNice").checked;
      const reviewer = selectedReviewer();
      const changesFile = selectedChangesFile();
      const res = await fetch(
        `/api/changes?show_pending=${showPending ? "1" : "0"}&show_approved=${showApproved ? "1" : "0"}&show_denied=${showDenied ? "1" : "0"}&show_mod=${showMod ? "1" : "0"}&show_add=${showAdd ? "1" : "0"}&show_del=${showDel ? "1" : "0"}&show_before_no_record=${showBeforeNoRecord ? "1" : "0"}&show_before_nuanced=${showBeforeNuanced ? "1" : "0"}&show_before_naughty=${showBeforeNaughty ? "1" : "0"}&show_before_nice=${showBeforeNice ? "1" : "0"}&show_after_no_record=${showAfterNoRecord ? "1" : "0"}&show_after_nuanced=${showAfterNuanced ? "1" : "0"}&show_after_naughty=${showAfterNaughty ? "1" : "0"}&show_after_nice=${showAfterNice ? "1" : "0"}&reviewer=${encodeURIComponent(reviewer)}&changes_file=${encodeURIComponent(changesFile)}`
      );
      const data = await res.json();
      renderChanges(data.groups || []);
    }

    async function fetchJobs() {
      const res = await fetch("/api/jobs");
      const data = await res.json();
      renderJobs(data.jobs || [], "jobsMirror", ["run", "apply"], "No research/apply jobs started yet.");
      renderJobs(data.jobs || [], "verdictJobsMirror", ["verdict_review"], "No verdict review jobs started yet.");
    }

    async function fetchInspectData() {
      const table = document.getElementById("inspectTable").value;
      const filter = document.getElementById("inspectFilter").value.trim();
      const params = new URLSearchParams({ table });
      params.set("changes_file", selectedChangesFile());
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
        fetch(`/api/data?table=changes&changes_file=${encodeURIComponent(selectedChangesFile())}`),
      ]);
      const evidenceData = await evidenceRes.json();
      const candidateData = await candidatesRes.json();
      const changesData = await changesRes.json();
      renderDomainChart(evidenceData.rows || []);
      renderWikipediaList(evidenceData.rows || []);
      renderVerdictChart(candidateData.rows || []);
      renderChangeCoverageChart(candidateData.rows || [], evidenceData.rows || [], changesData.rows || []);
      renderChangeTypeChart(changesData.rows || []);
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

    async function updateStatus(changeId, reviewer, status) {
      setStatus("Saving…", "info");
      const res = await fetch("/api/status", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ change_id: changeId, reviewer, status, changes_file: selectedChangesFile() })
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Save failed.", "error");
        return;
      }
      setStatus(`Updated ${changeId} -> ${reviewer}:${status}`, "ok");
      await fetchChanges();
    }

    async function updateReviewerReasoning(changeId, reviewer, reasoning) {
      const res = await fetch("/api/reviewer-reasoning", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ change_id: changeId, reviewer, reasoning, changes_file: selectedChangesFile() })
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data.error || "Reasoning save failed.");
      }
      return data;
    }

    async function startRun() {
      setStatus("Starting job…", "info");
      const payload = {
        provider: document.getElementById("provider").value,
        race: document.getElementById("race").value,
        candidate: document.getElementById("candidate").value,
        candidate_verdict: document.getElementById("candidateVerdict").value,
        skip_candidates_with_changes: document.getElementById("skipCandidatesWithChanges").checked,
        prompt_template: document.getElementById("promptTemplate").value,
        changes_file: selectedChangesFile()
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
        all_races: true,
        changes_file: selectedChangesFile()
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

    function selectedVerdictReviewValues(prefix) {
      const mapping = [
        ["NoRecord", "no_record"],
        ["Nuanced", "nuanced"],
        ["Naughty", "naughty"],
        ["Nice", "nice"],
      ];
      return mapping
        .filter(([suffix]) => document.getElementById(`${prefix}${suffix}`).checked)
        .map(([, value]) => value);
    }

    async function startVerdictReview() {
      setStatus("Starting verdict review job…", "info");
      const oldVerdicts = selectedVerdictReviewValues("verdictReviewBefore");
      const newVerdicts = selectedVerdictReviewValues("verdictReviewAfter");
      if (!oldVerdicts.length) {
        setStatus("Select at least one 'Before' verdict filter.", "error");
        return;
      }
      if (!newVerdicts.length) {
        setStatus("Select at least one 'After' verdict filter.", "error");
        return;
      }
      const payload = {
        provider: document.getElementById("verdictReviewProvider").value,
        candidate: document.getElementById("verdictReviewCandidate").value.trim(),
        timeout: Number.parseInt(document.getElementById("verdictReviewTimeout").value || "300", 10),
        old_verdicts: oldVerdicts,
        new_verdicts: newVerdicts,
        changes_file: selectedChangesFile()
      };
      const res = await fetch("/api/verdict-review", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!res.ok) {
        setStatus(data.error || "Failed to start verdict review job.", "error");
        return;
      }
      setStatus(`Started ${data.job.provider} verdict review job ${data.job.id}`, "ok");
      await fetchJobs();
    }

    async function applyApproved() {
      setStatus("Starting apply job…", "info");
      const res = await fetch("/api/apply", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ changes_file: selectedChangesFile() })
      });
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
      const names = ["run", "verify-verdicts", "review", "inspect", "logs"];
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
      } else if (tabName === "verify-verdicts") {
        fetchJobs().catch(console.error);
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
    document.getElementById("verdictReviewProvider").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.verdictReviewProvider", document.getElementById("verdictReviewProvider").value);
      } catch {}
    });
    document.getElementById("changesFile").addEventListener("change", async () => {
      try {
        localStorage.setItem("reviewChanges.selectedChangesFile", document.getElementById("changesFile").value);
      } catch {}
      await fetchChanges();
      if (!document.getElementById("tab-inspect").hidden) {
        await fetchDomainChart();
        await fetchInspectData();
      }
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
    document.getElementById("verdictReviewCandidate").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.verdictReviewCandidate", document.getElementById("verdictReviewCandidate").value);
      } catch {}
    });
    document.getElementById("verdictReviewTimeout").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.verdictReviewTimeout", document.getElementById("verdictReviewTimeout").value);
      } catch {}
    });
    document.getElementById("skipCandidatesWithChanges").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.skipCandidatesWithChanges", String(document.getElementById("skipCandidatesWithChanges").checked));
      } catch {}
    });
    document.getElementById("raceSearch").addEventListener("input", populateRaces);
    document.getElementById("showPending").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.showPending", String(document.getElementById("showPending").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("showApproved").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.showApproved", String(document.getElementById("showApproved").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("showDenied").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.showDenied", String(document.getElementById("showDenied").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("showMod").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.showMod", String(document.getElementById("showMod").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("showAdd").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.showAdd", String(document.getElementById("showAdd").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("showDel").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.showDel", String(document.getElementById("showDel").checked));
      } catch {}
      fetchChanges();
    });
    document.getElementById("reviewer").addEventListener("change", () => {
      try {
        localStorage.setItem("reviewChanges.reviewer", document.getElementById("reviewer").value);
      } catch {}
      fetchChanges();
    });
    [
      "showBeforeNoRecord",
      "showBeforeNuanced",
      "showBeforeNaughty",
      "showBeforeNice",
      "showAfterNoRecord",
      "showAfterNuanced",
      "showAfterNaughty",
      "showAfterNice",
    ].forEach((id) => {
      document.getElementById(id).addEventListener("change", () => {
        try {
          localStorage.setItem(`reviewChanges.${id}`, String(document.getElementById(id).checked));
        } catch {}
        fetchChanges();
      });
    });
    [
      "verdictReviewBeforeNoRecord",
      "verdictReviewBeforeNuanced",
      "verdictReviewBeforeNaughty",
      "verdictReviewBeforeNice",
      "verdictReviewAfterNoRecord",
      "verdictReviewAfterNuanced",
      "verdictReviewAfterNaughty",
      "verdictReviewAfterNice",
    ].forEach((id) => {
      document.getElementById(id).addEventListener("change", () => {
        try {
          localStorage.setItem(`reviewChanges.${id}`, String(document.getElementById(id).checked));
        } catch {}
      });
    });
    document.getElementById("reloadBtn").addEventListener("click", refreshAll);
    document.getElementById("runBtn").addEventListener("click", startRun);
    document.getElementById("runAllBtn").addEventListener("click", startAllRacesRun);
    document.getElementById("startVerdictReviewBtn").addEventListener("click", startVerdictReview);
    document.getElementById("clearFinishedVerdictJobsBtn").addEventListener("click", clearFinishedJobs);
    document.getElementById("clearFinishedBtn").addEventListener("click", clearFinishedJobs);
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
        if (savedTab && ["run", "verify-verdicts", "review", "inspect", "logs"].includes(savedTab)) {
          initialTab = savedTab;
        }
        document.getElementById("reviewer").value = localStorage.getItem("reviewChanges.reviewer") || "D";
        document.getElementById("showPending").checked = localStorage.getItem("reviewChanges.showPending") !== "false";
        document.getElementById("showApproved").checked = localStorage.getItem("reviewChanges.showApproved") === "true";
        document.getElementById("showDenied").checked = localStorage.getItem("reviewChanges.showDenied") === "true";
        document.getElementById("showMod").checked = localStorage.getItem("reviewChanges.showMod") !== "false";
        document.getElementById("showAdd").checked = localStorage.getItem("reviewChanges.showAdd") !== "false";
        document.getElementById("showDel").checked = localStorage.getItem("reviewChanges.showDel") !== "false";
        document.getElementById("showBeforeNoRecord").checked = localStorage.getItem("reviewChanges.showBeforeNoRecord") !== "false";
        document.getElementById("showBeforeNuanced").checked = localStorage.getItem("reviewChanges.showBeforeNuanced") !== "false";
        document.getElementById("showBeforeNaughty").checked = localStorage.getItem("reviewChanges.showBeforeNaughty") !== "false";
        document.getElementById("showBeforeNice").checked = localStorage.getItem("reviewChanges.showBeforeNice") !== "false";
        document.getElementById("showAfterNoRecord").checked = localStorage.getItem("reviewChanges.showAfterNoRecord") !== "false";
        document.getElementById("showAfterNuanced").checked = localStorage.getItem("reviewChanges.showAfterNuanced") !== "false";
        document.getElementById("showAfterNaughty").checked = localStorage.getItem("reviewChanges.showAfterNaughty") !== "false";
        document.getElementById("showAfterNice").checked = localStorage.getItem("reviewChanges.showAfterNice") !== "false";
        document.getElementById("verdictReviewBeforeNoRecord").checked = localStorage.getItem("reviewChanges.verdictReviewBeforeNoRecord") !== "false";
        document.getElementById("verdictReviewBeforeNuanced").checked = localStorage.getItem("reviewChanges.verdictReviewBeforeNuanced") !== "false";
        document.getElementById("verdictReviewBeforeNaughty").checked = localStorage.getItem("reviewChanges.verdictReviewBeforeNaughty") !== "false";
        document.getElementById("verdictReviewBeforeNice").checked = localStorage.getItem("reviewChanges.verdictReviewBeforeNice") !== "false";
        document.getElementById("verdictReviewAfterNoRecord").checked = localStorage.getItem("reviewChanges.verdictReviewAfterNoRecord") !== "false";
        document.getElementById("verdictReviewAfterNuanced").checked = localStorage.getItem("reviewChanges.verdictReviewAfterNuanced") !== "false";
        document.getElementById("verdictReviewAfterNaughty").checked = localStorage.getItem("reviewChanges.verdictReviewAfterNaughty") !== "false";
        document.getElementById("verdictReviewAfterNice").checked = localStorage.getItem("reviewChanges.verdictReviewAfterNice") !== "false";
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


def changes_csv_path(filename: str | None = None) -> Path:
    return resolve_changes_csv(filename)


def ensure_changes_csv(filename: str | None = None) -> None:
    path = changes_csv_path(filename)
    if path.exists():
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_changes(filename: str | None = None) -> list[dict[str, str]]:
    path = changes_csv_path(filename)
    ensure_changes_csv(path.name)
    rows = load_csv(path)
    out = []
    for row in rows:
        normalized = {field: row.get(field, "").strip() for field in FIELDNAMES}
        out.append(normalized)
    return out


def load_data_table(name: str, filter_text: str, changes_filename: str | None = None) -> dict[str, object]:
    table_map = {
        "races": RACES_CSV,
        "candidates": CANDIDATES_CSV,
        "evidence": EVIDENCE_CSV,
        "changes": changes_csv_path(changes_filename),
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


def write_changes(rows: list[dict[str, str]], filename: str | None = None) -> None:
    with changes_csv_path(filename).open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def set_change_status(change_id: str, reviewer: str, status: str, filename: str | None = None) -> int:
    if reviewer not in REVIEW_COLUMNS:
        raise ValueError(f"Invalid reviewer: {reviewer}")
    if status and status not in VALID_STATUSES:
        raise ValueError(f"Invalid status: {status}")
    rows = load_changes(filename)
    count = 0
    for row in rows:
        if row["change_id"] == change_id:
            row[reviewer] = status
            count += 1
    if count == 0:
        raise KeyError(change_id)
    write_changes(rows, filename)
    return count


def set_change_reasoning(change_id: str, reviewer: str, reasoning_text: str, filename: str | None = None) -> int:
    if reviewer not in REVIEW_COLUMNS:
        raise ValueError(f"Invalid reviewer: {reviewer}")
    reasoning_field = f"Reasoning {reviewer}"
    rows = load_changes(filename)
    count = 0
    for row in rows:
        if row["change_id"] == change_id:
            row[reasoning_field] = reasoning_text
            count += 1
    if count == 0:
        raise KeyError(change_id)
    write_changes(rows, filename)
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


def available_verdict_review_providers() -> list[dict[str, str]]:
    providers: list[dict[str, str]] = []
    has_claude = bool(resolve_cli("claude"))
    has_codex = bool(resolve_cli("codex"))
    if has_codex:
        providers.append({"name": "codex", "label": "Codex"})
    if has_claude:
        providers.append({"name": "claude", "label": "Claude"})
    if has_codex and has_claude:
        providers.append({"name": "both", "label": "Both"})
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


def candidate_key(row: dict[str, str]) -> str:
    existing = row.get("Candidate_Key", "").strip()
    if existing:
        return existing
    candidate = row.get("Candidate", "").strip()
    state = row.get("State", "").strip()
    office = row.get("Office", "").strip()
    if candidate and state and office:
        return f"{state}|{office}|{candidate}"
    return candidate


def build_review_groups(rows: list[dict[str, str]], *, all_rows: list[dict[str, str]] | None = None) -> list[dict[str, object]]:
    candidate_rows = load_csv(CANDIDATES_CSV)
    evidence_rows = load_csv(EVIDENCE_CSV)
    source_rows = all_rows if all_rows is not None else rows
    unique_name_lookup: dict[str, dict[str, object]] = {}
    counts: dict[str, int] = {}
    for row in candidate_rows:
        name = row.get("Candidate", "").strip()
        counts[name] = counts.get(name, 0) + 1
    candidate_lookup = {
        candidate_key(row): {
            "state": row.get("State", "").strip(),
            "office": row.get("Office", "").strip(),
            "candidate": row.get("Candidate", "").strip(),
            "row": dict(row),
        }
        for row in candidate_rows
    }
    for row in candidate_rows:
        name = row.get("Candidate", "").strip()
        if counts.get(name, 0) == 1:
            unique_name_lookup[name] = candidate_lookup[candidate_key(row)]
    current_verdict_by_candidate_key = {
        candidate_key(row): (row.get("Verdict", "").strip() or "no_record")
        for row in candidate_rows
    }
    latest_verdict_change_by_candidate_key: dict[str, tuple[int, str]] = {}
    for row in source_rows:
        if row.get("table", "").strip() != "candidates":
            continue
        if row.get("field", "").strip() != "Verdict":
            continue
        candidate_key_value = row.get("key", "").strip()
        if not candidate_key_value:
            continue
        try:
            change_id = int(row.get("change_id", "0").strip() or "0")
        except ValueError:
            change_id = 0
        verdict_value = row.get("value", "").strip() or "no_record"
        previous = latest_verdict_change_by_candidate_key.get(candidate_key_value)
        if previous is None or change_id >= previous[0]:
            latest_verdict_change_by_candidate_key[candidate_key_value] = (change_id, verdict_value)
    evidence_lookup = {
        row.get("Evidence_ID", "").strip(): {
            "candidate": row.get("Candidate", "").strip(),
            "candidate_key": row.get("Candidate_Key", "").strip(),
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
        candidate_key_value = ""
        evidence_label = ""
        current_row: dict[str, str] = {}

        if table == "races":
            state, office = parse_race_key(key)
            evidence_label = "(race change)"
        elif table == "candidates":
            candidate_key_value = key
            candidate_meta = candidate_lookup.get(key, {})
            candidate = str(candidate_meta.get("candidate", "")).strip() or key
            state = str(candidate_meta.get("state", "")).strip()
            office = str(candidate_meta.get("office", "")).strip()
            evidence_label = "(candidate change)"
            current_row = dict(candidate_meta.get("row", {}))
        elif table == "evidence":
            if key and key in evidence_lookup:
                evidence_meta = evidence_lookup[key]
                candidate = str(evidence_meta.get("candidate", "")).strip()
                candidate_key_value = str(evidence_meta.get("candidate_key", "")).strip()
                candidate_meta = candidate_lookup.get(candidate_key_value, {})
                if not candidate_meta:
                    candidate_meta = unique_name_lookup.get(candidate, {})
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
                candidate_key_value = next(
                    (
                        row.get("value", "").strip()
                        for row in group
                        if row.get("field", "").strip() == "Candidate_Key" and row.get("value", "").strip()
                    ),
                    "",
                )
                evidence_label = "New evidence"
                candidate_key_value = candidate_key_value.strip()
                candidate_meta = candidate_lookup.get(candidate_key_value, {})
                if not candidate_meta:
                    candidate_meta = unique_name_lookup.get(candidate, {})
                if not candidate:
                    candidate = str(candidate_meta.get("candidate", "")).strip()
            state = str(candidate_meta.get("state", "")).strip()
            office = str(candidate_meta.get("office", "")).strip()

        race = office or "(unknown race)"
        if not state:
            state = "(unknown state)"
        if not candidate:
            candidate = "(no candidate)"
        if not evidence_label:
            evidence_label = "(row change)"
        current_verdict = current_verdict_by_candidate_key.get(candidate_key_value, "no_record")
        final_verdict = latest_verdict_change_by_candidate_key.get(candidate_key_value, (0, current_verdict))[1]

        out.append(
            {
                "change_id": change_id,
                "table": table,
                "key": key,
                "candidate_key": candidate_key_value,
                "D": first.get("D", "").strip(),
                "Reasoning D": first.get("Reasoning D", "").strip(),
                "I": first.get("I", "").strip(),
                "Reasoning I": first.get("Reasoning I", "").strip(),
                "reasoning": first.get("reasoning", "").strip(),
                "state": state,
                "race": race,
                "candidate": candidate,
                "current_verdict": current_verdict,
                "final_verdict": final_verdict,
                "candidate_verdict": str(candidate_meta.get("row", {}).get("Verdict", "")).strip(),
                "evidence": evidence_label,
                "current_row": current_row,
                "rows": group,
            }
        )
    return out


def filter_review_groups(
    groups: list[dict[str, object]],
    allowed_before: set[str],
    allowed_after: set[str],
) -> list[dict[str, object]]:
    filtered: list[dict[str, object]] = []
    for group in groups:
        current_verdict = str(group.get("current_verdict", "")).strip() or "no_record"
        final_verdict = str(group.get("final_verdict", "")).strip() or current_verdict
        if current_verdict not in allowed_before:
            continue
        if final_verdict not in allowed_after:
            continue
        filtered.append(group)
    return filtered


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
                "verdict_review_providers": available_verdict_review_providers(),
                "races": races,
                "candidates_by_race": candidates_by_race,
                "default_prompt_template": load_race_runner_prompt_template(),
                "change_files": available_changes_filenames(),
                "current_changes_file": CHANGES_CSV.name,
            })
            return
        if parsed.path == "/api/changes":
            params = parse_qs(parsed.query)
            changes_filename = params.get("changes_file", [CHANGES_CSV.name])[0].strip() or CHANGES_CSV.name
            show_pending = params.get("show_pending", ["1"])[0] == "1"
            show_approved = params.get("show_approved", ["0"])[0] == "1"
            show_denied = params.get("show_denied", ["0"])[0] == "1"
            show_mod = params.get("show_mod", ["1"])[0] == "1"
            show_add = params.get("show_add", ["1"])[0] == "1"
            show_del = params.get("show_del", ["1"])[0] == "1"
            show_before_no_record = params.get("show_before_no_record", ["1"])[0] == "1"
            show_before_nuanced = params.get("show_before_nuanced", ["1"])[0] == "1"
            show_before_naughty = params.get("show_before_naughty", ["1"])[0] == "1"
            show_before_nice = params.get("show_before_nice", ["1"])[0] == "1"
            show_after_no_record = params.get("show_after_no_record", ["1"])[0] == "1"
            show_after_nuanced = params.get("show_after_nuanced", ["1"])[0] == "1"
            show_after_naughty = params.get("show_after_naughty", ["1"])[0] == "1"
            show_after_nice = params.get("show_after_nice", ["1"])[0] == "1"
            reviewer = params.get("reviewer", ["D"])[0].strip() or "D"
            if reviewer not in REVIEW_COLUMNS:
                self._send_json({"error": f"Unknown reviewer: {reviewer}"}, HTTPStatus.BAD_REQUEST)
                return
            all_rows = load_changes(changes_filename)
            rows = list(all_rows)
            allowed_statuses = set()
            if show_pending:
                allowed_statuses.update({"", "pending"})
            if show_approved:
                allowed_statuses.add("approved")
            if show_denied:
                allowed_statuses.add("denied")
            rows = [row for row in rows if row.get(reviewer, "").strip() in allowed_statuses]
            allowed_actions = set()
            if show_mod:
                allowed_actions.add("mod")
            if show_add:
                allowed_actions.add("add")
            if show_del:
                allowed_actions.add("del")
            groups = build_review_groups(rows, all_rows=all_rows)
            groups = [
                group for group in groups
                if str(group.get("rows", [{}])[0].get("action", "")).strip() in allowed_actions
            ]
            allowed_before = set()
            if show_before_no_record:
                allowed_before.add("no_record")
            if show_before_nuanced:
                allowed_before.add("nuanced")
            if show_before_naughty:
                allowed_before.add("naughty")
            if show_before_nice:
                allowed_before.add("nice")
            allowed_after = set()
            if show_after_no_record:
                allowed_after.add("no_record")
            if show_after_nuanced:
                allowed_after.add("nuanced")
            if show_after_naughty:
                allowed_after.add("naughty")
            if show_after_nice:
                allowed_after.add("nice")
            groups = filter_review_groups(groups, allowed_before, allowed_after)
            self._send_json({"groups": groups})
            return
        if parsed.path == "/api/jobs":
            self._send_json({"jobs": JOB_MANAGER.list_jobs()})
            return
        if parsed.path == "/api/data":
            params = parse_qs(parsed.query)
            table = params.get("table", ["races"])[0].strip()
            filter_text = params.get("filter", [""])[0].strip()
            changes_filename = params.get("changes_file", [CHANGES_CSV.name])[0].strip() or CHANGES_CSV.name
            try:
                payload = load_data_table(table, filter_text, changes_filename)
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
            reviewer = str(payload.get("reviewer", "")).strip()
            status = str(payload.get("status", "")).strip()
            changes_filename = str(payload.get("changes_file", "")).strip() or CHANGES_CSV.name
            if not change_id or not reviewer:
                self._send_json({"error": "Missing change_id or reviewer."}, HTTPStatus.BAD_REQUEST)
                return
            try:
                updated = set_change_status(change_id, reviewer, status, changes_filename)
            except ValueError as exc:
                self._send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)
                return
            except KeyError:
                self._send_json({"error": f"Unknown change_id: {change_id}"}, HTTPStatus.NOT_FOUND)
                return
            self._send_json({"ok": True, "updated": updated})
            return

        if self.path == "/api/reviewer-reasoning":
            change_id = str(payload.get("change_id", "")).strip()
            reviewer = str(payload.get("reviewer", "")).strip()
            reasoning = str(payload.get("reasoning", ""))
            changes_filename = str(payload.get("changes_file", "")).strip() or CHANGES_CSV.name
            if not change_id or not reviewer:
                self._send_json({"error": "Missing change_id or reviewer."}, HTTPStatus.BAD_REQUEST)
                return
            try:
                updated = set_change_reasoning(change_id, reviewer, reasoning, changes_filename)
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
            changes_filename = str(payload.get("changes_file", "")).strip() or CHANGES_CSV.name
            if not all_races and not race:
                self._send_json({"error": "Missing race."}, HTTPStatus.BAD_REQUEST)
                return
            if provider not in {item["name"] for item in available_providers()}:
                self._send_json({"error": f"Provider unavailable: {provider}"}, HTTPStatus.BAD_REQUEST)
                return
            command = [sys.executable, str(RACE_RUNNER), "--provider", provider]
            command.extend(["--changes-file", changes_filename])
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

        if self.path == "/api/verdict-review":
            provider = str(payload.get("provider", "")).strip() or "codex"
            candidate = str(payload.get("candidate", "")).strip()
            timeout = int(payload.get("timeout", 300) or 300)
            old_verdicts = [
                str(value).strip()
                for value in payload.get("old_verdicts", [])
                if str(value).strip()
            ]
            new_verdicts = [
                str(value).strip()
                for value in payload.get("new_verdicts", [])
                if str(value).strip()
            ]
            changes_filename = str(payload.get("changes_file", "")).strip() or CHANGES_CSV.name
            valid_providers = {item["name"] for item in available_verdict_review_providers()}
            if provider not in valid_providers:
                self._send_json({"error": f"Verdict review provider unavailable: {provider}"}, HTTPStatus.BAD_REQUEST)
                return
            valid_verdicts = {"no_record", "nuanced", "naughty", "nice"}
            if any(value not in valid_verdicts for value in old_verdicts + new_verdicts):
                self._send_json({"error": "Invalid verdict filter."}, HTTPStatus.BAD_REQUEST)
                return
            if timeout < 1:
                self._send_json({"error": "Timeout must be at least 1 second."}, HTTPStatus.BAD_REQUEST)
                return
            command = [
                sys.executable,
                str(VERDICT_REVIEW),
                "--provider",
                provider,
                "--timeout",
                str(timeout),
                "--changes-file",
                changes_filename,
                "--output",
                str(REPORTS_DIR / f"verdict_review_confidence.{Path(changes_filename).stem}.csv"),
            ]
            if candidate:
                command.extend(["--candidate", candidate])
            for verdict in old_verdicts:
                command.extend(["--old-verdict", verdict])
            for verdict in new_verdicts:
                command.extend(["--new-verdict", verdict])
            job_candidate = candidate or "pending verdict changes"
            job = JOB_MANAGER.start(kind="verdict_review", provider=provider, race=changes_filename, candidate=job_candidate, command=command)
            self._send_json({"ok": True, "job": job})
            return

        if self.path == "/api/apply":
            changes_filename = str(payload.get("changes_file", "")).strip() or CHANGES_CSV.name
            command = [sys.executable, str(APPLY_CHANGES), "--changes-file", changes_filename]
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
