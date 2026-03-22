#!/usr/bin/env python3
import csv
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


API_ROOT = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"


def extract_district_code(record):
    if not isinstance(record, dict):
        return ""

    for key, value in record.items():
        if re.fullmatch(r"CD\d+", str(key).upper()) and str(value).strip():
            raw = str(value).strip()
            return "AL" if raw == "00" else raw.zfill(2)

    basename = str(record.get("BASENAME", "")).strip()
    if basename.isdigit():
        return "AL" if basename == "0" else basename.zfill(2)

    name = str(record.get("NAME", ""))
    parts = name.split("Congressional District", 1)[0].strip().split()
    if parts and parts[-1].isdigit():
        return parts[-1].zfill(2)

    geoid = str(record.get("GEOID", "")).strip()
    if len(geoid) == 4 and geoid.isdigit():
        suffix = geoid[-2:]
        return "AL" if suffix == "00" else suffix

    return ""


def lookup_address(address):
    params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    }
    url = f"{API_ROOT}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.load(response)

    matches = payload.get("result", {}).get("addressMatches", [])
    if not matches:
        return None

    match = matches[0]
    geographies = match.get("geographies", {})
    state_records = geographies.get("States") or []
    state_code = str(state_records[0].get("STUSAB", "")).upper() if state_records else ""

    district_record = None
    for layer_name, records in geographies.items():
        if "Congressional District" in layer_name and records:
            district_record = records[0]
            break

    return {
        "matched_address": match.get("matchedAddress", ""),
        "state_code": state_code,
        "district_code": extract_district_code(district_record),
    }


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/district_lookup_test_addresses.csv")
    rows = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
    if len(sys.argv) > 2:
        rows = rows[: int(sys.argv[2])]
    failures = []

    for index, row in enumerate(rows, start=1):
        address = row["address"].strip()
        expected_state = row["state"].strip().upper()
        try:
            result = lookup_address(address)
        except Exception as exc:
            failures.append((index, expected_state, address, f"request failed: {exc}"))
            continue

        if not result:
            failures.append((index, expected_state, address, "no match"))
            continue

        if result["state_code"] != expected_state:
            failures.append(
                (index, expected_state, address, f"wrong state: {result['state_code']}"))
            continue

        if not result["district_code"]:
            failures.append((index, expected_state, address, "missing district"))
            continue

        print(
            f"{index:03d} OK {expected_state}-{result['district_code']} "
            f"{result['matched_address']}"
        , flush=True)
        time.sleep(0.05)

    print(f"\nChecked {len(rows)} addresses.", flush=True)
    if failures:
        print(f"{len(failures)} failures:", flush=True)
        for index, expected_state, address, message in failures:
            print(f"{index:03d} {expected_state} {address} -> {message}", flush=True)
        raise SystemExit(1)

    print("All addresses resolved to a state and congressional district.", flush=True)


if __name__ == "__main__":
    main()
