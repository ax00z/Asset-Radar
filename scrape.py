#!/usr/bin/env python3
"""
Toronto Asset Safety Radar v2 — Backend ETL Engine (scrape.py)

This script is the "Extract, Transform, Load" pipeline that runs daily
(via GitHub Actions or manually) to bake fresh data into static JSON files
for the frontend to consume.

Three phases:
  1. EXTRACT  — Fetch raw data from Toronto Police ArcGIS API (paginated, unsorted)
  2. TRANSFORM — Rename columns, validate coordinates, tag theft type
  3. LOAD     — Write clean JSON to public/data/

Usage:
  python scrape.py

Output:
  public/data/auto_thefts.json
  public/data/bike_thefts.json
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone

# ──────────────────────────────────────────────
# 1. CONFIGURATION & ENDPOINTS
# ──────────────────────────────────────────────

# Toronto Police Service ArcGIS REST API endpoints
# Auto Thefts — dedicated endpoint (not the MCI aggregate)
# Bike Thefts — dedicated endpoint
ENDPOINTS = {
    "auto": (
        "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/"
        "Auto_Theft_Open_Data/FeatureServer/0/query"
    ),
    "bike": (
        "https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/"
        "Bicycle_Thefts_Open_Data/FeatureServer/0/query"
    ),
}

# How far back to look (in months)
TIME_WINDOW_MONTHS = 6

# Page size per API request (max the server comfortably handles)
PAGE_SIZE = 2000

# Output directory (relative to project root)
OUTPUT_DIR = os.path.join("public", "data")

# Retry / timeout settings
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 60

# ──────────────────────────────────────────────
# 2. FIELD MAPS  (cryptic API names → clean JSON keys)
# ──────────────────────────────────────────────

FIELD_MAP = {
    "id_fields":            ["EVENT_UNIQUE_ID", "OBJECTID"],
    "date_field":           ["OCC_DATE", "REPORT_DATE"],
    "year_field":           ["OCC_YEAR"],
    "month_field":          ["OCC_MONTH"],
    "day_field":            ["OCC_DAY", "OCC_DOW"],
    "hour_field":           ["OCC_HOUR"],
    "neighbourhood_fields": ["NEIGHBOURHOOD_158", "NEIGHBOURHOOD_140", "HOOD_158", "NEIGHBOURHOOD"],
    "premise_fields":       ["PREMISES_TYPE", "PREMISE_TYPE"],
    "lat_fields":           ["LAT_WGS84", "Y"],
    "lng_fields":           ["LONG_WGS84", "X"],
    "status_fields":        ["STATUS"],
    "division_fields":      ["DIVISION"],
    "location_fields":      ["LOCATION_TYPE"],
}

MONTH_NAME_TO_NUM = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}

# ──────────────────────────────────────────────
# 3. HELPER — robust HTTP GET with retries
# ──────────────────────────────────────────────

def http_get_json(url, params):
    """Make a GET request with URL-encoded params and return parsed JSON.
    Retries up to MAX_RETRIES on transient failures."""

    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(full_url, headers={
                "User-Agent": "TorontoRadar-Scraper/2.0",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                body = resp.read().decode("utf-8")
                data = json.loads(body)

                # ArcGIS sometimes returns 200 but includes an error object
                if "error" in data:
                    code = data["error"].get("code", "?")
                    msg = data["error"].get("message", "Unknown API error")
                    details = data["error"].get("details", [])
                    detail_str = "; ".join(str(d) for d in details) if details else ""
                    full_msg = f"ArcGIS error {code}: {msg}"
                    if detail_str:
                        full_msg += f" [{detail_str}]"
                    raise RuntimeError(full_msg)

                return data

        except (urllib.error.URLError, urllib.error.HTTPError, RuntimeError, OSError) as exc:
            print(f"  ⚠  Attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                print(f"     Retrying in {RETRY_DELAY_SECONDS}s...")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                raise


def discover_fields(endpoint_url):
    """Query the endpoint with no results to discover available field names.
    This helps us build correct WHERE clauses."""
    try:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "resultRecordCount": "1",
            "resultOffset": "0",
        }
        data = http_get_json(endpoint_url, params)
        features = data.get("features", [])
        if features:
            attrs = features[0].get("attributes", {})
            return list(attrs.keys())
        fields_meta = data.get("fields", [])
        if fields_meta:
            return [f["name"] for f in fields_meta]
        return []
    except Exception as exc:
        print(f"  ⚠  Field discovery failed: {exc}")
        return []


# ──────────────────────────────────────────────
# 4. EXTRACT — paginated fetch (no server-side sort!)
# ──────────────────────────────────────────────

def now_utc():
    """Get current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def build_where_clauses(theft_type, available_fields):
    """
    Build a list of WHERE clause strategies to try, from most specific to broadest.
    We inspect available_fields to avoid requesting columns that don't exist.
    """
    cutoff_dt = now_utc() - timedelta(days=TIME_WINDOW_MONTHS * 30)
    cutoff_year = cutoff_dt.year
    current_year = now_utc().year

    clauses = []

    has_occ_year = "OCC_YEAR" in available_fields
    has_occ_date = "OCC_DATE" in available_fields
    has_report_date = "REPORT_DATE" in available_fields

    # Strategy 1: OCC_YEAR filter (most reliable on ArcGIS)
    if has_occ_year:
        clauses.append(
            (f"OCC_YEAR >= {cutoff_year}", f"year >= {cutoff_year}")
        )

    # Strategy 2: Try timestamp on OCC_DATE (epoch ms)
    if has_occ_date:
        cutoff_ms = int(cutoff_dt.timestamp() * 1000)
        clauses.append(
            (f"OCC_DATE >= {cutoff_ms}", f"OCC_DATE epoch >= {cutoff_ms}")
        )

    # Strategy 3: Try timestamp on REPORT_DATE
    if has_report_date:
        cutoff_ms = int(cutoff_dt.timestamp() * 1000)
        clauses.append(
            (f"REPORT_DATE >= {cutoff_ms}", f"REPORT_DATE epoch >= {cutoff_ms}")
        )

    # Strategy 4: Super broad fallback — just get everything
    clauses.append(("1=1", "unfiltered (all records)"))

    return clauses


def fetch_features(endpoint_url, theft_type):
    """
    Download ALL matching features from an ArcGIS FeatureServer endpoint.

    Strategy:
      1. Discover available fields first.
      2. Try multiple WHERE clause strategies until one works.
      3. Paginate in batches of PAGE_SIZE using resultOffset.
      4. Do NOT ask the server to sort — sort in Python memory afterward.

    Returns a list of raw feature dicts.
    """

    print(f"\n{'='*60}")
    print(f"  Fetching: {theft_type.upper()} thefts")
    print(f"  Endpoint: ...{endpoint_url.split('services/')[1][:60]}")

    # Discover fields
    print(f"  Discovering available fields...")
    available_fields = discover_fields(endpoint_url)
    if available_fields:
        print(f"  Found {len(available_fields)} fields: {', '.join(available_fields[:10])}...")
    else:
        print(f"  ⚠  Could not discover fields, will try common patterns")
        available_fields = ["OCC_YEAR", "OCC_DATE", "REPORT_DATE"]  # guess

    # Build WHERE clause strategies
    where_strategies = build_where_clauses(theft_type, available_fields)

    all_features = []

    for where_clause, description in where_strategies:
        print(f"\n  Strategy: {description}")
        print(f"  WHERE:    {where_clause}")

        all_features = []
        offset = 0
        success = True

        try:
            while True:
                params = {
                    "where": where_clause,
                    "outFields": "*",
                    "outSR": "4326",
                    "f": "json",
                    "resultRecordCount": str(PAGE_SIZE),
                    "resultOffset": str(offset),
                    # ⚠ CRITICAL: No "orderByFields" parameter!
                    # The ArcGIS server often returns "Invalid Query" (400)
                    # when asked to sort large datasets.
                }

                page_num = offset // PAGE_SIZE + 1
                print(f"  📥 Page {page_num} (offset={offset})...", end=" ", flush=True)
                data = http_get_json(endpoint_url, params)
                features = data.get("features", [])
                count = len(features)
                print(f"got {count} records")

                if count == 0:
                    break

                all_features.extend(features)
                offset += PAGE_SIZE

                # Safety limit: don't download more than 100k records
                if len(all_features) >= 100_000:
                    print(f"  ⚠  Hit safety limit of 100k records, stopping pagination")
                    break

                # ArcGIS signals "no more pages" when exceededTransferLimit is absent/False
                if not data.get("exceededTransferLimit", False) and count < PAGE_SIZE:
                    break

            if len(all_features) > 0:
                print(f"  ✅ Strategy '{description}' worked — got {len(all_features)} raw features")
                break  # Success! Don't try other strategies
            else:
                print(f"  ⚠  Strategy returned 0 records, trying next...")
                success = False

        except Exception as exc:
            print(f"\n  ⚠  Strategy '{description}' failed: {exc}")
            success = False
            continue

    if not all_features:
        print(f"\n  ❌ All strategies failed for {theft_type} — 0 features retrieved")
        return []

    # ── SORT IN PYTHON MEMORY (newest first) ──
    def sort_key(feature):
        attrs = feature.get("attributes", {})
        occ_date = attrs.get("OCC_DATE") or attrs.get("REPORT_DATE")
        if isinstance(occ_date, (int, float)) and occ_date > 1_000_000_000:
            return -occ_date
        year = attrs.get("OCC_YEAR", 0) or 0
        month = attrs.get("OCC_MONTH", "")
        if isinstance(month, str):
            month = MONTH_NAME_TO_NUM.get(month, 0)
        day = attrs.get("OCC_DAY", 0) or 0
        hour = attrs.get("OCC_HOUR", 0) or 0
        return -(year * 100000000 + int(month or 0) * 1000000 + int(day or 0) * 10000 + int(hour or 0) * 100)

    all_features.sort(key=sort_key)

    print(f"\n  ✅ Total {theft_type} features fetched & sorted: {len(all_features)}")
    return all_features

# ──────────────────────────────────────────────
# 5. TRANSFORM — clean, rename, validate
# ──────────────────────────────────────────────

def _first_of(attrs, field_names, default=None):
    """Return the first non-None value from a list of possible attribute keys."""
    for name in field_names:
        val = attrs.get(name)
        if val is not None:
            return val
    return default


def parse_month(raw_month):
    """Convert month to an integer (1-12). Handles both names and numbers."""
    if isinstance(raw_month, (int, float)):
        return int(raw_month)
    if isinstance(raw_month, str):
        return MONTH_NAME_TO_NUM.get(raw_month, 1)
    return 1


def parse_date_string(raw_date, year, month, day):
    """Produce a clean YYYY-MM-DD date string from whatever the API gives us."""
    # If OCC_DATE is epoch milliseconds, convert it
    if isinstance(raw_date, (int, float)) and raw_date > 1_000_000_000:
        try:
            dt = datetime.fromtimestamp(raw_date / 1000.0, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d")
        except (OSError, ValueError):
            pass

    # If it's already a string, try to parse it
    if isinstance(raw_date, str) and len(raw_date) >= 10:
        return raw_date[:10]

    # Construct from components
    try:
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
    except (TypeError, ValueError):
        return f"{now_utc().year}-01-01"


def process_features(raw_features, theft_type):
    """
    Transform raw ArcGIS features into clean dictionaries.

    Steps:
      - Rename cryptic columns to readable keys
      - Validate coordinates (discard 0,0 or null)
      - Enforce 6-month time window (discard records outside window)
      - Tag each record as 'auto' or 'bike'
      - Return a list of clean dicts ready for JSON serialization
    """

    cutoff_dt = now_utc() - timedelta(days=TIME_WINDOW_MONTHS * 30)
    cutoff_year = cutoff_dt.year
    cutoff_month = cutoff_dt.month

    clean_records = []
    discarded_coords = 0
    discarded_date = 0
    discarded_other = 0

    for feature in raw_features:
        attrs = feature.get("attributes", {})
        geometry = feature.get("geometry", {})

        # ── Extract coordinates ──
        lat = geometry.get("y") if geometry else None
        lng = geometry.get("x") if geometry else None

        if lat is None or lng is None:
            lat = _first_of(attrs, FIELD_MAP["lat_fields"])
            lng = _first_of(attrs, FIELD_MAP["lng_fields"])

        # ── VALIDATION: discard invalid coordinates ──
        try:
            lat = float(lat) if lat is not None else 0.0
            lng = float(lng) if lng is not None else 0.0
        except (TypeError, ValueError):
            lat, lng = 0.0, 0.0

        if lat == 0.0 and lng == 0.0:
            discarded_coords += 1
            continue

        # Basic sanity: must be roughly within Ontario
        if not (41.0 <= lat <= 57.0 and -95.0 <= lng <= -73.0):
            discarded_coords += 1
            continue

        # ── Extract & rename fields ──
        record_id = _first_of(attrs, FIELD_MAP["id_fields"], default=str(len(clean_records)))
        raw_date = _first_of(attrs, FIELD_MAP["date_field"], default="")
        year = _first_of(attrs, FIELD_MAP["year_field"], default=now_utc().year)
        raw_month = _first_of(attrs, FIELD_MAP["month_field"], default=1)
        day = _first_of(attrs, FIELD_MAP["day_field"], default=1)
        hour = _first_of(attrs, FIELD_MAP["hour_field"], default=12)
        neighbourhood = _first_of(attrs, FIELD_MAP["neighbourhood_fields"], default="Unknown")
        premise_type = _first_of(attrs, FIELD_MAP["premise_fields"], default="Unknown")
        status = _first_of(attrs, FIELD_MAP["status_fields"], default="Unknown")
        division = _first_of(attrs, FIELD_MAP["division_fields"], default="")
        location_type = _first_of(attrs, FIELD_MAP["location_fields"], default="")

        month = parse_month(raw_month)
        date_str = parse_date_string(raw_date, year, month, day)

        # ── Enforce 6-month window ──
        try:
            record_year = int(year) if year else 0
            record_month = int(month) if month else 0
            # Compare year*100+month against cutoff
            record_ym = record_year * 100 + record_month
            cutoff_ym = cutoff_year * 100 + cutoff_month
            if record_ym < cutoff_ym:
                discarded_date += 1
                continue
        except (TypeError, ValueError):
            pass  # keep records we can't date-check

        # ── Build clean record ──
        clean_records.append({
            "id": f"{theft_type}-{record_id}",
            "type": theft_type,
            "date": date_str,
            "year": int(year) if year else now_utc().year,
            "month": month,
            "day": int(day) if day else 1,
            "hour": int(hour) if hour else 0,
            "neighbourhood": str(neighbourhood).strip(),
            "premiseType": str(premise_type).strip(),
            "lat": round(lat, 6),
            "lng": round(lng, 6),
            "status": str(status).strip(),
        })

    print(f"  📊 Processed: {len(clean_records)} valid / {discarded_coords} bad coords / {discarded_date} outside window / {discarded_other} other")
    return clean_records

# ──────────────────────────────────────────────
# 6. LOAD — write JSON to public/data/
# ──────────────────────────────────────────────

def save_json(records, filename):
    """Write a list of records to a JSON file inside OUTPUT_DIR."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=None, separators=(",", ":"))

    size_kb = os.path.getsize(filepath) / 1024
    print(f"  💾 Saved {len(records)} records → {filepath} ({size_kb:.1f} KB)")

# ──────────────────────────────────────────────
# 7. MAIN — orchestrate the full ETL pipeline
# ──────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  Toronto Asset Safety Radar v2 — ETL Scraper            ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print(f"  Time:   {now_utc().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Window: Last {TIME_WINDOW_MONTHS} months")
    print(f"  Output: {os.path.abspath(OUTPUT_DIR)}")

    start_time = time.time()
    total_records = 0

    for theft_type, endpoint_url in ENDPOINTS.items():
        filename = f"{theft_type}_thefts.json"

        try:
            # EXTRACT
            raw_features = fetch_features(endpoint_url, theft_type)

            # TRANSFORM
            clean_records = process_features(raw_features, theft_type)

            # LOAD
            save_json(clean_records, filename)
            total_records += len(clean_records)

        except Exception as exc:
            print(f"\n  ❌ FATAL error processing {theft_type} thefts: {exc}")
            print(f"     Writing empty file to prevent frontend crash...")
            save_json([], filename)

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"  ✅ Pipeline complete!")
    print(f"  Total records: {total_records:,}")
    print(f"  Elapsed time:  {elapsed:.1f}s")
    print(f"  Files written to: {os.path.abspath(OUTPUT_DIR)}/")
    print(f"{'='*60}\n")

    if total_records == 0:
        print("  ⚠  WARNING: Zero records saved. The API may be down or the")
        print("     query returned no results. Check the endpoints above.")
        sys.exit(1)

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
