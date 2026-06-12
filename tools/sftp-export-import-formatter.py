#!/usr/bin/env python3
"""
Klaviyo SFTP Export → Import Formatter (CLI version)
=====================================================
Converts a Klaviyo SFTP export CSV into an import-ready CSV.
Emails are matched by profile_id from a profile mapping CSV.
Rows with no email match are saved to a separate errors CSV.

Usage:
    python3 sftp-export-import-formatter.py <export_csv> <mapping_csv> <metric_name> [options]

Examples:
    python3 sftp-export-import-formatter.py export.csv mapping.csv "Viewed Product"
    python3 sftp-export-import-formatter.py export.csv mapping.csv "Placed Order" -o output.csv

Requirements:
    Python 3.7+, no third-party packages needed.
"""

import csv
import json
import argparse
import sys
import os
from datetime import datetime

# ── Constants ────────────────────────────────────────────────────────────────

REQUIRED_EXPORT_FIELDS  = {"id", "metric_id", "profile_id", "event_properties", "datetime", "uuid"}
REQUIRED_MAPPING_FIELDS = {"Email", "Klaviyo ID"}
SCHEMA_SCAN_ROWS        = 10_000   # rows scanned to discover event_properties columns

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_case_insensitive(d, key):
    """Return the value from dict d whose key matches (case-insensitively)."""
    key_lower = key.lower()
    for k, v in d.items():
        if k.lower() == key_lower:
            return v
    return None


def serialize(val):
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val, separators=(',', ':'))
    return str(val)


def parse_event_properties(row):
    raw = row.get("event_properties", "{}")
    try:
        parsed = json.loads(raw if isinstance(raw, str) else "{}")
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    return {}

# ── Core logic ───────────────────────────────────────────────────────────────

def load_profile_mapping(filepath):
    mapping = {}
    with open(filepath, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fields = set(reader.fieldnames or [])
        missing = REQUIRED_MAPPING_FIELDS - fields
        if missing:
            raise ValueError(
                f'Profile mapping CSV is missing required column(s): {", ".join(sorted(missing))}\n'
                f'Found: {", ".join(sorted(fields))}'
            )
        for row in reader:
            pid   = row.get("Klaviyo ID", "").strip()
            email = row.get("Email", "").strip()
            if pid and email:
                mapping[pid] = email
    return mapping


def collect_prop_keys(filepath, max_rows=SCHEMA_SCAN_ROWS):
    """Scan the first max_rows rows to build the ordered list of event_properties keys."""
    key_order, key_set = [], set()
    with open(filepath, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            for k in parse_event_properties(row):
                if k not in key_set:
                    key_set.add(k)
                    key_order.append(k)
    return key_order


def transform_row(row, metric_name, profile_email_map):
    """
    Returns (output_row, None) on success, or (None, error_row) if no email match.
    """
    props      = parse_event_properties(row)
    profile_id = row.get("profile_id", "").strip()
    email      = profile_email_map.get(profile_id, "")

    if not email:
        return None, {**row, "_reason": "No email found for profile_id"}

    raw_value = get_case_insensitive(props, "value")
    value     = raw_value if (raw_value is not None and raw_value != "") else 0

    event_id = get_case_insensitive(props, "$event_id") or row.get("id", "")

    output_row = {
        "Metric Name":   metric_name,
        "Time":          row.get("datetime", ""),
        "Profile.Email": email,
        "Value":         serialize(value),
        "Unique ID":     serialize(event_id),
    }
    for k, v in props.items():
        output_row[k] = serialize(v)

    return output_row, None

# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Convert a Klaviyo SFTP export CSV into an import-ready CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("export_csv",   help="Path to the SFTP export CSV")
    parser.add_argument("mapping_csv",  help="Path to the profile mapping CSV (requires 'Email' and 'Klaviyo ID' columns)")
    parser.add_argument("metric_name",  help='Metric name to apply to every row, e.g. "Viewed Product"')
    parser.add_argument("-o", "--output", help="Output CSV path (default: sftp_import_<timestamp>.csv)")
    parser.add_argument("--errors",       help="Errors CSV path (default: sftp_import_errors_<timestamp>.csv)")
    args = parser.parse_args()

    ts           = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path  = args.output or f"sftp_import_{ts}.csv"
    errors_path  = args.errors or f"sftp_import_errors_{ts}.csv"

    # ── Step 1: profile mapping ──────────────────────────────────────────────
    print(f"Loading profile mapping from {args.mapping_csv}...")
    try:
        profile_email_map = load_profile_mapping(args.mapping_csv)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"  {len(profile_email_map):,} profiles loaded.")

    # ── Step 2: discover column schema ──────────────────────────────────────
    print(f"Scanning column schema (first {SCHEMA_SCAN_ROWS:,} rows of {args.export_csv})...")
    try:
        prop_keys = collect_prop_keys(args.export_csv)
    except Exception as e:
        print(f"Error scanning schema: {e}", file=sys.stderr)
        sys.exit(1)
    all_columns = ["Metric Name", "Time", "Profile.Email", "Value", "Unique ID"] + prop_keys
    print(f"  {len(prop_keys)} event_properties column(s) found: {', '.join(prop_keys) or '(none)'}")

    # ── Step 3: stream and convert ───────────────────────────────────────────
    print(f"Converting {args.export_csv} → {output_path}...")
    total = output_count = error_count = 0

    try:
        with open(args.export_csv, newline='', encoding='utf-8-sig') as infile, \
             open(output_path, 'w', newline='', encoding='utf-8') as outfile, \
             open(errors_path, 'w', newline='', encoding='utf-8') as errfile:

            reader = csv.DictReader(infile)
            fields = set(reader.fieldnames or [])
            missing = REQUIRED_EXPORT_FIELDS - fields
            if missing:
                print(
                    f"Error: Export CSV is missing required column(s): {', '.join(sorted(missing))}\n"
                    f"Found: {', '.join(sorted(fields))}",
                    file=sys.stderr,
                )
                sys.exit(1)

            writer = csv.DictWriter(
                outfile,
                fieldnames=all_columns,
                extrasaction='ignore',
                lineterminator='\n',
            )
            writer.writeheader()

            err_writer = None  # initialised on first error row so we know the field names

            for row in reader:
                total += 1
                output_row, error_row = transform_row(row, args.metric_name, profile_email_map)

                if error_row:
                    error_count += 1
                    if err_writer is None:
                        err_writer = csv.DictWriter(
                            errfile,
                            fieldnames=list(error_row.keys()),
                            extrasaction='ignore',
                            lineterminator='\n',
                        )
                        err_writer.writeheader()
                    err_writer.writerow(error_row)
                else:
                    for col in all_columns:
                        output_row.setdefault(col, "")
                    writer.writerow(output_row)
                    output_count += 1

                if total % 50_000 == 0:
                    print(f"  Processed {total:,} rows...")

    except Exception as e:
        print(f"Error during conversion: {e}", file=sys.stderr)
        sys.exit(1)

    if error_count == 0:
        os.remove(errors_path)
        errors_path = None

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\nDone.")
    print(f"  Input rows:   {total:,}")
    print(f"  Output rows:  {output_count:,}  →  {output_path}")
    if error_count > 0:
        print(f"  Error rows:   {error_count:,}  →  {errors_path}")
    else:
        print(f"  Error rows:   0")


if __name__ == "__main__":
    main()
