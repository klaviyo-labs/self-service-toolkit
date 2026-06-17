#!/usr/bin/env python3
"""
SFTP Download + Concatenate
============================
Connects to Klaviyo SFTP, walks the date-partitioned export directory,
downloads every .csv.gz (or .csv) file within the configured date range,
decompresses, and concatenates them into a single local CSV.

Usage
-----
    python3 sftp_download_concat.py [output_path]

    output_path  – optional; defaults to LOCAL_DIR/input_<timestamp>.csv

Environment / .env variables
-----------------------------
    SFTP_HOST            (default: sftp.klaviyo.com)
    SFTP_PORT            (default: 22)
    SFTP_USERNAME
    SFTP_PASSWORD        (used if no private key)
    SFTP_PRIVATE_KEY_PATH  (path to RSA private key file)
    SFTP_INPUT_DIR       (default: /exports/events)
    LOCAL_DIR            (default: current directory)
    START_DATE           YYYY-MM-DD, inclusive lower bound (blank = unbounded)
    END_DATE             YYYY-MM-DD, inclusive upper bound (blank = unbounded)
    TEST_RUN             true → only download first TEST_RUN_LIMIT files
    TEST_RUN_LIMIT       (default: 5)
"""

import gzip
import logging
import os
import sys
import tempfile
from datetime import datetime
from typing import List, Tuple

import paramiko
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SFTP_HOST        = os.getenv("SFTP_HOST",             "sftp.klaviyo.com")
SFTP_PORT        = int(os.getenv("SFTP_PORT",         "22"))
SFTP_USERNAME    = os.getenv("SFTP_USERNAME",         "")
SFTP_PASSWORD    = os.getenv("SFTP_PASSWORD",         "")
SFTP_PRIVATE_KEY = os.path.expanduser(os.getenv("SFTP_PRIVATE_KEY_PATH", ""))
SFTP_INPUT_DIR   = os.getenv("SFTP_INPUT_DIR",   "/exports/events")
LOCAL_DIR        = os.getenv("LOCAL_DIR", os.getcwd())
START_DATE       = os.getenv("START_DATE", "")
END_DATE         = os.getenv("END_DATE",   "")
TEST_RUN         = os.getenv("TEST_RUN", "false").lower() == "true"
TEST_RUN_LIMIT   = int(os.getenv("TEST_RUN_LIMIT", "5"))
SFTP_BUFFER_SIZE = 1 << 20  # 1 MB read/write buffer


# ── SFTP helpers ──────────────────────────────────────────────────────────────

def _get_sftp_client() -> Tuple[paramiko.SFTPClient, paramiko.Transport]:
    """Open an authenticated SFTP connection. Supports password or key auth."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    if SFTP_PRIVATE_KEY:
        log.info("SFTP auth: key-based (%s)", SFTP_PRIVATE_KEY)
        private_key = paramiko.RSAKey.from_private_key_file(SFTP_PRIVATE_KEY)
        transport.connect(username=SFTP_USERNAME, pkey=private_key)
    else:
        log.info("SFTP auth: password-based")
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, transport


def _in_date_range(date_str: str) -> bool:
    """Return True if date_str (YYYY-MM-DD) falls within [START_DATE, END_DATE]."""
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return False
    if START_DATE and d < datetime.strptime(START_DATE, "%Y-%m-%d").date():
        return False
    if END_DATE and d > datetime.strptime(END_DATE, "%Y-%m-%d").date():
        return False
    return True


def list_input_files(sftp: paramiko.SFTPClient) -> List[str]:
    """
    Walk SFTP_INPUT_DIR and return a sorted list of all .csv.gz (or .csv)
    file paths under date partitions that fall within the configured date range.

    Expected structure:
        SFTP_INPUT_DIR/
            dt=2025-05-10/
                01-22-45-342142.csv.gz
            dt=2025-05-11/
                ...
    """
    try:
        date_dirs = sorted(sftp.listdir(SFTP_INPUT_DIR))
    except FileNotFoundError:
        raise RuntimeError(f"SFTP directory not found: {SFTP_INPUT_DIR}")

    remote_paths: List[str] = []
    matched_dirs = 0
    for dir_name in date_dirs:
        if not dir_name.startswith("dt="):
            continue
        date_str = dir_name[3:]
        if not _in_date_range(date_str):
            continue
        matched_dirs += 1
        dir_path = f"{SFTP_INPUT_DIR}/{dir_name}"
        try:
            files = sorted(sftp.listdir(dir_path))
        except Exception as exc:
            log.warning("Could not list %s: %s", dir_path, exc)
            continue
        for fname in files:
            if fname.endswith(".csv.gz") or fname.endswith(".csv"):
                remote_paths.append(f"{dir_path}/{fname}")

    if TEST_RUN:
        remote_paths = remote_paths[:TEST_RUN_LIMIT]
        log.info("TEST_RUN=true – limiting to first %d file(s) (found %d total).",
                 TEST_RUN_LIMIT, len(remote_paths))
    else:
        log.info("Found %d file(s) across %d date partition(s) in range [%s, %s].",
                 len(remote_paths), matched_dirs,
                 START_DATE or "unbounded", END_DATE or "unbounded")
    return remote_paths


def download_and_concat(remote_paths: List[str], local_path: str) -> None:
    """
    Download every file in remote_paths, decompress if gzipped, and
    concatenate into a single local CSV at local_path.

    The header row is written once from the first file; subsequent files
    have their header line skipped. Each file is streamed through a temp
    file on disk to avoid holding the full compressed file in memory.
    """
    if not remote_paths:
        raise RuntimeError("No input files found. Check SFTP_INPUT_DIR and date range.")

    sftp, transport = _get_sftp_client()
    try:
        with open(local_path, "w", encoding="utf-8", newline="") as out_fh:
            first_file = True
            for idx, remote_path in enumerate(remote_paths, 1):
                log.info("  [%d/%d] Downloading %s", idx, len(remote_paths), remote_path)
                is_gzip = remote_path.endswith(".gz")

                with tempfile.NamedTemporaryFile(
                    suffix=".csv.gz" if is_gzip else ".csv", delete=False
                ) as tmp_fh:
                    tmp_path = tmp_fh.name
                    with sftp.open(remote_path, "rb") as remote_fh:
                        remote_fh.prefetch()
                        while True:
                            chunk = remote_fh.read(SFTP_BUFFER_SIZE)
                            if not chunk:
                                break
                            tmp_fh.write(chunk)

                try:
                    open_fn = gzip.open if is_gzip else open
                    with open_fn(tmp_path, "rt", encoding="utf-8-sig") as in_fh:
                        for line_num, line in enumerate(in_fh):
                            if line_num == 0 and not first_file:
                                continue  # skip header on all files after the first
                            out_fh.write(line)
                    first_file = False
                finally:
                    os.unlink(tmp_path)
    finally:
        sftp.close()
        transport.close()

    log.info("Done – all files concatenated → %s", local_path)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    os.makedirs(LOCAL_DIR, exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if len(sys.argv) > 1:
        local_output = os.path.abspath(sys.argv[1])
    else:
        local_output = os.path.join(LOCAL_DIR, f"input_{run_ts}.csv")

    log.info("Output file: %s", local_output)
    log.info("Date range:  [%s, %s]", START_DATE or "unbounded", END_DATE or "unbounded")

    sftp, transport = _get_sftp_client()
    try:
        remote_paths = list_input_files(sftp)
    finally:
        sftp.close()
        transport.close()

    if not remote_paths:
        log.error("No files found to download. Check SFTP_INPUT_DIR and date range.")
        sys.exit(1)

    download_and_concat(remote_paths, local_output)


if __name__ == "__main__":
    main()