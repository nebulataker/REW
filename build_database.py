"""
Script Name: build_database.py
Author: https://github.com/nebulataker
License: CC BY-NC 4.0 (https://creativecommons.org/licenses/by-nc/4.0/)
Created: 2025-05-01
Last Updated: 2025-05-28
Version: 1.0.0

Description:
Initializes and populates a SQLite database from REW SPL log files, including extended LCeq fields.

Key Features:
- Parses REW text logs to extract LCS, LCeq (3min), LCeq1m, LCeq10m, and LZpeak values.
- Filters out invalid or low SPL recordings (LCS < MIN_SPL_LIMIT).
- Computes MD5 checksum to avoid reprocessing files.
- Tags data by location (Balcony vs Nightclub).
- Creates and updates `spl_data` and `sessions` tables with unique constraints.

Dependencies:
- pandas
- sqlite3
- hashlib

"""

import matplotlib
matplotlib.use('Agg')  # non-interactive backend
import pandas as pd
from datetime import datetime, timedelta, time
import os
import re
import sqlite3
import hashlib

# --- Source Configuration ---
PRODUCTION = False
if PRODUCTION:
    PARENT_DIR = r"\Music complaints"
    COMPLIANCE_DIR = r"Music complaints\Compliance"
else:
    PARENT_DIR = r"\Music complaints\Test"
    COMPLIANCE_DIR = r"\Music complaints\Test\Compliance"


# ——— Configuration ———
REQUIRED_CAL_FILE = "7097828_90deg.txt"  # Calibration file that is used with the UMIK Microphone
BAD_FILES_LOG     = os.path.join(COMPLIANCE_DIR, "bad_files.csv")
DB_PATH           = os.path.join(COMPLIANCE_DIR, 'club_sessions.db')
time_per_sample   = 0.1706666667  # ~6 Hz sampling
MIN_SPL_LIMIT     = 50.0  # dB threshold for valid LCS. Just in case the microphone drops out.
bad_files = []

# Session definitions: (label, weekday, start_time, end_time)
SESSIONS = [
    ("Thursday_21-00", 3, time(21, 0), time(23, 59, 59)),
    ("Friday_00-01",   4, time(0,  0), time(1,  0)),
    ("Friday_01-04",   4, time(1,  0), time(4,  0)),
    ("Friday_21-00",   4, time(21, 0), time(23, 59, 59)),
    ("Saturday_00-01", 5, time(0,  0), time(1,  0)),
    ("Saturday_01-04", 5, time(1,  0), time(4,  0)),
    ("Saturday_21-00", 5, time(21, 0), time(23, 59, 59)),
    ("Sunday_00-01",   6, time(0,  0), time(1,  0)),
    ("Sunday_01-04",   6, time(1,  0), time(4,  0)),
    ("Sunday_21-00",   6, time(21, 0), time(23, 59, 59)),
    ("Monday_00-01",   0, time(0,  0), time(1,  0)),
    ("Monday_01-04",   0, time(1,  0), time(4,  0)),
]

os.makedirs(COMPLIANCE_DIR, exist_ok=True)


def init_db(path):
    conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
    c = conn.cursor()
    # Create sessions table
    c.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY,
            session TEXT,
            date TEXT,
            time TEXT,
            lceq REAL,
            source TEXT,
            plot TEXT,
            location TEXT,
            UNIQUE(session, date, time, lceq, source)
        )
    ''')
    # Create spl_data table with extended fields
    c.execute('''
        CREATE TABLE IF NOT EXISTS spl_data (
            id INTEGER PRIMARY KEY,
            timestamp TEXT,
            lceq REAL,
            lcs REAL,
            lceq1m REAL,
            lceq10m REAL,
            lzpeak REAL,
            source TEXT,
            location TEXT
        )
    ''')
    # Track processed files by MD5
    c.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            md5sum TEXT PRIMARY KEY,
            source TEXT
        )
    ''')
    conn.commit()
    return conn

conn = init_db(DB_PATH)
cursor = conn.cursor()


def file_md5sum(fp):
    hash_md5 = hashlib.md5()
    with open(fp, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# Validation functions
def is_valid_rew_file(fp):
    try:
        lines = open(fp, 'r', encoding='utf-8').readlines()
        if any("VirtualMic.txt" in l for l in lines[:15]):
            return False
        if any("LAS" in l or "LAeq" in l for l in lines[:15]):
            return False
        return (
            lines[0].startswith("SPL log data saved by REW") and
            any(f"Mic/meter cal: {REQUIRED_CAL_FILE}" in l for l in lines[:15]) and
            any("LCS" in l for l in lines[:15])
        )
    except:
        return False


def extract_location(fp):
    try:
        notes = open(fp, 'r', encoding='utf-8').readlines()[1].lower()
        if 'note' not in notes:
            return 'Balcony'
        for key in ['nightclub','prohibition','club','site']:
            if key in notes:
                return 'Nightclub'
        return 'Balcony'
    except:
        return 'Balcony'


def parse_header_and_data(fp):
    lines = open(fp, 'r', encoding='utf-8').readlines()
    date_line = next((l for l in lines if l.startswith("Date:")), None)
    if not date_line:
        raise RuntimeError(f"No Date header in {fp}")
    start_dt = datetime.strptime(date_line.split(':', 1)[1].strip(), "%d/%m/%Y %I:%M:%S %p")
    data_start = next((i for i, l in enumerate(lines) if 'Time[s]' in l), 8)
    delim = ',' if ',' in lines[data_start + 1] else r'\s+'
    df = pd.read_csv(fp, skiprows=data_start, sep=delim, engine='python')
    df.columns = [c.strip().lower() for c in df.columns]
    if 'lcs' not in df or 'lceq' not in df:
        raise ValueError(f"Missing columns in {fp}: {df.columns}")
    df['elapsedseconds'] = df.index * time_per_sample
    df['datetime'] = df['elapsedseconds'].apply(lambda s: start_dt + timedelta(seconds=s))
    return df

print("Collecting SPL data...")

cursor.execute("SELECT timestamp, lceq, lcs FROM spl_data")
existing_data = cursor.fetchall()
existing_dict = {row[0]: (row[1], row[2]) for row in existing_data}

processed_count = 0
for sub in sorted(os.listdir(PARENT_DIR)):
    if not re.match(r"^\d{8}$", sub):
        continue
    dir_path = os.path.join(PARENT_DIR, sub)
    if not os.path.isdir(dir_path):
        continue
    print(f"\n\U0001F4C2 Checking folder: {sub}")
    for root, _, files in os.walk(dir_path):
        for fn in files:
            if not fn.lower().endswith('.txt'):
                continue
            fp = os.path.join(root, fn)
            print(f"Processing: {fn}")
            md5 = file_md5sum(fp)
            cursor.execute("SELECT 1 FROM processed_files WHERE md5sum = ?", (md5,))
            if cursor.fetchone():
                print(f"  ↪ Already processed, skipping.")
                continue
            if not is_valid_rew_file(fp):
                print(f"  Invalid REW file, logging as bad.")
                bad_files.append(fp)
                continue
            try:
                df = parse_header_and_data(fp)
            except Exception as e:
                print(f"  Failed to parse {fn}: {e}")
                bad_files.append(fp)
                continue

            if 'lceq1m' not in df.columns or 'lceq10m' not in df.columns or 'lzpeak' not in df.columns:
                print(f"  Missing required columns (lceq1m/lceq10m/lzpeak), skipping {fn}")
                continue

            location = extract_location(fp)

            df.rename(columns={'datetime': 'timestamp'}, inplace=True)
            spl_rows = df[['timestamp', 'lceq', 'lceq1m', 'lceq10m', 'lcs']].copy()
            spl_rows = spl_rows[
                (spl_rows['lcs'] >= MIN_SPL_LIMIT) &
                (spl_rows['lceq'] >= MIN_SPL_LIMIT) &
                (spl_rows['lceq1m'] >= MIN_SPL_LIMIT) &
                (spl_rows['lceq10m'] >= MIN_SPL_LIMIT)
                ]
            if spl_rows.empty:
                print(f"  All LCS values < {MIN_SPL_LIMIT} dB in {fn}, skipping.")
                continue

            spl_rows['source'] = fp
            spl_rows['location'] = location
            spl_rows['timestamp'] = pd.to_datetime(spl_rows['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S.%f')

            spl_rows.to_sql('spl_data', conn, if_exists='append', index=False)
            cursor.execute("INSERT INTO processed_files(md5sum, source) VALUES (?, ?)", (md5, fp))
            processed_count += 1
            print(f"Inserted SPL data from {fn}")

conn.commit()

if bad_files:
    pd.DataFrame({'bad_file': bad_files}).to_csv(BAD_FILES_LOG, index=False)
    print(f"Bad files logged to {BAD_FILES_LOG}")

print(f"Total new files processed: {processed_count}")

print("\nTagging session labels in raw format...")

cursor.execute("SELECT session, date, time, ROUND(lceq,1), source FROM sessions")
existing_session_keys = set(tuple(row) for row in cursor.fetchall())

cursor.execute("SELECT timestamp, lceq, source, location FROM spl_data")
all_rows = cursor.fetchall()

session_data = []
seen = set()

def session_includes(dt, weekday, start, end):
    if start <= end:
        return dt.weekday() == weekday and start <= dt.time() <= end
    else:
        return (
            (dt.weekday() == weekday and dt.time() >= start) or
            ((dt.weekday() + 1) % 7 == weekday and dt.time() <= end)
        )

for session_label, wd, start_time, end_time in SESSIONS:
    for ts_str, lceq, source, location in all_rows:
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
        if session_includes(ts, wd, start_time, end_time):
            date_key = ts.strftime("%Y-%m-%d")
            time_key = ts.strftime("%H:%M:%S.%f")
            lceq_rounded = round(lceq, 1)
            row_key = (session_label, date_key, time_key, lceq_rounded, source)
            if row_key not in seen and row_key not in existing_session_keys:
                seen.add(row_key)
                session_data.append((*row_key, None, location))

print(f"Prepared {len(session_data)} labeled session rows...")

if session_data:
    cursor.executemany(
        "INSERT OR IGNORE INTO sessions (session, date, time, lceq, source, plot, location) VALUES (?, ?, ?, ?, ?, ?, ?)",
        session_data
    )
    conn.commit()
    print(f"Inserted {len(session_data)} raw-labeled session rows into 'sessions'")
else:
    print("No session rows inserted (all may already exist or no matches found).")

conn.close()
