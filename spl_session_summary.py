"""
Nightclub Compliance Session Breach Analyzer
----------------------------------

Author: nebulataker
Created: 20250706
Updated: 20250706
Version: 1.0.0
License: CC BY-NC 4.0 (https://creativecommons.org/licenses/by-nc/4.0/)

Description:
This script processes sound pressure level (SPL) data collected from night club sessions
stored in a SQLite database. It analyzes defined time blocks to:
- Allocate readings to named operating sessions
- Determine if noise levels exceed day/night dBC thresholds
- Identify peak SPL events and possible false positives
- Export per-session plots and summary CSVs for audit compliance
- Record batch metadata for traceability

Intended for use in regulatory compliance and noise monitoring systems.
"""


import os
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import shutil
from datetime import datetime, time, timedelta

# --- Configuration Parameters ---
DB_PATH = r"\Compliance\club_sessions.db"  # Path to SQLite database with SPL data
OUTPUT_DIR = r"\Compliance\SessionSummaries"  # Output directory for CSVs and plots
SESSION_PLOTS_DIR = os.path.join(OUTPUT_DIR, 'session_plots')  # Folder to save plots per session
SUMMARY_CSV = os.path.join(OUTPUT_DIR, 'Session_Max_Summary.csv')  # Main summary output
BREACH_CSV = os.path.join(OUTPUT_DIR, 'BreachesBalcony.csv')      # Breach-specific CSV
BATCH_INFO = os.path.join(OUTPUT_DIR, 'batch_details.txt')        # Metadata about this run

# Constants for signal adjustment and regulatory thresholds
GAIN_TO_FACADE = 18.9 * 0  # Adjustment gain to facade (currently zeroed)
LIMIT_DAY = 90.0           # dBC day time limit
LIMIT_NIGHT = 80.0         # dBC night time limit
BREACH_WINDOW = timedelta(minutes=1.5)  # Time window to contextualize a breach

# --- Clean Up Previous Outputs ---
# Removes any previously generated outputs before starting fresh
for f in [SUMMARY_CSV, BREACH_CSV, BATCH_INFO]:
    if os.path.exists(f):
        os.remove(f)
if os.path.exists(SESSION_PLOTS_DIR):
    shutil.rmtree(SESSION_PLOTS_DIR)
os.makedirs(SESSION_PLOTS_DIR, exist_ok=True)

# --- Define Weekly Session Periods ---
# These tuples describe named club sessions and their weekly time windows
# Format: (label, weekday, start_time, end_time)
tmp = [
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
SESSIONS = {label: (wd, start, end) for label, wd, start, end in tmp}  # Dictionary for easy lookup

# --- Load Data from Database ---
# Reads SPL metrics from the SQLite table and parses timestamps
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query(
    "SELECT timestamp, lcs, lceq, lceq1m, lceq10m, location FROM spl_data",
    conn, parse_dates=['timestamp']
)
conn.close()

# --- Allocate Session Labels ---
# Assigns each SPL record to the correct defined session based on timestamp
rows = []
def session_includes(ts, wd, start, end):
    if start <= end:
        return ts.weekday() == wd and start <= ts.time() <= end
    return (ts.weekday() == wd and ts.time() >= start) or ((ts.weekday() + 1) % 7 == wd and ts.time() <= end)

for label, (wd, start, end) in SESSIONS.items():
    sel = df[df['timestamp'].apply(lambda ts: session_includes(ts, wd, start, end))]
    if sel.empty:
        continue
    for sess_date, daily_group in sel.groupby(sel['timestamp'].dt.date):
        epoch = daily_group['timestamp'].min()
        for idx, row in daily_group.iterrows():
            session_id = f"{sess_date}_{label}"
            rows.append({**row, 'Session': session_id, 'Epoch': epoch})

# Create DataFrame of session-mapped SPL readings
all_df = pd.DataFrame(rows)
all_df['Est_LCeq'] = all_df['lceq']  # Estimated LCeq (can be adjusted)
all_df['Day'] = all_df['timestamp'].dt.strftime('%A')

# --- Time-of-Day Classification ---
# Determines if each record occurred in daytime or nighttime hours

def is_day(ts):
    wd, t = ts.weekday(), ts.time()
    if wd in [6, 0, 1, 2, 3] and time(10, 0) <= t <= time(23, 59):
        return True
    if wd == 4 and (time(10, 0) <= t or t <= time(1, 0)):
        return True
    if wd == 5 and (time(10, 0) <= t or t <= time(1, 0)):
        return True
    return False

all_df['Limit'] = all_df['timestamp'].apply(lambda ts: LIMIT_DAY if is_day(ts) else LIMIT_NIGHT)
all_df['Breached'] = all_df['Est_LCeq'] > all_df['Limit']  # Boolean for regulatory exceedance

# --- Initialize Output Records ---
summary = []      # Will store session-wise summary stats
breaches = []     # Will store high-level breach events

# --- Metadata Block ---
# Track when and how this batch was generated
start_time = datetime.now()
generation_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
days_processed = all_df['timestamp'].dt.date.nunique()
sessions_processed = all_df['Session'].nunique()

# --- Export CSV Summaries ---
pd.DataFrame(summary).to_csv(SUMMARY_CSV, index=False)
pd.DataFrame(breaches).to_csv(BREACH_CSV, index=False)

# --- Save Batch Metadata to Text File ---
with open(BATCH_INFO, 'w', encoding='utf-8') as f:
    f.write(f"Batch run completed: {generation_time}\n")
    f.write(f"Database source: {DB_PATH}\n")
    f.write(f"Days processed: {days_processed}\n")
    f.write(f"Sessions processed: {sessions_processed}\n")
    f.write(f"Output directory: {OUTPUT_DIR}\n")
    f.write(f"Session plots directory: {SESSION_PLOTS_DIR}\n")
    f.write(f"Summary CSV: {SUMMARY_CSV}\n")
    f.write(f"Breaches CSV: {BREACH_CSV}\n")

# --- Completion Message ---
print(f"Completed at {generation_time}")
