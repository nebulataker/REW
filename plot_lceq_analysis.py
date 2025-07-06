"""
Script Name: plot_lceq_analysis.py
Author: https://github.com/nebulataker
License: CC BY-NC 4.0 (https://creativecommons.org/licenses/by-nc/4.0/)
Created: 2025-06-20
Last Updated: 2025-06-30
Version: 1.0.0

Description:
This script extracts LCS and LCeq time-series data from a SQLite database for a specified
location and time range, then plots the sound levels with an annotated peak LCeq.

Intended Usage:
Useful for reviewing sound level trends during specific time windows
(e.g., compliance audits or visualization for reports).

Dependencies:
- matplotlib
- pandas
- sqlite3
"""

import matplotlib
matplotlib.use('TkAgg')  # Enable interactive GUI backend for plotting
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import sqlite3

# -------- Configuration --------
DB_PATH = r"/Music complaints/Compliance/club_sessions.db"  # SQLite database path
LOCATION = "Balcony"  # Choose location: "Balcony" or "Nightclub"
START_DATETIME = datetime(2025, 5, 18, 14, 0, 0)  # Start time for data filtering
STOP_DATETIME  = datetime(2025, 5, 18, 20, 0, 0)  # End time for data filtering

# -------- Load Data from SQLite --------
print(f"Connecting to database: {DB_PATH}")
conn = sqlite3.connect(DB_PATH)
query = '''
    SELECT timestamp, lceq, lcs
    FROM spl_data
    WHERE location = ?
      AND timestamp BETWEEN ? AND ?
'''

print(f"Querying data between {START_DATETIME} and {STOP_DATETIME} for location: {LOCATION}")
df = pd.read_sql_query(query, conn, params=(LOCATION, START_DATETIME, STOP_DATETIME))
conn.close()

# -------- Validate and Prepare Data --------
if df.empty:
    print("No data found in the specified time range.")
    exit()

df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values('timestamp')  # Ensure chronological order
print(f"Retrieved {len(df)} rows from database.")

# -------- Analyze Peak LCeq --------
max_lceq_val = df['lceq'].max()
max_lceq_idx = df['lceq'].idxmax()
max_lceq_time = df.loc[max_lceq_idx, 'timestamp']
print(f"Max LCeq = {max_lceq_val:.2f} dB at {max_lceq_time}")

# -------- Plotting --------
print("Generating interactive 4K plot...")
fig, ax = plt.subplots(figsize=(38.4, 21.6))  # 4K canvas at 100 DPI

# Plot LCS and LCeq lines
ax.plot(df['timestamp'], df['lcs'], label='LCS', linewidth=2.5, color='tab:blue')
ax.plot(df['timestamp'], df['lceq'], label='LCeq', linewidth=5.0, color='tab:orange', alpha=0.85)

# Annotate the peak LCeq point
ax.annotate(
    f"Max LCeq: {max_lceq_val:.2f} dB\n{max_lceq_time.strftime('%Y-%m-%d %H:%M:%S')}",
    xy=(max_lceq_time, max_lceq_val),
    xytext=(max_lceq_time, max_lceq_val + 2),
    arrowprops=dict(facecolor='black', shrink=0.05),
    fontsize=16,
    backgroundcolor='white'
)

# Format axes and labels
ax.set_title(f"{LOCATION} | {START_DATETIME:%Y-%m-%d %H:%M} to {STOP_DATETIME:%H:%M}", fontsize=22)
ax.set_ylabel("Level (dB)", fontsize=18)
ax.set_xlabel("Time", fontsize=18)
ax.tick_params(labelsize=14)
ax.grid(True)
ax.legend(fontsize=16)
fig.autofmt_xdate()
plt.tight_layout()

# Display the plot
plt.show()  # Blocks until window is closed
