"""
Publishes the scoring job's CSV output into MariaDB for the dashboard.

Kept separate from the Spark job on purpose - see score_batch.py's docstring
for why. Uses upsert on trip_id so re-running is idempotent.
"""
import csv
import glob
import os
from datetime import datetime

import mysql.connector

CSV_DIR = r"D:\bigdata-final\docs\predictions_out"
DB = dict(host="127.0.0.1", port=3306, user="root", password="",
          database="ridehail")

files = glob.glob(os.path.join(CSV_DIR, "part-*.csv"))
if not files:
    raise SystemExit(f"no part-*.csv found in {CSV_DIR} - "
                     f"run score_batch.py first")

conn = mysql.connector.connect(**DB)
cur = conn.cursor()
cur.execute("TRUNCATE TABLE predictions")

now = datetime.now()
rows = 0
for path in files:
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            cur.execute(
                """INSERT INTO predictions
                   (trip_id, pickup_zone, dropoff_zone, distance_km,
                    traffic_level, actual_min, predicted_min, error_min,
                    scored_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE
                     predicted_min = VALUES(predicted_min),
                     error_min = VALUES(error_min),
                     scored_at = VALUES(scored_at)""",
                (r["trip_id"], r["pickup_zone"], r["dropoff_zone"],
                 float(r["distance_km"]), r["traffic_level"],
                 float(r["actual_min"]), float(r["predicted_min"]),
                 float(r["error_min"]), now))
            rows += 1

conn.commit()
cur.execute("SELECT COUNT(*) FROM predictions")
total = cur.fetchone()[0]
cur.close()
conn.close()

print(f"loaded {rows} predictions, table now holds {total}")