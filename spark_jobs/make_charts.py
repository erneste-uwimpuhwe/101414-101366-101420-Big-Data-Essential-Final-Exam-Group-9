"""
Chart generation for the report and slides.

Reads the insight tables PySpark already computed and wrote to MariaDB, and
renders them as PNG files. Deliberately plain Python rather than a Spark job:
the aggregation work was Spark's, and it is already done - this step only
draws pictures of the results, so putting it inside a Spark driver would add
a JVM, a Python worker bridge and no benefit.

Charts produced in docs/charts/:
  1_zone_demand.png       trips and revenue by pickup zone
  2_hourly_demand.png     the commuter curve - morning and evening peaks
  3_traffic_impact.png    minutes per km by traffic level
  4_model_comparison.png  GBT vs linear baseline on all three metrics
  5_predicted_vs_actual.png  scatter of prediction against truth

Run:
    python spark_jobs\\make_charts.py
"""
import os

import matplotlib
matplotlib.use("Agg")          # no display needed - write straight to file
import matplotlib.pyplot as plt
import mysql.connector

OUT_DIR = os.path.join("docs", "charts")
os.makedirs(OUT_DIR, exist_ok=True)

DB = dict(host="127.0.0.1", port=3306, user="root", password="",
          database="ridehail")

INK = "#14262F"
ACCENT = "#2A6B5F"
WARN = "#B5852C"
ALERT = "#B0472F"

plt.rcParams.update({
    "figure.dpi": 130,
    "font.size": 10,
    "axes.edgecolor": "#C6D1D5",
    "axes.labelcolor": INK,
    "text.color": INK,
    "xtick.color": INK,
    "ytick.color": INK,
    "axes.spines.top": False,
    "axes.spines.right": False,
})


def query(sql):
    conn = mysql.connector.connect(**DB)
    cur = conn.cursor(dictionary=True)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def save(fig, name):
    path = os.path.join(OUT_DIR, name)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {path}")


# --- 1. demand and revenue by zone --------------------------------------
rows = query("""SELECT pickup_zone, trip_count, total_fare
                FROM insight_zone_demand ORDER BY trip_count DESC""")
if rows:
    zones = [r["pickup_zone"] for r in rows]
    counts = [r["trip_count"] for r in rows]
    fares = [float(r["total_fare"]) / 1e6 for r in rows]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4.2))
    a1.barh(zones[::-1], counts[::-1], color=ACCENT)
    a1.set_title("Trips by pickup zone", loc="left", fontweight="600")
    a1.set_xlabel("trips")
    a2.barh(zones[::-1], fares[::-1], color=INK)
    a2.set_title("Fares collected by zone", loc="left", fontweight="600")
    a2.set_xlabel("million RWF")
    fig.suptitle("Insight 1 — demand is evenly spread across zones, "
                 "so positioning gains come from timing not geography",
                 x=0.01, ha="left", fontsize=9, color="#6B7C85")
    save(fig, "1_zone_demand.png")

# --- 2. the commuter curve ----------------------------------------------
rows = query("""SELECT pickup_hour, trip_count, avg_duration
                FROM insight_hourly_demand ORDER BY pickup_hour""")
if rows:
    hours = [r["pickup_hour"] for r in rows]
    counts = [r["trip_count"] for r in rows]
    durations = [float(r["avg_duration"]) for r in rows]

    fig, ax = plt.subplots(figsize=(10, 4.2))
    bars = ax.bar(hours, counts, color="#C6D1D5")
    for h, b in zip(hours, bars):
        if h in (7, 8, 17, 18, 19):
            b.set_color(WARN)          # the generator's rush hours
    ax.set_xlabel("hour of day")
    ax.set_ylabel("trips")
    ax.set_xticks(range(0, 24))
    ax.set_title("Insight 2 — demand by hour, rush hours highlighted",
                 loc="left", fontweight="600")

    ax2 = ax.twinx()
    ax2.plot(hours, durations, color=ALERT, marker="o", markersize=3.5,
             linewidth=1.6, label="mean trip time")
    ax2.set_ylabel("mean minutes", color=ALERT)
    ax2.tick_params(axis="y", colors=ALERT)
    ax2.spines["right"].set_visible(True)
    ax2.legend(loc="upper left", frameon=False, fontsize=8)
    save(fig, "2_hourly_demand.png")

# --- 3. what congestion costs -------------------------------------------
rows = query("""SELECT traffic_level, trip_count, avg_min_per_km
                FROM insight_traffic_impact ORDER BY avg_min_per_km""")
if rows:
    levels = [r["traffic_level"] for r in rows]
    mpk = [float(r["avg_min_per_km"]) for r in rows]

    fig, ax = plt.subplots(figsize=(7.5, 4))
    colors = [ACCENT, "#7A9A7A", WARN, ALERT][:len(levels)]
    bars = ax.bar(levels, mpk, color=colors, width=0.6)
    for b, v in zip(bars, mpk):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.08, f"{v:.2f}",
                ha="center", fontsize=9, fontweight="600")
    ax.set_ylabel("minutes per kilometre")
    ax.set_ylim(0, max(mpk) * 1.18)
    ax.set_title("Insight 3 — congestion more than doubles travel time "
                 "per kilometre", loc="left", fontweight="600")
    ax.text(0, -0.22, "Normalised for trip length, so this isolates traffic "
            "rather than distance. This is the evidence for using "
            "traffic_level as a model feature.",
            transform=ax.transAxes, fontsize=8, color="#6B7C85")
    save(fig, "3_traffic_impact.png")

# --- 4. model comparison ------------------------------------------------
rows = query("""SELECT model_name, rmse, mae, r2 FROM model_metrics
                ORDER BY trained_at DESC LIMIT 2""")
if len(rows) >= 2:
    rows = rows[::-1]
    names = [r["model_name"].split(" (")[0] for r in rows]
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.6))
    for ax, metric, label, lower_better in [
            (axes[0], "rmse", "RMSE (minutes)", True),
            (axes[1], "mae", "MAE (minutes)", True),
            (axes[2], "r2", "R squared", False)]:
        vals = [float(r[metric]) for r in rows]
        cols = [ALERT if (v == max(vals)) == lower_better else ACCENT
                for v in vals]
        bars = ax.bar(names, vals, color=cols, width=0.55)
        for b, v in zip(bars, vals):
            ax.text(b.get_x() + b.get_width() / 2, v * 1.02, f"{v:.3f}",
                    ha="center", fontsize=9, fontweight="600")
        ax.set_title(label, loc="left", fontsize=9.5, fontweight="600")
        ax.set_ylim(0, max(vals) * 1.22)
        ax.tick_params(axis="x", labelsize=8.5)
    fig.suptitle("Gradient-boosted trees against a linear baseline — "
                 "green is better", x=0.01, ha="left", fontweight="600")
    save(fig, "4_model_comparison.png")
else:
    print("skipped chart 4: need two rows in model_metrics")

# --- 5. predicted vs actual --------------------------------------------
rows = query("""SELECT actual_min, predicted_min FROM predictions
                LIMIT 2000""")
if rows:
    actual = [float(r["actual_min"]) for r in rows]
    pred = [float(r["predicted_min"]) for r in rows]
    lim = max(max(actual), max(pred)) * 1.05

    fig, ax = plt.subplots(figsize=(5.6, 5.4))
    ax.scatter(actual, pred, s=7, alpha=0.3, color=ACCENT,
               edgecolors="none")
    ax.plot([0, lim], [0, lim], color=ALERT, linewidth=1.2,
            linestyle="--", label="perfect prediction")
    ax.set_xlabel("actual minutes")
    ax.set_ylabel("predicted minutes")
    ax.set_xlim(0, lim)
    ax.set_ylim(0, lim)
    ax.legend(frameon=False, fontsize=8.5)
    ax.set_title("Predicted against actual trip time",
                 loc="left", fontweight="600")
    ax.text(0, -0.13, "Points hug the diagonal. Spread around it is the "
            "generator's irreducible noise, which no model can predict.",
            transform=ax.transAxes, fontsize=8, color="#6B7C85")
    save(fig, "5_predicted_vs_actual.png")
else:
    print("skipped chart 5: predictions table is empty - "
          "run score_batch.py first")

print(f"\ncharts written to {os.path.abspath(OUT_DIR)}")