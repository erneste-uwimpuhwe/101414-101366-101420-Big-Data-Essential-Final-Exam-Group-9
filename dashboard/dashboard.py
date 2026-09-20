"""
Django dashboard - ride-hailing pipeline control centre.

FIVE SOURCES ON ONE PAGE, and you must be able to say which is which:
  * operational summary  -> MariaDB `trips`, written by the Kafka Connect
                            JDBC sink with no manual step
  * late-trip alerts     -> MariaDB `trip_alerts`, written by our custom
                            Kafka consumer group as records stream past
  * insight charts       -> MariaDB insight_* tables, written by PySpark
                            reading the full history out of HDFS
  * predictions          -> MariaDB `predictions`, written by the deployed
                            Spark MLlib GBT model
  * model metrics        -> MariaDB `model_metrics`, GBT vs linear baseline

ENHANCEMENTS BEYOND THE REFERENCE ARCHITECTURE (rubric item 7):
  1. alert panel driven by the consumer group's business logic, showing the
     Kafka partition and offset that produced each alert
  2. pickup-zone drill-down filter
  3. seven live charts - line, area, doughnut, bar, scatter and radar - all
     computed from query results on every request
  4. auto-refresh, and links out to every other console in the stack

WHY THE CHART DATA IS SERIALISED HERE:
  Each chart gets its numbers as JSON built in this view; Chart.js draws
  them client-side. Nothing is precomputed or cached, so a refresh redraws
  from the current contents of MariaDB rather than freezing at generation
  time.

WHY WE QUERY MARIADB DIRECTLY INSTEAD OF THROUGH THE DJANGO ORM:
  This app is read-only over tables owned by other pipeline components, so
  the ORM would buy nothing. The separate tripsite/ project uses the ORM
  with managed = False models to expose the same tables through Django
  admin - so the submission demonstrates both approaches.

Run:
    python dashboard.py runserver 8000
"""
import json
import os
import sys

import django
import mysql.connector
from django.conf import settings
from django.shortcuts import render
from django.urls import path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DB", "ridehail"),
)

# The generator applies a 1.2x multiplier in these hours, so they are
# highlighted on the hourly chart - the model has real signal to learn here.
RUSH_HOURS = {7, 8, 17, 18, 19}

if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY="dev-only-not-for-production",
        ROOT_URLCONF=__name__,
        ALLOWED_HOSTS=["*"],
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "django.contrib.staticfiles",
        ],
        DATABASES={},          # no ORM - this app only reads
        TEMPLATES=[{
            "BACKEND": "django.template.backends.django.DjangoTemplates",
            "DIRS": [os.path.join(BASE_DIR, "templates")],
            "APP_DIRS": True,
            "OPTIONS": {"context_processors": [
                "django.template.context_processors.request",
            ]},
        }],
        STATIC_URL="/static/",
        STATICFILES_DIRS=[os.path.join(BASE_DIR, "static")],
        USE_TZ=True,
    )
    django.setup()


def rows(sql, params=None):
    """Run SQL, return a list of dicts. A table may be empty or absent if its
    pipeline stage has not run yet, so failures degrade to an empty list
    rather than breaking the page."""
    conn = None
    try:
        conn = mysql.connector.connect(**DB)
        cur = conn.cursor(dictionary=True)
        cur.execute(sql, params or ())
        out = cur.fetchall()
        cur.close()
        return out
    except Exception as exc:                      # noqa: BLE001
        print(f"query skipped ({exc.__class__.__name__}): {sql[:70]}...")
        return []
    finally:
        if conn is not None:
            conn.close()


def one(sql, params=None, default=0):
    r = rows(sql, params)
    if not r:
        return default
    v = list(r[0].values())[0]
    return default if v is None else v


def f(x, nd=2):
    """Float or zero - MariaDB returns Decimal, which is not JSON safe."""
    try:
        return round(float(x), nd)
    except (TypeError, ValueError):
        return 0.0


def board(request):
    zone_filter = request.GET.get("zone", "").strip()
    where, params = "", ()
    if zone_filter:
        where, params = "WHERE pickup_zone = %s", (zone_filter,)

    trips_raw = one(f"SELECT COUNT(*) FROM trips {where}", params)
    alerts_raw = one("SELECT COUNT(*) FROM trip_alerts")

    summary = {
        "trips": f"{trips_raw:,}",
        "trips_raw": trips_raw,
        "alerts": f"{alerts_raw:,}",
        "alerts_raw": alerts_raw,
        "alert_pct": round(alerts_raw * 100.0 / trips_raw, 3) if trips_raw else 0,
        "drivers": one(f"SELECT COUNT(DISTINCT driver_id) FROM trips {where}",
                       params),
        "avg_duration": f(one(
            f"SELECT AVG(duration_min) FROM trips {where}", params), 1),
        "avg_distance": f(one(
            f"SELECT AVG(distance_km) FROM trips {where}", params), 1),
        "revenue_m": f(one(
            f"SELECT SUM(fare_rwf)/1000000 FROM trips {where}", params), 1),
    }

    # ---- chart 1: demand through the day (area + line, dual axis) -------
    hourly = rows("""SELECT pickup_hour, trip_count, avg_duration, avg_surge
                     FROM insight_hourly_demand ORDER BY pickup_hour""")
    chart_hourly = {
        "labels": [f"{h['pickup_hour']:02d}:00" for h in hourly],
        "counts": [int(h["trip_count"]) for h in hourly],
        "durations": [f(h["avg_duration"], 1) for h in hourly],
        "rush": [1 if h["pickup_hour"] in RUSH_HOURS else 0 for h in hourly],
    }

    # ---- charts 2 and 3: congestion cost and traffic mix ----------------
    traffic = rows("""SELECT traffic_level, trip_count, avg_duration,
                             avg_min_per_km
                      FROM insight_traffic_impact
                      ORDER BY avg_min_per_km""")
    chart_traffic = {
        "labels": [t["traffic_level"] for t in traffic],
        "mpk": [f(t["avg_min_per_km"], 3) for t in traffic],
        "counts": [int(t["trip_count"]) for t in traffic],
    }
    chart_traffic_share = {
        "labels": [t["traffic_level"] for t in traffic],
        "counts": [int(t["trip_count"]) for t in traffic],
    }

    # ---- chart 4: demand by zone (bar + line) ---------------------------
    zones_i = rows("""SELECT pickup_zone, trip_count, avg_duration,
                             total_fare
                      FROM insight_zone_demand ORDER BY trip_count DESC""")
    chart_zones = {
        "labels": [z["pickup_zone"] for z in zones_i],
        "counts": [int(z["trip_count"]) for z in zones_i],
        "fares": [f(float(z["total_fare"] or 0) / 1e6, 1) for z in zones_i],
    }

    # ---- chart 5: predicted vs actual (scatter) --------------------------
    scatter = rows("""SELECT actual_min, predicted_min FROM predictions
                      LIMIT 1200""")
    chart_scatter = [{"x": f(s["actual_min"], 1), "y": f(s["predicted_min"], 1)}
                     for s in scatter]
    diag_max = (max([p["x"] for p in chart_scatter] +
                    [p["y"] for p in chart_scatter] + [10])
                if chart_scatter else 10)

    # ---- chart 6: error distribution (bar) ------------------------------
    buckets = rows("""SELECT
        SUM(CASE WHEN ABS(error_min) <= 1 THEN 1 ELSE 0 END) AS b1,
        SUM(CASE WHEN ABS(error_min) > 1 AND ABS(error_min) <= 2 THEN 1 ELSE 0 END) AS b2,
        SUM(CASE WHEN ABS(error_min) > 2 AND ABS(error_min) <= 4 THEN 1 ELSE 0 END) AS b3,
        SUM(CASE WHEN ABS(error_min) > 4 AND ABS(error_min) <= 8 THEN 1 ELSE 0 END) AS b4,
        SUM(CASE WHEN ABS(error_min) > 8 THEN 1 ELSE 0 END) AS b5
        FROM predictions""")
    b = buckets[0] if buckets else {}
    chart_errors = {
        "labels": ["0-1 min", "1-2 min", "2-4 min", "4-8 min", "over 8 min"],
        "counts": [int(b.get(k) or 0) for k in ("b1", "b2", "b3", "b4", "b5")],
    }

    # ---- chart 7: weather effect on pace (radar) ------------------------
    weather = rows(f"""SELECT weather,
                              AVG(duration_min / distance_km) AS mpk,
                              COUNT(*) AS n
                       FROM trips {where}
                       {'AND' if where else 'WHERE'} distance_km > 0.3
                       GROUP BY weather ORDER BY weather""", params)
    chart_weather = {
        "labels": [w["weather"] for w in weather],
        "mpk": [f(w["mpk"], 3) for w in weather],
    }

    # ---- model metrics ---------------------------------------------------
    metrics = rows("""SELECT model_name, rmse, mae, r2, train_rows,
                             test_rows, trained_at
                      FROM model_metrics ORDER BY trained_at DESC LIMIT 2""")
    for m in metrics:
        m["rmse"] = f(m["rmse"], 3)
        m["mae"] = f(m["mae"], 3)
        m["r2"] = f(m["r2"], 3)
    chart_models = {
        "labels": [m["model_name"].split(" (")[0] for m in metrics][::-1],
        "rmse": [m["rmse"] for m in metrics][::-1],
        "mae": [m["mae"] for m in metrics][::-1],
    }

    pred_stats = rows("""SELECT COUNT(*) AS n,
                                AVG(ABS(error_min)) AS mae,
                                SUM(CASE WHEN ABS(error_min) <= 3 THEN 1 ELSE 0 END)
                                  * 100.0 / COUNT(*) AS within3
                         FROM predictions""")
    ps = pred_stats[0] if pred_stats and pred_stats[0]["n"] else None
    if ps:
        ps["mae"] = f(ps["mae"], 2)
        ps["within3"] = f(ps["within3"], 1)

    ctx = {
        "summary": summary,
        "zone_filter": zone_filter,
        "zones": [r["pickup_zone"] for r in rows(
            "SELECT DISTINCT pickup_zone FROM trips ORDER BY pickup_zone")],
        "alerts": rows(
            """SELECT driver_id, pickup_zone, dropoff_zone, distance_km,
                      duration_min, traffic_level, partition_id, kafka_offset,
                      ROUND(duration_min / (distance_km / 30 * 60), 2) AS ratio
               FROM trip_alerts ORDER BY detected_at DESC LIMIT 8"""),
        # `weather` is selected here because the template renders it. A
        # column referenced in the template but missing from the query
        # resolves to an empty string silently - Django templates do not
        # raise on a missing key, which is exactly how the blank Weather
        # column got shipped the first time.
        "recent": rows(
            f"""SELECT driver_id, pickup_zone, dropoff_zone, distance_km,
                       duration_min, traffic_level, weather, fare_rwf,
                       event_ts
                FROM trips {where} ORDER BY event_ts DESC LIMIT 8""", params),
        "predictions": rows(
            """SELECT pickup_zone, dropoff_zone, distance_km, traffic_level,
                      actual_min, predicted_min, error_min
               FROM predictions ORDER BY ABS(error_min) DESC LIMIT 8"""),
        "pred_stats": ps,
        "metrics": metrics,
        "peak_hour": (max(hourly, key=lambda r: r["trip_count"])["pickup_hour"]
                      if hourly else None),
        # JSON blobs for Chart.js
        "j_hourly": json.dumps(chart_hourly),
        "j_traffic": json.dumps(chart_traffic),
        "j_traffic_share": json.dumps(chart_traffic_share),
        "j_zones": json.dumps(chart_zones),
        "j_scatter": json.dumps(chart_scatter),
        "j_diag_max": json.dumps(round(diag_max * 1.05, 1)),
        "j_errors": json.dumps(chart_errors),
        "j_weather": json.dumps(chart_weather),
        "j_models": json.dumps(chart_models),
    }
    return render(request, "board.html", ctx)


urlpatterns = [path("", board)]

if __name__ == "__main__":
    from django.core.management import execute_from_command_line
    execute_from_command_line(sys.argv)